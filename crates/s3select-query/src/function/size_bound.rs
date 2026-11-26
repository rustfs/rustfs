// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Result-size guard for scalar functions whose output can expand beyond a single
//! input column (`concat`, `concat_ws`, `to_char`, and count-sized functions such
//! as `repeat`, `lpad`, and `rpad` if registered).
//!
//! Scalar results are built outside the DataFusion memory pool, so any unbounded
//! allocation can exhaust memory or abort the process via `handle_alloc_error`.
//! This wrapper checks a saturating upper bound on the bytes a call would produce
//! before invoking the inner UDF.

use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{DataType, FieldRef, Int32Type, Int64Type, UInt32Type, UInt64Type};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult as _, TreeNode as _};
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::preimage::PreimageResult;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::udf_eq::UdfEq;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, Expr, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, StructFieldMapping,
};
use rustfs_s3select_api::query::session::DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES;

/// Largest result, in bytes, a guarded scalar function may build for a single batch.
pub(crate) const MAX_SCALAR_RESULT_BYTES: usize = DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES / 4;

/// Conservative expansion factor for chrono `strftime` specifiers (for example `%c`, `%B`).
const TO_CHAR_FORMAT_EXPANSION_FACTOR: usize = 20;
const TO_CHAR_PER_ROW_OVERHEAD_BYTES: usize = 64;
const UNKNOWN_SCALAR_ESTIMATE_BYTES: usize = 64;

/// How a guarded function sizes its result from its arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ResultSize {
    /// `repeat(string, count)`: each row builds `len(string) * count` bytes.
    Repeat,
    /// `lpad(string, length[, fill])` / `rpad`: each row builds `length` characters,
    /// which DataFusion allocates at up to four bytes per character.
    Pad,
    /// `concat(arg1, ..., argN)`: each row concatenates all non-null arguments.
    Concat,
    /// `concat_ws(sep, arg1, ..., argN)`: each row concatenates `arg1..` with `sep`.
    ConcatWs,
    /// `to_char(datetime, format)`: expands strftime specifiers in `format`.
    ToChar,
}

impl ResultSize {
    fn for_function(name: &str) -> Option<Self> {
        match name {
            "repeat" => Some(Self::Repeat),
            "lpad" | "rpad" => Some(Self::Pad),
            "concat" => Some(Self::Concat),
            "concat_ws" => Some(Self::ConcatWs),
            "to_char" => Some(Self::ToChar),
            _ => None,
        }
    }

    /// Upper bound of the bytes the function builds for this batch. Arguments the
    /// function would reject anyway contribute nothing so the inner function reports
    /// the original type/argument error.
    fn bytes(self, args: &[ColumnarValue], number_rows: usize) -> usize {
        // All-scalar arguments are evaluated once, not once per row.
        let rows = if args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_))) {
            1
        } else {
            number_rows
        };
        match (self, args) {
            (Self::Repeat, [string, count]) => string_lengths(string, rows)
                .zip(counts(count, rows))
                .fold(0usize, |total, (len, count)| total.saturating_add(len.saturating_mul(count))),
            (Self::Pad, [_, length, ..]) => {
                counts(length, rows).fold(0usize, |total, length| total.saturating_add(length.saturating_mul(4)))
            }
            (Self::Concat, args) => args
                .iter()
                .fold(0usize, |total, arg| total.saturating_add(columnar_total_bytes(arg, rows))),
            (Self::ConcatWs, [separator, rest @ ..]) => {
                let values_bytes = rest
                    .iter()
                    .fold(0usize, |total, arg| total.saturating_add(columnar_total_bytes(arg, rows)));
                let sep_bytes = columnar_total_bytes(separator, rows).saturating_mul(rest.len().saturating_sub(1));
                values_bytes.saturating_add(sep_bytes)
            }
            (Self::ToChar, [_, format]) => columnar_total_bytes(format, rows)
                .saturating_mul(TO_CHAR_FORMAT_EXPANSION_FACTOR)
                .saturating_add(rows.saturating_mul(TO_CHAR_PER_ROW_OVERHEAD_BYTES)),
            _ => 0,
        }
    }

    fn check_literal_simplify_args(self, name: &str, args: &[Expr]) -> DFResult<()> {
        let total = match self {
            Self::Concat => args
                .iter()
                .fold(0usize, |acc, expr| acc.saturating_add(literal_expr_bytes(expr))),
            Self::ConcatWs => match args {
                [separator, rest @ ..] => {
                    let values_bytes = rest
                        .iter()
                        .fold(0usize, |acc, expr| acc.saturating_add(literal_expr_bytes(expr)));
                    let sep_bytes = literal_expr_bytes(separator).saturating_mul(rest.len().saturating_sub(1));
                    values_bytes.saturating_add(sep_bytes)
                }
                [] => 0,
            },
            Self::Repeat | Self::Pad | Self::ToChar => 0,
        };
        if total > MAX_SCALAR_RESULT_BYTES {
            return exec_err!("string size overflow on {name}, max size is {MAX_SCALAR_RESULT_BYTES}, but got {total}");
        }
        Ok(())
    }
}

fn literal_expr_bytes(expr: &Expr) -> usize {
    let Expr::Literal(scalar, _) = expr else {
        return 0;
    };
    scalar_byte_len(scalar)
}

fn scalar_byte_len(scalar: &ScalarValue) -> usize {
    match scalar {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => s.len(),
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::BinaryView(Some(b))
        | ScalarValue::FixedSizeBinary(_, Some(b)) => b.len(),
        ScalarValue::Null
        | ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::Binary(None)
        | ScalarValue::LargeBinary(None)
        | ScalarValue::BinaryView(None)
        | ScalarValue::FixedSizeBinary(_, None) => 0,
        _ => UNKNOWN_SCALAR_ESTIMATE_BYTES,
    }
}

fn columnar_total_bytes(value: &ColumnarValue, rows: usize) -> usize {
    match value {
        ColumnarValue::Scalar(scalar) => scalar_byte_len(scalar).saturating_mul(rows),
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => array
                .as_string::<i32>()
                .iter()
                .fold(0usize, |acc, s| acc.saturating_add(s.map_or(0, str::len))),
            DataType::LargeUtf8 => array
                .as_string::<i64>()
                .iter()
                .fold(0usize, |acc, s| acc.saturating_add(s.map_or(0, str::len))),
            DataType::Utf8View => array
                .as_string_view()
                .iter()
                .fold(0usize, |acc, s| acc.saturating_add(s.map_or(0, str::len))),
            DataType::Binary => array
                .as_binary::<i32>()
                .iter()
                .fold(0usize, |acc, b| acc.saturating_add(b.map_or(0, <[u8]>::len))),
            DataType::LargeBinary => array
                .as_binary::<i64>()
                .iter()
                .fold(0usize, |acc, b| acc.saturating_add(b.map_or(0, <[u8]>::len))),
            DataType::BinaryView => array
                .as_binary_view()
                .iter()
                .fold(0usize, |acc, b| acc.saturating_add(b.map_or(0, <[u8]>::len))),
            DataType::FixedSizeBinary(width) => usize::try_from(*width)
                .unwrap_or(0)
                .saturating_mul(array.len().saturating_sub(array.null_count())),
            DataType::Null => 0,
            _ => UNKNOWN_SCALAR_ESTIMATE_BYTES.saturating_mul(array.len().saturating_sub(array.null_count())),
        },
    }
}

fn string_lengths<'a>(value: &'a ColumnarValue, rows: usize) -> Box<dyn Iterator<Item = usize> + 'a> {
    match value {
        ColumnarValue::Scalar(scalar) => {
            let len = match scalar {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => s.len(),
                _ => 0,
            };
            Box::new(std::iter::repeat_n(len, rows))
        }
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => Box::new(array.as_string::<i32>().iter().map(|s| s.map_or(0, str::len))),
            DataType::LargeUtf8 => Box::new(array.as_string::<i64>().iter().map(|s| s.map_or(0, str::len))),
            DataType::Utf8View => Box::new(array.as_string_view().iter().map(|s| s.map_or(0, str::len))),
            _ => Box::new(std::iter::repeat_n(0, rows)),
        },
    }
}

fn counts<'a>(value: &'a ColumnarValue, rows: usize) -> Box<dyn Iterator<Item = usize> + 'a> {
    let i64_to_count = |count: Option<i64>| count.and_then(|count| usize::try_from(count).ok()).unwrap_or(0);
    let i32_to_count = |count: Option<i32>| count.and_then(|count| usize::try_from(count).ok()).unwrap_or(0);
    let u64_to_count = |count: Option<u64>| count.map_or(0, |count| usize::try_from(count).unwrap_or(usize::MAX));
    let u32_to_count = |count: Option<u32>| count.map_or(0, |count| usize::try_from(count).unwrap_or(usize::MAX));
    match value {
        ColumnarValue::Scalar(ScalarValue::Int64(count)) => Box::new(std::iter::repeat_n(i64_to_count(*count), rows)),
        ColumnarValue::Scalar(ScalarValue::Int32(count)) => Box::new(std::iter::repeat_n(i32_to_count(*count), rows)),
        ColumnarValue::Scalar(ScalarValue::UInt64(count)) => Box::new(std::iter::repeat_n(u64_to_count(*count), rows)),
        ColumnarValue::Scalar(ScalarValue::UInt32(count)) => Box::new(std::iter::repeat_n(u32_to_count(*count), rows)),
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Int64 => Box::new(array.as_primitive::<Int64Type>().iter().map(i64_to_count)),
            DataType::Int32 => Box::new(array.as_primitive::<Int32Type>().iter().map(i32_to_count)),
            DataType::UInt64 => Box::new(array.as_primitive::<UInt64Type>().iter().map(u64_to_count)),
            DataType::UInt32 => Box::new(array.as_primitive::<UInt32Type>().iter().map(u32_to_count)),
            _ => Box::new(std::iter::repeat_n(0, rows)),
        },
        _ => Box::new(std::iter::repeat_n(0, rows)),
    }
}

/// Wraps `udf` so that an oversized result fails the call instead of being allocated.
/// Functions whose result is bounded by their input are returned unchanged.
pub(crate) fn bound_result_size(udf: Arc<ScalarUDF>) -> Arc<ScalarUDF> {
    if (udf.inner().as_ref() as &dyn std::any::Any).is::<ResultSizeBoundedUdf>() {
        return udf;
    }
    match ResultSize::for_function(udf.name()) {
        Some(result_size) => Arc::new(ScalarUDF::new_from_impl(ResultSizeBoundedUdf {
            inner: Arc::clone(udf.inner()).into(),
            result_size,
        })),
        None => udf,
    }
}

/// A [`ScalarUDF`] whose `invoke_with_args` checks the size of the result the inner
/// function would build; all other behavior delegates to the inner function.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ResultSizeBoundedUdf {
    inner: UdfEq<Arc<dyn ScalarUDFImpl>>,
    result_size: ResultSize,
}

impl ScalarUDFImpl for ResultSizeBoundedUdf {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn schema_name(&self, args: &[Expr]) -> DFResult<String> {
        self.inner.schema_name(args)
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        self.inner.return_field_from_args(args)
    }

    fn is_strict(&self) -> bool {
        self.inner.is_strict()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let bytes = self.result_size.bytes(&args.args, args.number_rows);
        if bytes > MAX_SCALAR_RESULT_BYTES {
            return exec_err!(
                "string size overflow on {}, max size is {}, but got {}",
                self.name(),
                MAX_SCALAR_RESULT_BYTES,
                bytes
            );
        }
        self.inner.invoke_with_args(args)
    }

    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        self.inner.with_updated_config(config).map(|udf| {
            ScalarUDF::new_from_impl(Self {
                inner: Arc::clone(udf.inner()).into(),
                result_size: self.result_size,
            })
        })
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> DFResult<ExprSimplifyResult> {
        self.result_size.check_literal_simplify_args(self.name(), &args)?;
        let simplified = self.inner.simplify(args, info)?;
        match simplified {
            ExprSimplifyResult::Original(args) => Ok(ExprSimplifyResult::Original(args)),
            ExprSimplifyResult::Simplified(expr) => {
                // Re-wrap any scalar UDF synthesized by simplification (for example
                // `concat_ws('', ...)` rewriting into `concat(...)`) so the size bound
                // cannot be bypassed.
                let expr = expr
                    .transform_up(|node| match node {
                        Expr::ScalarFunction(mut func) => {
                            let bounded = bound_result_size(Arc::clone(&func.func));
                            if Arc::ptr_eq(&bounded, &func.func) {
                                Ok(Transformed::no(Expr::ScalarFunction(func)))
                            } else {
                                func.func = bounded;
                                Ok(Transformed::yes(Expr::ScalarFunction(func)))
                            }
                        }
                        other => Ok(Transformed::no(other)),
                    })
                    .data()?;
                Ok(ExprSimplifyResult::Simplified(expr))
            }
        }
    }

    fn preimage(&self, args: &[Expr], lit_expr: &Expr, info: &SimplifyContext) -> DFResult<PreimageResult> {
        self.inner.preimage(args, lit_expr, info)
    }

    fn conditional_arguments<'a>(&self, args: &'a [Expr]) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        self.inner.conditional_arguments(args)
    }

    fn short_circuits(&self) -> bool {
        self.inner.short_circuits()
    }

    fn evaluate_bounds(&self, input: &[&Interval]) -> DFResult<Interval> {
        self.inner.evaluate_bounds(input)
    }

    fn propagate_constraints(&self, interval: &Interval, inputs: &[&Interval]) -> DFResult<Option<Vec<Interval>>> {
        self.inner.propagate_constraints(interval, inputs)
    }

    fn struct_field_mapping(&self, literal_args: &[Option<ScalarValue>]) -> Option<StructFieldMapping> {
        self.inner.struct_field_mapping(literal_args)
    }

    fn output_ordering(&self, inputs: &[ExprProperties]) -> DFResult<SortProperties> {
        self.inner.output_ordering(inputs)
    }

    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> DFResult<bool> {
        self.inner.preserves_lex_ordering(inputs)
    }

    fn strictly_order_preserving(&self, inputs: &[ExprProperties]) -> DFResult<bool> {
        self.inner.strictly_order_preserving(inputs)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        self.inner.placement(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::execution::SessionStateDefaults;

    fn scalar_args(string: ScalarValue, count: i64) -> Vec<ColumnarValue> {
        vec![
            ColumnarValue::Scalar(string),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(count))),
        ]
    }

    #[test]
    fn repeat_bound_is_len_times_count_regardless_of_offset_width() {
        for string in [
            ScalarValue::Utf8(Some("ab".into())),
            ScalarValue::LargeUtf8(Some("ab".into())),
            ScalarValue::Utf8View(Some("ab".into())),
        ] {
            assert_eq!(ResultSize::Repeat.bytes(&scalar_args(string.clone(), 3), 8192), 6);
            assert_eq!(ResultSize::Repeat.bytes(&scalar_args(string, 1 << 62), 1), 1 << 63);
        }
        assert_eq!(
            ResultSize::Repeat.bytes(&scalar_args(ScalarValue::Utf8(Some("abc".into())), i64::MAX), 1),
            usize::MAX,
            "oversized counts saturate instead of wrapping"
        );
        assert_eq!(ResultSize::Repeat.bytes(&scalar_args(ScalarValue::Utf8(None), 1 << 62), 1), 0);
        assert_eq!(ResultSize::Repeat.bytes(&scalar_args(ScalarValue::Utf8(Some("a".into())), -1), 1), 0);
    }

    #[test]
    fn repeat_bound_sums_over_rows() {
        let strings: ColumnarValue = ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("abc"), None, Some("d")])));
        let counts = ColumnarValue::Array(Arc::new(Int64Array::from(vec![Some(2), Some(1 << 40), Some(5)])));
        assert_eq!(ResultSize::Repeat.bytes(&[strings.clone(), counts], 3), 11);
        let scalar_count = ColumnarValue::Scalar(ScalarValue::Int64(Some(10)));
        assert_eq!(ResultSize::Repeat.bytes(&[strings, scalar_count], 3), 40);
    }

    #[test]
    fn pad_bound_is_four_bytes_per_requested_character_per_row() {
        let string = ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".into())));
        let length = ColumnarValue::Scalar(ScalarValue::Int64(Some(10)));
        let fill = ColumnarValue::Scalar(ScalarValue::Utf8(Some("*".into())));
        assert_eq!(ResultSize::Pad.bytes(&[string.clone(), length.clone()], 8192), 40);
        assert_eq!(ResultSize::Pad.bytes(&[string, length, fill], 8192), 40);
        let strings = ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("a"), Some("b")])));
        let length = ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::from(i32::MAX))));
        assert!(ResultSize::Pad.bytes(&[strings, length], 2) > MAX_SCALAR_RESULT_BYTES);
    }

    #[test]
    fn concat_and_to_char_bounds_accumulate_across_rows() {
        let chunk = "x".repeat(1024 * 1024);
        let col_val = ColumnarValue::Array(Arc::new(StringArray::from(vec![Some(chunk.as_str()); 4])));
        let args = vec![col_val; 5];
        assert_eq!(ResultSize::Concat.bytes(&args, 4), 20 * 1024 * 1024);
        assert!(ResultSize::Concat.bytes(&args, 4) > MAX_SCALAR_RESULT_BYTES);

        let sep = ColumnarValue::Scalar(ScalarValue::Utf8(Some(chunk)));
        let short = ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("a"); 4])));
        let ws_args = vec![sep, short.clone(), short.clone(), short.clone(), short.clone(), short];
        assert!(ResultSize::ConcatWs.bytes(&ws_args, 4) >= 16 * 1024 * 1024);

        let fmt = ColumnarValue::Scalar(ScalarValue::Utf8(Some("%B".repeat(50_000))));
        let dt = ColumnarValue::Array(Arc::new(Int64Array::from(vec![Some(0); 10])));
        assert!(ResultSize::ToChar.bytes(&[dt, fmt], 10) > MAX_SCALAR_RESULT_BYTES);
    }

    #[test]
    fn expected_expanding_functions_are_wrapped_and_idempotent() {
        let mut wrapped = Vec::new();
        for udf in SessionStateDefaults::default_scalar_functions() {
            let name = udf.name().to_string();
            let bounded = bound_result_size(Arc::clone(&udf));
            if !Arc::ptr_eq(&udf, &bounded) {
                assert_eq!(bounded.name(), name);
                assert_eq!(bounded.aliases(), udf.aliases());
                assert_eq!(bounded.signature(), udf.signature());
                let double_bounded = bound_result_size(Arc::clone(&bounded));
                assert!(Arc::ptr_eq(&bounded, &double_bounded), "wrapping must be idempotent");
                wrapped.push(name);
            }
        }
        wrapped.sort();
        assert_eq!(wrapped, ["concat", "concat_ws", "lpad", "repeat", "rpad", "to_char"]);
    }
}
