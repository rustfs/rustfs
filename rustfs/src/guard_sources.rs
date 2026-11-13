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

//! Test-only source reader for text guards that must survive a mechanical file split.
//!
//! A text guard asserts that some literal appears in production source. When it reads one file
//! with `include_str!`, moving the guarded function into another file either breaks the guard or,
//! worse, makes it stop seeing the code it guards. A guard built on [`SourceSet`] instead names
//! the file it reads today plus the sibling directory a split moves that file's code into, and
//! reads every `.rs` file of both at test time.
//!
//! The named file is required: a missing file panics with its path, so a guard never passes on
//! absent input. The directory is optional while the split has not happened; once it exists,
//! every `.rs` file under it (recursively) is read in byte order of its manifest-relative path,
//! after the named file.
//!
//! [`SourceSet::production`] keeps production text only, because a guard about production code
//! must not be satisfied by a test that merely mentions the guarded call: it leaves out files
//! that are test-only by path (a `tests` directory, `tests.rs`, `*_test.rs`, `*_tests.rs`,
//! `test_support.rs`, a file with `#![cfg(test)]`, or a child of a `#[cfg(test)] mod x;`) and
//! removes every item annotated `#[cfg(test)]` or `#[cfg(all(test, ...))]`. The stripper relies on
//! rustfmt's layout and panics, naming the file and line, on any shape it does not recognise
//! instead of guessing which text to keep.

use std::path::{Path, PathBuf};

/// One source file of a [`SourceSet`].
pub(crate) struct SourceFile {
    /// Path relative to the `rustfs` package root, with `/` separators.
    pub(crate) path: String,
    pub(crate) text: String,
}

/// The ordered files a text guard reads: one required file, then the split directory.
pub(crate) struct SourceSet {
    files: Vec<SourceFile>,
}

impl SourceSet {
    /// Production text of `file` and of every `.rs` file under `split_dir`, both relative to the
    /// `rustfs` package root.
    ///
    /// # Panics
    ///
    /// Panics, naming the path, when `file` is missing, unreadable or test-only, when `split_dir`
    /// exists but is not a directory, when a file cannot be read, or when a `#[cfg(test)]` item
    /// has a shape the stripper does not recognise.
    pub(crate) fn production(file: &str, split_dir: &str) -> Self {
        Self::production_under(Path::new(env!("CARGO_MANIFEST_DIR")), file, split_dir)
    }

    /// [`SourceSet::production`] under another package root, for guard fixtures.
    pub(crate) fn production_under(root: &Path, file: &str, split_dir: &str) -> Self {
        assert!(
            !is_test_only_path(file),
            "guard source {file} is a test-only file; a production guard cannot be about it"
        );
        let mut stripped = Vec::new();
        let mut test_module_paths = Vec::new();
        for source in Self::read_under(root, file, split_dir).files {
            if is_test_only_path(&source.path) {
                continue;
            }
            let Some((text, test_modules)) = strip_test_items(&source.path, &source.text) else {
                assert!(source.path != file, "guard source {file} is test-only (#![cfg(test)])");
                continue;
            };
            let children = child_module_dir(&source.path);
            test_module_paths.extend(test_modules.into_iter().map(|name| format!("{children}/{name}")));
            stripped.push(SourceFile { path: source.path, text });
        }
        stripped.retain(|source| {
            !test_module_paths
                .iter()
                .any(|module| source.path == format!("{module}.rs") || source.path.starts_with(&format!("{module}/")))
        });
        Self { files: stripped }
    }

    fn read_under(root: &Path, file: &str, split_dir: &str) -> Self {
        let required = root.join(file);
        let text = std::fs::read_to_string(&required)
            .unwrap_or_else(|err| panic!("guard source {} must exist and be readable: {err}", required.display()));
        let mut files = vec![SourceFile {
            path: file.to_owned(),
            text,
        }];

        let dir = root.join(split_dir);
        if dir.exists() {
            assert!(dir.is_dir(), "guard split path {} must be a directory", dir.display());
            let mut paths = Vec::new();
            collect_rust_files(&dir, &mut paths);
            let mut split_files: Vec<SourceFile> = paths
                .into_iter()
                .map(|path| {
                    let text = std::fs::read_to_string(&path)
                        .unwrap_or_else(|err| panic!("guard source {} must be readable: {err}", path.display()));
                    let relative = path
                        .strip_prefix(root)
                        .unwrap_or_else(|_| panic!("guard source {} must be under {}", path.display(), root.display()));
                    let path = relative
                        .components()
                        .map(|component| component.as_os_str().to_string_lossy())
                        .collect::<Vec<_>>()
                        .join("/");
                    SourceFile { path, text }
                })
                .collect();
            split_files.sort_by(|left, right| left.path.cmp(&right.path));
            files.extend(split_files);
        }

        Self { files }
    }

    /// Whether any one file of the set contains `needle`.
    pub(crate) fn contains(&self, needle: &str) -> bool {
        self.files.iter().any(|file| file.text.contains(needle))
    }

    /// Non-overlapping occurrences of `needle`, summed over the files; a match never spans two
    /// files.
    pub(crate) fn count(&self, needle: &str) -> usize {
        self.files.iter().map(|file| file.text.matches(needle).count()).sum()
    }

    /// [`SourceSet::count`] with all whitespace removed from both the text and `needle`, so a
    /// call matches however rustfmt wraps its arguments.
    pub(crate) fn count_ignoring_whitespace(&self, needle: &str) -> usize {
        let needle: String = needle.split_whitespace().collect();
        self.files
            .iter()
            .map(|file| {
                file.text
                    .split_whitespace()
                    .collect::<String>()
                    .matches(needle.as_str())
                    .count()
            })
            .sum()
    }

    /// The text of the one `async fn name` method of the set: from its definition line, which
    /// must be indented by exactly four spaces (a method of a top-level `impl`), through the first
    /// following line that is exactly `    }`. A neighbouring method of any visibility, a test
    /// module, or the rest of the file is never part of it.
    ///
    /// # Panics
    ///
    /// Panics unless exactly one such definition exists across the set and it has that closing
    /// line.
    pub(crate) fn method_body(&self, name: &str) -> &str {
        let definitions: Vec<(&SourceFile, usize)> = self
            .files
            .iter()
            .flat_map(|file| {
                line_starts(&file.text)
                    .into_iter()
                    .filter(move |&start| is_method_definition(line_at(&file.text, start), name))
                    .map(move |start| (file, start))
            })
            .collect();
        assert_eq!(
            definitions.len(),
            1,
            "method {name} must be defined exactly once, as a four-space-indented `async fn`, across {}",
            self.describe()
        );
        let (file, start) = definitions[0];
        let end = line_starts(&file.text)
            .into_iter()
            .filter(|&line| line > start)
            .find(|&line| line_at(&file.text, line) == "    }")
            .unwrap_or_else(|| panic!("method {name} in {} has no closing `    }}` line", file.path));
        let close = end + "    }".len();
        &file.text[start..close]
    }

    /// Manifest-relative paths of the set, for panic messages.
    pub(crate) fn describe(&self) -> String {
        self.files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Byte offsets at which each line of `text` starts.
fn line_starts(text: &str) -> Vec<usize> {
    std::iter::once(0)
        .chain(text.match_indices('\n').map(|(index, _)| index + 1))
        .filter(|&start| start < text.len())
        .collect()
}

/// The line starting at `start`, without its line ending.
fn line_at(text: &str, start: usize) -> &str {
    let rest = &text[start..];
    let line = rest.split('\n').next().unwrap_or(rest);
    line.strip_suffix('\r').unwrap_or(line)
}

fn is_method_definition(line: &str, name: &str) -> bool {
    let Some(item) = line.strip_prefix("    ") else {
        return false;
    };
    if item.starts_with(' ') {
        return false;
    }
    let item = ["pub(crate) ", "pub(super) ", "pub "]
        .iter()
        .find_map(|visibility| item.strip_prefix(visibility))
        .unwrap_or(item);
    item.strip_prefix("async fn ")
        .and_then(|rest| rest.strip_prefix(name))
        .is_some_and(|rest| rest.starts_with(['(', '<']))
}

fn is_test_only_path(path: &str) -> bool {
    let components: Vec<&str> = path.split('/').collect();
    let Some((file, directories)) = components.split_last() else {
        return false;
    };
    let stem = file.strip_suffix(".rs").unwrap_or(*file);
    directories.contains(&"tests")
        || stem == "tests"
        || stem == "test_support"
        || stem.ends_with("_test")
        || stem.ends_with("_tests")
}

/// The directory that holds the child modules of the module in `path`.
fn child_module_dir(path: &str) -> String {
    match path.strip_suffix("/mod.rs") {
        Some(dir) => dir.to_owned(),
        None => path.strip_suffix(".rs").unwrap_or(path).to_owned(),
    }
}

fn indentation(line: &str) -> usize {
    line.len() - line.trim_start_matches(' ').len()
}

fn is_test_cfg_attribute(trimmed: &str) -> bool {
    trimmed == "#[cfg(test)]" || (trimmed.starts_with("#[cfg(all(test,") && trimmed.ends_with(")]"))
}

/// `text` without its `#[cfg(test)]` items, plus the names of the `#[cfg(test)] mod name;`
/// declarations it removed; `None` when the whole file is `#![cfg(test)]`.
fn strip_test_items(path: &str, text: &str) -> Option<(String, Vec<String>)> {
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    let mut kept = String::with_capacity(text.len());
    let mut test_modules = Vec::new();
    let mut index = 0;
    while index < lines.len() {
        let trimmed = lines[index].trim();
        if trimmed == "#![cfg(test)]" {
            return None;
        }
        if !is_test_cfg_attribute(trimmed) {
            assert!(
                !(trimmed.starts_with("#[cfg(test)]") || trimmed.starts_with("#[cfg(all(test,")),
                "{path}:{}: a test-only item on its attribute line is not a shape the guard stripper reads",
                index + 1
            );
            kept.push_str(lines[index]);
            index += 1;
            continue;
        }
        let indent = indentation(lines[index]);
        let mut item = index + 1;
        while item < lines.len() && is_attribute_or_comment(lines[item].trim()) {
            item += 1;
        }
        assert!(item < lines.len(), "{path}:{}: #[cfg(test)] is not followed by an item", index + 1);
        assert_eq!(
            indentation(lines[item]),
            indent,
            "{path}:{}: the #[cfg(test)] item is not at its attribute's indentation",
            item + 1
        );
        if let Some(name) = declared_module(lines[item].trim()) {
            test_modules.push(name.to_owned());
        }
        index = test_item_end(path, &lines, item, indent) + 1;
    }
    Some((kept, test_modules))
}

fn is_attribute_or_comment(trimmed: &str) -> bool {
    (trimmed.starts_with("#[") && trimmed.ends_with(']')) || trimmed.starts_with("//")
}

fn declared_module(item: &str) -> Option<&str> {
    let item = ["pub(crate) ", "pub(super) ", "pub "]
        .iter()
        .find_map(|visibility| item.strip_prefix(visibility))
        .unwrap_or(item);
    let name = item.strip_prefix("mod ")?.strip_suffix(';')?;
    (!name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')).then_some(name)
}

#[derive(Clone, Copy, PartialEq)]
enum Lexeme {
    Code,
    Str,
    RawStr(usize),
    BlockComment,
}

fn is_ident(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Index of the last line of the item that starts at `lines[start]` with indentation `indent`.
fn test_item_end(path: &str, lines: &[&str], start: usize, indent: usize) -> usize {
    let mut depth: i64 = 0;
    let mut state = Lexeme::Code;
    // Inside a `where` clause a trailing comma separates bounds instead of ending the item.
    let mut in_where_clause = false;
    for (offset, line) in lines[start..].iter().enumerate() {
        let number = start + offset + 1;
        if state == Lexeme::Code && depth == 0 && indentation(line) == indent && line.trim().starts_with("where") {
            in_where_clause = true;
        }
        let chars: Vec<char> = line.chars().collect();
        let mut last_code = None;
        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];
            let next = chars.get(i + 1).copied();
            match state {
                Lexeme::Code => match c {
                    '/' if next == Some('/') => break,
                    '/' if next == Some('*') => {
                        state = Lexeme::BlockComment;
                        i += 1;
                    }
                    '"' => state = Lexeme::Str,
                    'r' if matches!(next, Some('"' | '#'))
                        && (i == 0
                            || !is_ident(chars[i - 1])
                            || (chars[i - 1] == 'b' && (i == 1 || !is_ident(chars[i - 2])))) =>
                    {
                        let mut quote = i + 1;
                        while chars.get(quote) == Some(&'#') {
                            quote += 1;
                        }
                        if chars.get(quote) == Some(&'"') {
                            state = Lexeme::RawStr(quote - i - 1);
                            i = quote;
                        } else {
                            last_code = Some(c);
                        }
                    }
                    '\'' => {
                        if next == Some('\\') {
                            let close = chars
                                .get(i + 3..)
                                .and_then(|rest| rest.iter().position(|&candidate| candidate == '\''))
                                .unwrap_or_else(|| panic!("{path}:{number}: unterminated character literal"));
                            i += 3 + close;
                            last_code = Some('\'');
                        } else if chars.get(i + 2) == Some(&'\'') {
                            i += 2;
                            last_code = Some('\'');
                        } else {
                            last_code = Some(c);
                        }
                    }
                    '{' | '(' | '[' => {
                        depth += 1;
                        last_code = Some(c);
                    }
                    '}' | ')' | ']' => {
                        depth -= 1;
                        assert!(depth >= 0, "{path}:{number}: unbalanced `{c}` in a #[cfg(test)] item");
                        last_code = Some(c);
                    }
                    c if !c.is_whitespace() => last_code = Some(c),
                    _ => {}
                },
                Lexeme::Str => match c {
                    '\\' => i += 1,
                    '"' => {
                        state = Lexeme::Code;
                        last_code = Some('"');
                    }
                    _ => {}
                },
                Lexeme::RawStr(hashes) => {
                    if c == '"' && chars.len() > i + hashes && chars[i + 1..=i + hashes].iter().all(|&h| h == '#') {
                        i += hashes;
                        state = Lexeme::Code;
                        last_code = Some('"');
                    }
                }
                Lexeme::BlockComment => {
                    if c == '*' && next == Some('/') {
                        state = Lexeme::Code;
                        i += 1;
                    }
                }
            }
            i += 1;
        }
        if state != Lexeme::Code || depth != 0 {
            continue;
        }
        let line_indent = indentation(line);
        assert!(
            offset == 0 || line_indent >= indent,
            "{path}:{number}: a #[cfg(test)] item continues below its own indentation"
        );
        match last_code {
            Some(';') => return start + offset,
            Some(',') if !in_where_clause => return start + offset,
            Some('}') => {
                assert!(
                    offset == 0 || line_indent == indent,
                    "{path}:{number}: a #[cfg(test)] item block closes at indentation {line_indent}, expected {indent}"
                );
                return start + offset;
            }
            _ => {
                let following = lines
                    .get(start + offset + 1)
                    .unwrap_or_else(|| panic!("{path}:{number}: a #[cfg(test)] item never ends"));
                let following_trimmed = following.trim();
                assert!(
                    indentation(following) > indent
                        || (indentation(following) == indent
                            && ["where", "{", "->", ".", ")", "]"]
                                .iter()
                                .any(|prefix| following_trimmed.starts_with(prefix))),
                    "{path}:{}: the line after an unterminated #[cfg(test)] item line does not continue it",
                    number + 1
                );
            }
        }
    }
    panic!("{path}:{}: a #[cfg(test)] item never ends", start + 1);
}

fn collect_rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries =
        std::fs::read_dir(dir).unwrap_or_else(|err| panic!("guard split directory {} must be readable: {err}", dir.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|err| panic!("guard split directory {} must be listable: {err}", dir.display()));
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

/// Writes `files` under a fresh temporary package root, for guard fixtures.
pub(crate) fn fixture_root(files: &[(&str, &str)]) -> tempfile::TempDir {
    let root = tempfile::tempdir().expect("fixture root");
    for (relative, text) in files {
        let path = root.path().join(relative);
        std::fs::create_dir_all(path.parent().expect("fixture path has a parent")).expect("create fixture dir");
        std::fs::write(path, text).expect("write fixture");
    }
    root
}

/// Runs `run`, which must panic, and returns its panic message.
pub(crate) fn panic_message(run: impl FnOnce() + std::panic::UnwindSafe) -> String {
    let payload = std::panic::catch_unwind(run).expect_err("the guard input must be rejected");
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|message| (*message).to_owned()))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{SourceSet, fixture_root, panic_message};

    const MAIN: &str = "src/app/usecase.rs";
    const SPLIT: &str = "src/app/usecase";

    #[test]
    fn absent_split_directory_reads_only_the_required_file() {
        let root = fixture_root(&[(MAIN, "guarded();\n")]);

        let set = SourceSet::read_under(root.path(), MAIN, SPLIT);

        assert_eq!(set.describe(), MAIN);
        assert!(set.contains("guarded();"));
        assert_eq!(set.count("guarded();"), 1);
    }

    #[test]
    fn split_directory_files_follow_the_required_file_in_path_order() {
        let root = fixture_root(&[
            (MAIN, "guarded();\n"),
            ("src/app/usecase/zeta.rs", "guarded();\n"),
            ("src/app/usecase/alpha.rs", "guarded();\nguarded();\n"),
            ("src/app/usecase/tests/nested.rs", "guarded();\n"),
            ("src/app/usecase/notes.txt", "guarded();\n"),
        ]);

        let set = SourceSet::read_under(root.path(), MAIN, SPLIT);

        assert_eq!(
            set.describe(),
            "src/app/usecase.rs, src/app/usecase/alpha.rs, src/app/usecase/tests/nested.rs, src/app/usecase/zeta.rs"
        );
        assert_eq!(set.count("guarded();"), 5, "non-.rs files are not source");
    }

    #[test]
    fn missing_required_file_fails_even_when_the_split_directory_exists() {
        let root = fixture_root(&[("src/app/usecase/moved.rs", "guarded();\n")]);
        let root_path = root.path().to_path_buf();

        let message = panic_message(move || {
            SourceSet::production_under(&root_path, MAIN, SPLIT);
        });

        assert!(message.contains(MAIN), "panic must name the missing file: {message}");
    }

    #[test]
    fn split_path_that_is_a_file_fails() {
        let root = fixture_root(&[(MAIN, "guarded();\n"), (SPLIT, "not a directory\n")]);
        let root_path = root.path().to_path_buf();

        let message = panic_message(move || {
            SourceSet::production_under(&root_path, MAIN, SPLIT);
        });

        assert!(message.contains("must be a directory"), "{message}");
    }

    #[test]
    fn count_does_not_join_a_needle_across_two_files() {
        let root = fixture_root(&[(MAIN, "guard"), ("src/app/usecase/tail.rs", "ed();\n")]);

        let set = SourceSet::production_under(root.path(), MAIN, SPLIT);

        assert_eq!(set.count("guarded();"), 0);
        assert!(!set.contains("guarded();"));
    }

    #[test]
    fn production_text_keeps_production_items_and_drops_every_test_only_one() {
        let main = "\
use crate::kept;
#[cfg(test)]
use crate::test_only_import;
#[cfg(any(test, feature = \"x\"))]
fn kept_in_some_production_build() { guarded(); }

impl Usecase {
    #[cfg(test)]
    field_for_tests: Option<u8>,
    #[cfg(test)]
    pub fn without_context() -> Self {
        guarded();
    }
    fn run(&self) {
        #[cfg(test)]
        let value = injected()
            .map_or(value, |err| guarded());
        #[cfg(test)]
        if let Some(hook) = self.hook {
            guarded();
        }
        guarded();
    }
}

#[cfg(test)]
pub(crate) async fn helper<F>(f: F) -> u8
where
    F: Fn(),
{
    guarded();
}

#[cfg(all(test, unix))]
const CANARY: &str = \"guarded();\";

#[cfg(test)]
mod declared;

#[cfg(test)]
mod tests {
    const BRACES: &str = \"}}} {{\";
    const RAW: &[u8] = br#\"{\"a\": \"}\"}\"#;
    fn t() { let c = '}'; let d = '\\''; guarded(); }
}
";
        let root = fixture_root(&[
            (MAIN, main),
            ("src/app/usecase/declared.rs", "guarded();\n"),
            ("src/app/usecase/declared/deeper.rs", "guarded();\n"),
            ("src/app/usecase/ops.rs", "fn op() { guarded(); }\n"),
            ("src/app/usecase/tests/op_tests.rs", "guarded();\n"),
            ("src/app/usecase/ops_test.rs", "guarded();\n"),
            ("src/app/usecase/test_support.rs", "guarded();\n"),
            ("src/app/usecase/gated.rs", "#![cfg(test)]\nguarded();\n"),
        ]);

        let set = SourceSet::production_under(root.path(), MAIN, SPLIT);

        assert_eq!(set.describe(), "src/app/usecase.rs, src/app/usecase/ops.rs");
        assert_eq!(set.count("guarded();"), 3, "production calls: kept_in_some_production_build, run, op");
        assert!(set.contains("use crate::kept;") && set.contains("impl Usecase {") && set.contains("fn run(&self) {"));
        assert!(!set.contains("test_only_import") && !set.contains("without_context") && !set.contains("mod tests"));
    }

    #[test]
    fn a_test_only_item_on_its_attribute_line_fails_loudly() {
        let root = fixture_root(&[(MAIN, "#[cfg(test)] use crate::hidden;\nguarded();\n")]);
        let root_path = root.path().to_path_buf();

        let message = panic_message(move || {
            SourceSet::production_under(&root_path, MAIN, SPLIT);
        });

        assert!(message.contains("src/app/usecase.rs:1"), "{message}");
    }

    #[test]
    fn a_test_only_item_that_never_closes_fails_loudly() {
        let root = fixture_root(&[(MAIN, "#[cfg(test)]\nmod tests {\n    fn t() {}\n")]);
        let root_path = root.path().to_path_buf();

        let message = panic_message(move || {
            SourceSet::production_under(&root_path, MAIN, SPLIT);
        });

        assert!(message.contains("never ends"), "{message}");
    }

    #[test]
    fn a_test_only_block_closing_at_another_indentation_fails_loudly() {
        let root = fixture_root(&[(MAIN, "impl X {\n    #[cfg(test)]\n    fn t() {\n}\n    fn kept() {}\n}\n")]);
        let root_path = root.path().to_path_buf();

        let message = panic_message(move || {
            SourceSet::production_under(&root_path, MAIN, SPLIT);
        });

        assert!(message.contains("src/app/usecase.rs:4"), "{message}");
    }

    #[test]
    fn an_unterminated_test_only_line_followed_by_production_fails_loudly() {
        let root = fixture_root(&[(MAIN, "fn f() {\n    #[cfg(test)]\n    let hook = injected()\n    guarded();\n}\n")]);
        let root_path = root.path().to_path_buf();

        let message = panic_message(move || {
            SourceSet::production_under(&root_path, MAIN, SPLIT);
        });

        assert!(message.contains("does not continue it"), "{message}");
    }

    #[test]
    fn a_test_only_required_file_is_rejected() {
        let root = fixture_root(&[("src/app/usecase_test.rs", "guarded();\n")]);
        let root_path = root.path().to_path_buf();

        let message = panic_message(move || {
            SourceSet::production_under(&root_path, "src/app/usecase_test.rs", "src/app/usecase_test");
        });

        assert!(message.contains("test-only"), "{message}");
    }

    const METHODS: &str = "\
impl Usecase {
    pub async fn guarded(&self) {
        first();
    }

    pub(crate) async fn neighbour(&self) {
        needle();
    }

    pub(super) async fn other(&self) {
        needle();
    }
}
";

    #[test]
    fn method_body_stops_at_its_own_closing_line_before_any_neighbour() {
        let root = fixture_root(&[
            (MAIN, METHODS),
            ("src/app/usecase/later.rs", "impl Usecase {\n    fn x() { needle(); }\n}\n"),
        ]);

        let set = SourceSet::production_under(root.path(), MAIN, SPLIT);
        let body = set.method_body("guarded");

        assert_eq!(body, "    pub async fn guarded(&self) {\n        first();\n    }");
        assert!(!body.contains("needle"));
    }

    #[test]
    fn method_body_of_the_last_method_stops_before_the_test_module() {
        let text = "impl Usecase {\n    async fn last(&self) {\n        first();\n    }\n}\n\n#[cfg(test)]\nmod tests {\n    fn t() { needle(); }\n}\n";
        let root = fixture_root(&[("src/app/usecase/moved.rs", text), (MAIN, "\n")]);

        let set = SourceSet::read_under(root.path(), MAIN, SPLIT);

        assert!(!set.method_body("last").contains("needle"));
    }

    #[test]
    fn method_body_does_not_match_a_longer_name_or_a_nested_definition() {
        let text = "impl Usecase {\n    pub async fn guarded_more(&self) {}\n        async fn guarded(&self) {}\n}\n";
        let root = fixture_root(&[(MAIN, text)]);
        let set = SourceSet::production_under(root.path(), MAIN, SPLIT);

        let message = panic_message(move || {
            set.method_body("guarded");
        });

        assert!(message.contains("exactly once"), "{message}");
    }

    #[test]
    fn method_body_defined_in_two_files_fails() {
        let root = fixture_root(&[(MAIN, METHODS), ("src/app/usecase/copy.rs", METHODS)]);
        let set = SourceSet::production_under(root.path(), MAIN, SPLIT);

        let message = panic_message(move || {
            set.method_body("guarded");
        });

        assert!(message.contains("src/app/usecase/copy.rs"), "panic must name the set: {message}");
    }

    #[test]
    fn method_body_without_a_closing_line_fails() {
        let root = fixture_root(&[(MAIN, "impl Usecase {\n    async fn guarded(&self) {\n        first();\n  }\n}\n")]);
        let set = SourceSet::production_under(root.path(), MAIN, SPLIT);

        let message = panic_message(move || {
            set.method_body("guarded");
        });

        assert!(message.contains("no closing"), "{message}");
    }
}
