---
name: domain-ml
description: "Use when building ML/AI apps in Rust. Keywords: machine learning, ML, AI, tensor, model, inference, neural network, deep learning, training, prediction, ndarray, tch-rs, burn, candle, 机器学习, 人工智能, 模型推理"
---

# Machine Learning Domain

> **Layer 3: Domain Constraints**

## Domain Constraints → Design Implications

| Domain Rule | Design Constraint | Rust Implication |
|-------------|-------------------|------------------|
| Large data | Efficient memory | Zero-copy, streaming |
| GPU acceleration | CUDA/Metal support | candle, tch-rs |
| Model portability | Standard formats | ONNX |
| Batch processing | Throughput over latency | Batched inference |
| Numerical precision | Float handling | ndarray, careful f32/f64 |
| Reproducibility | Deterministic | Seeded random, versioning |

---

## Critical Constraints

### Memory Efficiency

```
RULE: Avoid copying large tensors
WHY: Memory bandwidth is bottleneck
RUST: References, views, in-place ops
```

### GPU Utilization

```
RULE: Batch operations for GPU efficiency
WHY: GPU overhead per kernel launch
RUST: Batch sizes, async data loading
```

### Model Portability

```
RULE: Use standard model formats
WHY: Train in Python, deploy in Rust
RUST: ONNX via tract or candle
```

---

## Trace Down ↓

From constraints to design (Layer 2):

```
"Need efficient data pipelines"
    ↓ m10-performance: Streaming, batching
    ↓ polars: Lazy evaluation

"Need GPU inference"
    ↓ m07-concurrency: Async data loading
    ↓ candle/tch-rs: CUDA backend

"Need model loading"
    ↓ m12-lifecycle: Lazy init, caching
    ↓ tract: ONNX runtime
```

---

## Use Case → Framework

| Use Case | Recommended | Why |
|----------|-------------|-----|
| Inference only | tract (ONNX) | Lightweight, portable |
| Training + inference | candle, burn | Pure Rust, GPU |
| PyTorch models | tch-rs | Direct bindings |
| Data pipelines | polars | Fast, lazy eval |

## Key Crates

| Purpose | Crate |
|---------|-------|
| Tensors | ndarray |
| ONNX inference | tract |
| ML framework | candle, burn |
| PyTorch bindings | tch-rs |
| Data processing | polars |
| Embeddings | fastembed |

## Design Patterns

| Pattern | Purpose | Implementation |
|---------|---------|----------------|
| Model loading | Once, reuse | `OnceLock<Model>` |
| Batching | Throughput | Collect then process |
| Streaming | Large data | Iterator-based |
| GPU async | Parallelism | Data loading parallel to compute |

## Code Pattern: Inference Server

```rust
use std::sync::OnceLock;
use tract_onnx::prelude::*;

static MODEL: OnceLock<SimplePlan<TypedFact, Box<dyn TypedOp>, Graph<TypedFact, Box<dyn TypedOp>>>> = OnceLock::new();

fn get_model() -> &'static SimplePlan<...> {
    MODEL.get_or_init(|| {
        tract_onnx::onnx()
            .model_for_path("model.onnx")
            .unwrap()
            .into_optimized()
            .unwrap()
            .into_runnable()
            .unwrap()
    })
}

async fn predict(input: Vec<f32>) -> anyhow::Result<Vec<f32>> {
    let model = get_model();
    let input = tract_ndarray::arr1(&input).into_shape((1, input.len()))?;
    let result = model.run(tvec!(input.into()))?;
    Ok(result[0].to_array_view::<f32>()?.iter().copied().collect())
}
```

## Code Pattern: Batched Inference

```rust
async fn batch_predict(inputs: Vec<Vec<f32>>, batch_size: usize) -> Vec<Vec<f32>> {
    let mut results = Vec::with_capacity(inputs.len());

    for batch in inputs.chunks(batch_size) {
        // Stack inputs into batch tensor
        let batch_tensor = stack_inputs(batch);

        // Run inference on batch
        let batch_output = model.run(batch_tensor).await;

        // Unstack results
        results.extend(unstack_outputs(batch_output));
    }

    results
}
```

---

## Common Mistakes

| Mistake | Domain Violation | Fix |
|---------|-----------------|-----|
| Clone tensors | Memory waste | Use views |
| Single inference | GPU underutilized | Batch processing |
| Load model per request | Slow | Singleton pattern |
| Sync data loading | GPU idle | Async pipeline |

---

## Trace to Layer 1

| Constraint | Layer 2 Pattern | Layer 1 Implementation |
|------------|-----------------|------------------------|
| Memory efficiency | Zero-copy | ndarray views |
| Model singleton | Lazy init | OnceLock<Model> |
| Batch processing | Chunked iteration | chunks() + parallel |
| GPU async | Concurrent loading | tokio::spawn + GPU |

---

## Related Skills

| When | See |
|------|-----|
| Performance | m10-performance |
| Lazy initialization | m12-lifecycle |
| Async patterns | m07-concurrency |
| Memory efficiency | m01-ownership |
