# TODO: Implement Dynamic Transformers (Wasm / JS Engine)

## Context
The current `Transformer` architecture for pre-processing data (e.g., masking PII) requires developers to write Go code, register the transformer factory, and completely recompile the Go binary.

## The Problem
This approach heavily restricts runtime flexibility and slows down iterative development. Users cannot add, modify, or test simple transformation logic on the fly without triggering a full deployment cycle.

## Action Items
- [ ] Evaluate embedding a WebAssembly (Wasm) runtime such as `wazero` or a Javascript engine like `goja` into the transformer framework.
- [ ] Design an interface that allows users to supply custom transformation logic via scripts or Wasm binaries loaded at runtime.
- [ ] Update the Control Plane to allow uploading and managing these dynamic transformer scripts.
- [ ] Benchmark the performance impact of Wasm/JS execution versus native Go compilation to ensure the pipeline's high throughput is not significantly degraded.
