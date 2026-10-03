# Architecture

Validate IDs/dependencies/cycles/adapters before execution. Persist RUNNING before adapter invocation. Only Retryable can spend the retry budget. All other errors HOLD. DONE records survive reopening. The CLI binds exact workflow bytes with a SHA256 sidecar and rejects a changed spec. Dependencies establish ordering; their outputs are recorded but not automatically passed into downstream inputs.

## Tradeoff

The lab optimizes local reproducibility and an inspectable failure boundary. The architecture is intentionally small enough to explain during an interview.

## Evidence boundaries

Kubernetes integration and an upstream Argo contribution are not implemented.
