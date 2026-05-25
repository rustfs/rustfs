# Internode Transport Fallback and Backend Selection Model

Status: design note only. This document defines backend-neutral selection,
fallback, failure handling, negotiation, security, and observability rules for
the `InternodeDataTransport` adapter. It does not implement a new backend and
does not change production behavior.

## Open-source Scope

The open-source RustFS path keeps `tcp-http` as the default internode data
transport. This document defines adapter contracts only:

- no additional production backend is introduced;
- no dependency is added;
- no new accepted production backend value is added;
- RustFS core data-plane logic remains independent of the concrete transport
  implementation.

## Static Backend Selection

Static config is the first selection model. Existing accepted values remain:

| Config value | Meaning |
| --- | --- |
| unset | Use default TCP/HTTP backend. |
| `tcp-http` | Use default TCP/HTTP backend. |
| `tcp` | Alias for `tcp-http`. |
| any unsupported value | Fail closed with a diagnostic naming `RUSTFS_INTERNODE_DATA_TRANSPORT` and the invalid value. |

Unknown backend values must fail closed. Unsupported backend values must fail
closed. Any additional backend value must be explicitly added and must not
silently replace `tcp-http`.

Backend selection must expose an observable backend identity for metrics, logs,
and benchmark interpretation. The default and fallback path remains `tcp-http`.

## Fallback Contract

Fallback must be explicit and observable. Silent fallback is not allowed for
benchmark or production interpretation because it hides which backend moved the
payload.

| Condition | Default behavior | Explicit fallback behavior | Observability |
| --- | --- | --- | --- |
| Unsupported configured backend | Fail closed during transport construction. | Fall back only when a separately configured policy explicitly allows unsupported-backend fallback. | Error includes config key and invalid value; fallback event is counted when fallback is enabled. |
| Peer does not support selected backend | Fail before payload transfer. | Use TCP/HTTP only when both local policy and peer policy allow it. | Count peer mismatch and selected fallback backend. |
| Capability mismatch | Fail before payload transfer. | Use TCP/HTTP only if it satisfies the operation and policy allows fallback. | Record missing capability names or a low-cardinality reason. |
| Connection setup failure | Fail the operation. | Retry on TCP/HTTP only when fallback is allowed and no payload bytes have transferred. | Count setup failure, retry, fallback backend, and fallback result. |
| Partial transfer failure | Fail the operation and let existing object/quorum logic decide retry behavior. | Do not silently resume on another backend unless the transfer protocol can prove byte range, checksum, and idempotency boundaries. | Count partial failure with bytes completed. |
| Max transfer size exceeded | Fail before payload transfer or split at a higher layer. | Use TCP/HTTP if policy allows and TCP has no RustFS-level cap. | Record rejected size and selected backend. |
| Auth or encryption mismatch | Fail closed. | No fallback unless the fallback path satisfies the same or stronger security requirements. | Security failure metric and audit log entry. |

Fallback settings should not be added until there is an implementation that
uses them. A backend must define failure behavior before production use.

## Dynamic Negotiation Boundary

Dynamic negotiation is not implemented by this PR. If it is added later, it
belongs on the existing gRPC control plane. Data transfer must start only after
both peers agree on:

| Negotiated item | Required property |
| --- | --- |
| Backend name | Both peers know the backend and have it enabled. |
| Capability set | Required capabilities match the operation. |
| Max transfer size | The selected operation fits or is split before transfer starts. |
| Buffer rules | Both peers agree on staging and ownership rules. |
| Completion semantics | Both peers agree when a transfer is considered complete and when buffers may be reused. |
| Security mode | Authentication and encryption requirements are satisfied before any out-of-band transfer. |
| Fallback policy | Both peers agree whether TCP/HTTP fallback is allowed for this operation. |

Negotiation must not silently downgrade security or bypass existing disk
health, quorum, timeout, and integrity semantics.

## Failure Handling Requirements

| Failure mode | Requirement |
| --- | --- |
| Invalid config | Fail closed with `RUSTFS_INTERNODE_DATA_TRANSPORT` and the invalid value. |
| Backend disabled | Fail closed with the selected backend name and the missing enablement condition. |
| Backend unavailable | Fail closed with an actionable diagnostic; do not silently use TCP/HTTP. |
| Peer mismatch | Fail before payload transfer unless explicit fallback is configured. |
| Connection failure | Fail the operation and record setup failure; fallback only if policy allows and no payload bytes moved. |
| Completion failure | Return an operation error and release backend-owned resources. |
| Timeout | Return an operation error and preserve existing disk health and quorum semantics. |
| Partial transfer | Do not silently resume on another backend without a safe byte-range/checksum/idempotency proof. |
| Unsupported operation | Return a clear unsupported-operation error. |

## Security Requirements

- Backend selection must preserve peer authentication.
- Fallback must not weaken encryption or authorization.
- Partial transfers must not bypass bitrot verification or erasure quorum
  handling.
- Any adapter implementation must preserve the same request authority, disk,
  volume, path, and operation binding as the current TCP/HTTP path.

## Metrics and Observability Requirements

Metrics and logs must use low-cardinality labels or metadata:

- selected backend;
- requested backend;
- fallback backend, when used;
- operation name;
- success/failure;
- transferred bytes;
- setup failure count;
- partial transfer failure count;
- capability mismatch count;
- fallback decision count.

Adapter implementations must not add high-cardinality labels such as object
names, full paths, full URLs, peer-specific dynamic strings, memory addresses,
or buffer identifiers.

## TCP/HTTP Compatibility

The `tcp-http` backend remains the default and behavior-preserving path. It
uses ordinary byte streams, does not require backend-specific buffer
registration, and remains suitable as the fallback path when an explicit
fallback policy exists.

Any adapter implementation must not change the correctness semantics of
object writes, object reads, healing, bitrot verification, erasure quorum,
timeouts, or disk health handling.

## Adapter Stability

`InternodeDataTransport` should keep RustFS core data-plane logic separate from
the concrete transport implementation. The trait and `tcp-http` backend remain
inside `ecstore`.

This PR does not perform a crate split, add runtime loading, introduce a plugin
system, add a backend value, or implement a new transport backend.
