# S3 Vectors pagination

**Use this when:** changing Vector continuation tokens, resource-list positions, session storage, snapshot retention, or expired-result collection.

## Session ownership

The first request selects a committed snapshot through ObjectBackedCatalog. QueryVectors computes the complete ranked candidate set once, stores row identifiers and exact f64 distance bits, and returns at most 100 matches per page. Metadata is read from the selected snapshot for each page. ListVectors resumes a scan offset in the selected snapshot, preserving segment membership and the processing limit. Writes, deletes and compaction after the first page cannot change those results. A request that fits one page creates no session object.

Each session generates a fresh 32-byte `session_signing_key` using the operating system random source. Its SHA-256 digest is the public session identifier. The key is used only for HMAC-SHA256 pagination signatures and stays in server-managed storage. The design requires no IAM master key configuration. SigV4 still authenticates every HTTP request, and current Vector IAM and optional KMS permissions are checked for every page.

## Storage and encoding

The immutable session object is stored in the Vector bucket at `.rustfs-table/<table_id>/query-results/<session_id>.session`. It is created through Managed Object I/O with the original request identity, quotas, audit and the Index's effective encryption configuration. Ordinary S3 access to these objects remains blocked by the Vector bucket boundary. Nodes share the existing ECStore object storage; local memory and process identity are not session authorities.

Record version 1 starts with the eight ASCII bytes `RFSVPG01`, followed by the raw 32-byte `session_signing_key` and strict JSON `SessionState`. The state binds the resource, original access key identity, normalized request digest, committed manifest version and digest, fixed issue and expiry times, and the operation-specific results. Unknown and duplicate fields are rejected. Query results encode distance as IEEE-754 f64 bits in an unsigned integer to preserve exact values through JSON decoding. List sessions store no ranked results. The record has a 64 MiB limit and QueryVectors retains at most 10,000 candidates. See `rustfs/src/vector/pagination.rs`.

The opaque `nextToken` contains base64url-encoded JSON claims followed by a base64url HMAC-SHA256 tag. The MAC covers the domain `rustfs.s3vectors.pagination.session.v1` followed by a zero byte and the complete claims bytes. Claims include the public identifier, session-state digest, binding digests, manifest version and digest, page position, and issue and expiry times. The signing key never appears in the token. A node uses only the bounded canonical identifier for the initial object lookup, then verifies the signature and all bindings before reopening the Catalog-selected historical manifest. Canonical request digests ignore JSON object member order and normalize omitted defaults. Clients return the opaque token and original parameters unchanged.

## Lifetime and collection

A session expires 900 seconds after the first request began. Later pages do not extend that deadline. Requests issued more than 60 seconds in the future relative to the serving node are rejected; cluster clocks must remain synchronized within that allowance. Final-page tokens remain retryable until expiry.

Active Index maintenance retains all committed historical versions. That existing policy protects the snapshot between requests. Each page also holds the Catalog operation's lifecycle and GC guards while reading. Any future policy that retires Active Index history must account for outstanding sessions before shortening this protection.

Periodic OrphanGc discovers canonical session objects alongside Dataset candidates in bounded pages. It admits session retirement only when native object modification time is at least one hour old, allowing time beyond the token lifetime and clock allowance. The existing Catalog retirement proof, exclusive GC protection, exact body revision, current maintenance IAM/KMS checks and recovery machinery govern deletion. Query-results access stays scoped separately from Dataset access. The same collection handles abandoned first-page attempts and sessions whose Index was deleted. Collection is subject to the existing reference-proof and job budgets. Maintenance must be configured and running for physical cleanup; expiry rejects tokens independently of cleanup progress. A format reader that does not recognize query-results retirement targets rejects the plan.

## Resource listings

ListVectorBuckets and ListIndexes use opaque position tokens of at most 512 characters. Each page accepts `maxResults` from 1 through 500; the implementation defaults to 500. Clients can change a valid page size while retaining the original prefix, credentials and resolved parent bucket. Bucket name and ARN selectors resolve to the same parent identity. A token is returned only when another matching item exists, and identifies the last item actually returned.

The application-private version-1 token is compact JSON encoded with unpadded base64url. Its fields are `version`, `context`, `after` and `identity`. The context digest binds the operation, existing ECStore deployment ID, region/account, original access-key digest, prefix and the complete parent bucket identity for ListIndexes. The position identity digest binds the last bucket incarnation or Index table ID. Names, field sets, versions, digest shapes and canonical base64 are validated before the cursor reaches Catalog. The current encoding needs at most 323 characters for a maximum-length name. See `rustfs/src/app/vector_usecase/resource_list_pagination.rs`.

These unsigned tokens carry a resource position. Every page authenticates the request, checks current Vector permissions and reads current resource records. ListIndexes retains its existing lifecycle and metadata guards. The token creates no access capability or managed object. Resource listings have no time-based token expiry and allocate no stored session. Static collections support identical retries and continuation on another node or after restart. Resource creation or deletion can change later pages.

Bucket listing retains the existing public-name ordering and full metadata scan. Index listing follows Catalog object-key order through a Catalog-owned cursor constructor. A deleted last item leaves its position usable; a same-name replacement with a different identity rejects the original token. Recreating the parent bucket also rejects an old ListIndexes token. Current authorization failures and Catalog integrity/storage errors retain their normal error paths. An invalid continuation token returns ValidationException. Ordinary listing permissions remain sufficient for these summary operations.

## Verification

`rustfs/src/vector/pagination/tests.rs` covers signatures, public identifier forgery, binding changes, expiry and persisted-state integrity. `rustfs/src/vector/lance/pagination_service_tests.rs` covers fixed snapshots, concurrent data changes, replay, segments and renewed authorization. `rustfs/src/app/vector_usecase/tests/sdk_acceptance/pagination.rs` exercises signed AWS SDK requests through separate Gateway instances sharing native storage. `rustfs/src/app/vector_usecase/maintenance/pagination_session_tests.rs` exercises native session writes and Catalog-authorized collection with observable modification-time injection.

Resource-list coverage includes bounded parsing, Catalog ordering, exact last-item and parent identities, current authorization, SDK continuation across nodes and restart, and parent-bucket recreation after verified cleanup.

The Vector external-activation gate remains unchanged. Pagination does not change existing lock algorithms, EC quorum rules, or the separately tracked multi-node commit limitations.
