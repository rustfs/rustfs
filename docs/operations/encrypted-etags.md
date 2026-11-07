# Encrypted object ETags

**Use this when:** upgrading encrypted storage or investigating missing ETags
in object listings after an upgrade.

New encrypted writes use a keyed fingerprint for the ETag rather than the
plaintext MD5. Single-part and part ETags are 32-character hexadecimal values;
completed multipart ETags retain their digest-and-part-count form. They stay
stable for the stored object. Request MD5 and checksum validation still apply
to the original plaintext. Encryption and decryption formats do not change.

## Listings and older objects

ListObjects, ListObjectsV2, and ListObjectVersions omit the optional ETag field for encrypted objects whose metadata does not attest to the protected format. This includes older encrypted objects and preserved source ETags without that proof. New single-part encrypted writes carry the internal format marker and expose their protected ETag in listings. Completed encrypted multipart uploads also carry that marker when every selected part has quorum-backed proof that its stored ETag came from the encryption reader and the final ETag is the aggregate of those parts. Unencrypted listings retain their existing ETags.

Each new encrypted part records its producer proof in optional part metadata. Older part metadata defaults to unproven. Mixing proven and unproven selected parts, or preserving a different source ETag, keeps the completed object unproven. The proof is checked during completion using the existing part metadata reads, so listings need no additional object reads or KMS requests. Internal same-version data movement, including rebalance and decommission, retains an existing source proof when quorum-validated staging metadata and the completed object agree on the version and preserved ETag. Tier restore also retains the proof for the same version and ETag when it commits under the matching restore operation. Sources without proof continue to require authorized HEAD access or a credentialed rewrite.

Clients that require an ETag for unproven objects must issue an authorized HeadObject request, including the SSE-C key when required. GET, HEAD, and conditional requests use the object's stored ETag. Already-completed encrypted multipart versions keep their original metadata and can still have no listing ETag after upgrading. Clients that enumerate historical versions, including backup software, may therefore require migration or an application change even after new uploads work. See the existing-data guidance below.

## Replication

Bucket and site replication forward the stored ETag on `x-rustfs-source-etag`
(with its `x-minio-source-*` twin). A target that adopts it (RustFS, MinIO)
reports the fingerprint for the replica; a target that computes its own ETag
(AWS S3 and most compatible stores) reports the MD5 of the plaintext it
received, which never equals the fingerprint. The post-PUT replica ETag audit
therefore skips encrypted sources. Managed-SSE sources forward no checksum on
the replication PUT either, so a target that transforms their payload is not
detected. See
`docs/operations/replication-outbound-transport.md`.

The same mismatch affects finding an existing replica on a target that mints
its own version ids (AWS S3, Wasabi), which a heal, MRF retry or
existing-object resync does before writing again:

- A replica recorded in the source's target-version ledger is addressed by
  that id, on a 404 and on an AWS-style 400 for the source id alike. For an
  encrypted source the replica counts when its size matches the bytes
  replication sent (plaintext for SSE-S3 / SSE-KMS, ciphertext for SSE-C) and
  it was written no earlier than the source version.
- Without a ledger entry (for example a PUT whose response was lost), the
  replica is located through `ListObjectVersions` by the same size and
  write-time rule, after removing versions that other generations of the key
  already claim. Only a single remaining match is adopted and recorded in the
  ledger. Several matches are never resolved by picking one: convergence writes
  the replica again, and a version purge is refused with the usual backoff.
  The `null` version of an unversioned target is never matched this way, and
  a target that refuses the listing keeps the earlier current-version check,
  which writes the replica again.
- Plaintext sources keep exact ETag identity. A target that adopts the source
  ETag still matches encrypted sources by ETag first.

Two same-size generations of a key whose own ledgers were both lost can still
be confused, and a version purge with no ledger and no shape match reports the
replica as already gone. Delete a leftover replica on the target by hand if a
`replication-check` VersionFidelity report shows one.

## Existing data and migration

The upgrade does not rewrite existing object metadata or erase fingerprints
already copied to backups, logs, caches, or replicas. Restrict access to those
copies separately. Rolling back to an older server also restores its listing
behavior, so keep the upgraded listing protection on every serving node.

To obtain a protected ETag, read the object with its encryption credentials and
write it through the upgraded encryption path. Verify the resulting plaintext
and application references before retiring an old copy. A server-side copy
that preserves the source ETag is insufficient.

In a versioned bucket, a new write creates a new version; historical versions
keep their original metadata. Object Lock retention and legal holds still
apply. Do not bypass them or delete retained versions to migrate an ETag.
There is no automatic in-place migration of retained metadata in this upgrade.
Unfinished uploads also keep their original part metadata. Restart an upload
through the upgraded path when its legacy part fingerprints need to be replaced.
