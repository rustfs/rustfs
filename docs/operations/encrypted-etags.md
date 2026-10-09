# Encrypted object ETags

**Use this when:** upgrading encrypted storage or investigating missing ETags
in object listings after an upgrade.

New encrypted writes use a keyed fingerprint for the ETag rather than the
plaintext MD5. Single-part and part ETags are 32-character hexadecimal values;
completed multipart ETags retain their digest-and-part-count form. They stay
stable for the stored object. Request MD5 and checksum validation still apply
to the original plaintext. Encryption and decryption formats do not change.

## Listings and older objects

ListObjects, ListObjectsV2, and ListObjectVersions omit the optional ETag field
for encrypted objects whose metadata does not attest to the protected format.
This includes encrypted objects written before the upgrade, preserved source
ETags without that proof, and completed encrypted multipart uploads. New
single-part encrypted writes carry the internal format marker and expose their
protected ETag in listings. Unencrypted listings retain their existing ETags.
Existing persistent listing snapshots are invalidated; the provider falls
back to verified storage until a new snapshot is built.

Clients that require an ETag for these older or multipart objects must issue an
authorized HeadObject request, including the SSE-C key when required. GET,
HEAD, and conditional requests continue to use the object's stored ETag.

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
