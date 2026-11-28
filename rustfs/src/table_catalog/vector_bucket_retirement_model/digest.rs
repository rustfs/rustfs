// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn string(bytes: &mut Vec<u8>, value: &str) -> TableCatalogStoreResult<()> {
    frame(bytes, value.as_bytes())
}
fn frame(bytes: &mut Vec<u8>, value: &[u8]) -> TableCatalogStoreResult<()> {
    let length = u64::try_from(value.len()).map_err(|_| invalid("Catalog retirement encoding length overflow"))?;
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(value);
    Ok(())
}
fn binding(value: &VectorBucketBindingV1) -> TableCatalogStoreResult<Vec<u8>> {
    value.to_handle()?;
    let mut bytes = value.version.to_be_bytes().to_vec();
    string(&mut bytes, &value.storage_bucket_id)?;
    bytes.extend_from_slice(value.bucket_incarnation.as_bytes());
    string(&mut bytes, &value.account_namespace_id)?;
    string(&mut bytes, &value.region)?;
    string(&mut bytes, &value.public_name)?;
    Ok(bytes)
}
fn resource(value: &VectorMaintenanceResourceV1) -> TableCatalogStoreResult<Vec<u8>> {
    value.validate()?;
    let mut bytes = Vec::new();
    frame(&mut bytes, &binding(&value.bucket)?)?;
    string(&mut bytes, &value.namespace)?;
    string(&mut bytes, &value.name)?;
    string(&mut bytes, &value.table_id)?;
    Ok(bytes)
}
fn target(value: &VectorCatalogRetirementTargetV1) -> TableCatalogStoreResult<Vec<u8>> {
    value.validate()?;
    let mut bytes = Vec::new();
    let kind = serde_json::to_value(value.kind).map_err(|e| invalid(e.to_string()))?;
    string(&mut bytes, kind.as_str().ok_or_else(|| invalid("invalid Catalog target kind encoding"))?)?;
    match &value.resource {
        None => bytes.push(0),
        Some(owner) => {
            bytes.push(1);
            frame(&mut bytes, &resource(owner)?)?;
        }
    }
    string(&mut bytes, &value.object_key)?;
    string(&mut bytes, &value.etag)?;
    match value.body_revision {
        None => bytes.push(0),
        Some(id) => {
            bytes.push(1);
            bytes.extend_from_slice(id.as_bytes());
        }
    }
    bytes.extend_from_slice(&decode_catalog_digest(&value.content_sha256)?);
    bytes.extend_from_slice(&value.content_length.to_be_bytes());
    ensure(
        bytes.len() <= VECTOR_BUCKET_RETIREMENT_PAGE_MAX_BYTES,
        "Catalog target encoding exceeds byte limit",
    )?;
    Ok(bytes)
}
fn hash_frame(hash: &mut Sha256, value: &[u8]) -> TableCatalogStoreResult<()> {
    hash.update(
        u64::try_from(value.len())
            .map_err(|_| invalid("Catalog digest length overflow"))?
            .to_be_bytes(),
    );
    hash.update(value);
    Ok(())
}
fn hash_prefix(domain: &[u8], bucket: &VectorBucketBindingV1, retirement_id: Uuid) -> TableCatalogStoreResult<Sha256> {
    nonnil(retirement_id)?;
    let mut hash = Sha256::new();
    hash.update(domain);
    hash_frame(&mut hash, &binding(bucket)?)?;
    hash.update(retirement_id.as_bytes());
    Ok(hash)
}
fn finish(hash: Sha256, count: u64, expected: u64) -> TableCatalogStoreResult<String> {
    ensure(count == expected, "Catalog digest count differs from authority")?;
    Ok(hex_simd::encode_to_string(hash.finalize(), hex_simd::AsciiCase::Lower))
}
fn increment(count: &mut u64, expected: u64) -> TableCatalogStoreResult<()> {
    *count = count.checked_add(1).ok_or_else(|| invalid("Catalog digest count overflow"))?;
    ensure(*count <= expected, "Catalog digest has excess records")
}

pub(in crate::table_catalog) struct CatalogRetirementInventoryDigest {
    hash: Sha256,
    bucket: VectorBucketBindingV1,
    previous: Option<String>,
    count: u64,
    expected: u64,
}
impl CatalogRetirementInventoryDigest {
    pub(in crate::table_catalog) fn new(
        bucket: &VectorBucketBindingV1,
        retirement_id: Uuid,
        expected: u64,
    ) -> TableCatalogStoreResult<Self> {
        let mut hash = hash_prefix(b"rustfs-vector-catalog-retirement-inventory-v1", bucket, retirement_id)?;
        hash.update(expected.to_be_bytes());
        Ok(Self {
            hash,
            bucket: bucket.clone(),
            previous: None,
            count: 0,
            expected,
        })
    }
    pub(in crate::table_catalog) fn push(&mut self, value: &VectorCatalogRetirementTargetV1) -> TableCatalogStoreResult<()> {
        ensure(
            value.resource.as_ref().is_none_or(|r| r.bucket == self.bucket),
            "foreign inventory resource",
        )?;
        ensure(
            self.previous.as_ref().is_none_or(|key| key < &value.object_key),
            "inventory keys repeat or are out of order",
        )?;
        let bytes = target(value)?;
        increment(&mut self.count, self.expected)?;
        hash_frame(&mut self.hash, &bytes)?;
        self.previous = Some(value.object_key.clone());
        Ok(())
    }
    pub(in crate::table_catalog) fn finish(self) -> TableCatalogStoreResult<String> {
        finish(self.hash, self.count, self.expected)
    }
}

pub(in crate::table_catalog) struct CatalogRetirementClosureDigest {
    hash: Sha256,
    bucket: VectorBucketBindingV1,
    previous: Option<(String, String, String)>,
    count: u64,
    expected: u64,
}
impl CatalogRetirementClosureDigest {
    pub(in crate::table_catalog) fn new(
        bucket: &VectorBucketBindingV1,
        retirement_id: Uuid,
        inventory_sha256: &str,
        expected: u64,
    ) -> TableCatalogStoreResult<Self> {
        let mut hash = hash_prefix(b"rustfs-vector-catalog-retirement-closure-v1", bucket, retirement_id)?;
        hash.update(decode_catalog_digest(inventory_sha256)?);
        hash.update(expected.to_be_bytes());
        Ok(Self {
            hash,
            bucket: bucket.clone(),
            previous: None,
            count: 0,
            expected,
        })
    }
    pub(in crate::table_catalog) fn push(
        &mut self,
        owner: &VectorMaintenanceResourceV1,
        proof_id: Uuid,
        fact_count: u64,
        proof_sha256: &str,
    ) -> TableCatalogStoreResult<()> {
        ensure(owner.bucket == self.bucket, "foreign closure resource")?;
        let key = (owner.namespace.clone(), owner.name.clone(), owner.table_id.clone());
        ensure(
            self.previous.as_ref().is_none_or(|last| last < &key),
            "closure resources repeat or are out of order",
        )?;
        nonnil(proof_id)?;
        ensure(fact_count > 0, "empty Catalog closure proof")?;
        let bytes = resource(owner)?;
        let digest = decode_catalog_digest(proof_sha256)?;
        increment(&mut self.count, self.expected)?;
        hash_frame(&mut self.hash, &bytes)?;
        self.hash.update(proof_id.as_bytes());
        self.hash.update(fact_count.to_be_bytes());
        self.hash.update(digest);
        self.previous = Some(key);
        Ok(())
    }
    pub(in crate::table_catalog) fn finish(self) -> TableCatalogStoreResult<String> {
        finish(self.hash, self.count, self.expected)
    }
}

pub(in crate::table_catalog) struct CatalogRetirementProofDigest {
    hash: Sha256,
    resource: VectorMaintenanceResourceV1,
    previous: Option<(u8, Vec<u8>, Vec<u8>, u64)>,
    count: u64,
    expected: u64,
    anchors: u64,
}
impl CatalogRetirementProofDigest {
    pub(in crate::table_catalog) fn new(
        bucket: &VectorBucketBindingV1,
        owner: &VectorMaintenanceResourceV1,
        retirement_id: Uuid,
        proof_id: Uuid,
        expected: u64,
    ) -> TableCatalogStoreResult<Self> {
        #[derive(Serialize)]
        struct Identity<'a> {
            bucket: &'a VectorBucketBindingV1,
            resource: &'a VectorMaintenanceResourceV1,
            retirement_id: Uuid,
            proof_id: Uuid,
        }
        bucket.to_handle()?;
        owner.validate()?;
        nonnil(retirement_id)?;
        nonnil(proof_id)?;
        ensure(owner.bucket == *bucket && expected > 0, "invalid Catalog proof identity or count")?;
        let identity = serde_json::to_vec(&Identity {
            bucket,
            resource: owner,
            retirement_id,
            proof_id,
        })
        .map_err(|e| invalid(e.to_string()))?;
        let mut hash = Sha256::new();
        hash.update(b"rustfs-vector-catalog-retirement-proof-v1");
        hash_frame(&mut hash, &identity)?;
        Ok(Self {
            hash,
            resource: owner.clone(),
            previous: None,
            count: 0,
            expected,
            anchors: 0,
        })
    }
    pub(in crate::table_catalog) fn push(&mut self, fact: &VectorCatalogRetirementProofFactV1) -> TableCatalogStoreResult<()> {
        fact.validate()?;
        fact.validate_resource(&self.resource)?;
        let order = fact.order();
        ensure(
            self.previous.as_ref().is_none_or(|last| last < &order),
            "Catalog proof facts repeat or are out of order",
        )?;
        let bytes = serde_json::to_vec(fact).map_err(|e| invalid(e.to_string()))?;
        ensure(
            bytes.len() <= VECTOR_BUCKET_RETIREMENT_PAGE_MAX_BYTES,
            "Catalog proof fact exceeds byte limit",
        )?;
        increment(&mut self.count, self.expected)?;
        hash_frame(&mut self.hash, &bytes)?;
        if matches!(fact, VectorCatalogRetirementProofFactV1::ResourceAnchor { .. }) {
            self.anchors += 1;
        }
        self.previous = Some(order);
        Ok(())
    }
    pub(in crate::table_catalog) fn finish(self) -> TableCatalogStoreResult<String> {
        ensure(self.anchors == 1, "Catalog proof requires one resource anchor")?;
        finish(self.hash, self.count, self.expected)
    }
}
