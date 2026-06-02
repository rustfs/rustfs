# RustFS S3 Table Core Concepts

This document keeps only the core concepts for adding table catalog capability
to RustFS. The direction is to build an Iceberg REST Catalog-compatible layer
on top of existing RustFS object storage.

## Concept Model

```text
TableBucket
  Warehouse
    Namespace
      TableIdentifier
        TableMetadataLocation
        TableMetadata
          Schema
          Snapshot
          Manifest
          DataFile
```

## TableBucket

`TableBucket` is a normal RustFS bucket enabled for table catalog use. It is not
a new physical bucket type.

RustFS can mark it with internal bucket metadata such as `table-bucket.json`.
That marker is an implementation detail, not a public compatibility API.

## Warehouse

`Warehouse` is the catalog storage root exposed to Iceberg clients. In the first
RustFS model, a warehouse maps to a table-enabled bucket.

Clients should interact with the warehouse through catalog APIs, not by knowing
RustFS internal metadata paths.

## Namespace

`Namespace` groups tables inside a warehouse. It is a first-class catalog
resource, not a raw object prefix.

Namespace validation must be conservative: no path traversal, encoded
separators, empty segments, control characters, or ambiguous normalization.

## TableIdentifier

`TableIdentifier` names one table inside a warehouse and namespace.

```text
warehouse + namespace + table name
```

It is resolved by the catalog layer to internal metadata objects. It should not
be treated as an S3 object key.

## TableMetadataLocation

`TableMetadataLocation` points to the current Iceberg table metadata file.

It is the main mutable table pointer. Only catalog commit code may update it.
Updates must use conflict detection so stale writers cannot overwrite newer
table state.

## TableMetadata

`TableMetadata` is the Iceberg metadata JSON referenced by the current metadata
location.

RustFS should preserve Iceberg metadata exactly where possible. Full parsing can
be added incrementally for fields RustFS must enforce, such as schemas,
snapshots, manifest lists, table location, and properties.

## Schema

`Schema` describes table columns.

RustFS does not need to interpret every schema detail at the beginning, but it
must preserve schema metadata without corrupting unknown Iceberg fields.

## Snapshot

`Snapshot` represents an immutable table state at a point in time.

Catalog commits advance the current metadata to a new snapshot. Maintenance
features such as rollback and snapshot expiration can be added later.

## Manifest

`Manifest` and manifest lists describe table data-file additions, deletions, and
references.

RustFS should store and protect these as catalog-owned metadata. Full manifest
parsing can be deferred until validation or maintenance requires it.

## DataFile

`DataFile` is a table data object, commonly Parquet, Avro, or ORC.

Data files live in object storage. The catalog metadata decides which data files
belong to a table snapshot.

## Reserved Metadata Boundary

Catalog-owned metadata should live under a reserved internal prefix, for
example:

```text
.rustfs-table/
  warehouses/
    <warehouse-id>/
      namespaces/
        <namespace-id>/
          tables/
            <table-id>/
              current.json
              metadata/
              manifests/
```

Ordinary S3 mutating APIs must not create, overwrite, copy into, delete, or
mutate objects under this reserved prefix for table-enabled buckets.

## Catalog Boundary

S3 object APIs remain object APIs.

Namespace, table, metadata pointer, and commit operations belong to a separate
catalog layer. Public compatibility should target Iceberg REST Catalog semantics
rather than exposing RustFS internal metadata objects directly.
