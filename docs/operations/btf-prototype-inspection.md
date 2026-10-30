# Read-only BTF declaration inspection

Use `scripts/inspect_btf_prototypes.py` to inspect declared function types or selected
integer member layouts in a
raw, little-endian BTF v1 regular file. Symlinks, devices and pipes are rejected.
The command reads the named file and writes one
JSON document to stdout. It does not load BPF, attach probes, read kernel memory,
change a service, or write to the source file.

```bash
python3 scripts/inspect_btf_prototypes.py \
  --btf /sys/kernel/btf/vmlinux \
  --function bpf_ringbuf_output
```

Repeat `--function` for up to 16 unique, exact names. The result preserves request
order, the observed source size/SHA-256, function/prototype type IDs, original
parameter type IDs, qualifier chains, and resolved declarations. A missing or
ambiguous function is an error, rather than a substitute selected by proximity.
The schema version is `1`.

Prototype records expose common BTF headers. A reported prototype is not a
complete C type, recursive layout proof or runtime calling convention.

For integer counters, select one exact struct and one or more named member paths:

```bash
python3 scripts/inspect_btf_prototypes.py \
  --btf /sys/kernel/btf/vmlinux \
  --struct bpf_ringbuf \
  --field mask --field consumer_pos --field producer_pos --field pending_pos
```

This mode validates the selected struct member records, qualifier chains and
integer payloads. `scalar_fields` preserves request order and reports each
declared byte offset, width, integer encoding flags and member path. Nested
structs are supported; every selected span must fit its declared container.
Only complete 1, 2, 4 or 8-byte integers are supported. Bitfields (including
legacy integer-offset encodings), pointer traversal, pointer/array/union leaves,
ambiguous members, missing types and by-value cycles are errors. No pointer
width is inferred. Byte alignment does not guarantee natural machine alignment.

`--struct` requires `--field` and cannot be combined with `--function`. Up to
16 unique paths, each containing at most 32 named members, are accepted. A
failure in any selected path rejects the whole report before emitting JSON.
The mode sets `runtime_ABI_verified` to `false`: it does not bind BTF to executing
machine code, pointer values, loaded images or an observer's admission contract.
Only requested paths are validated, not the complete BTF graph or all C layouts.

The reader supports BTF kinds 1 through 19, indexes at most 500,000 records,
reads at most 32 MiB plus one overflow-detection byte, limits names to 4,096
bytes, follows at most 32 type records per qualifier chain, and accepts at most 16 parameters
per selected prototype. Reports above 1 MiB are rejected before any JSON prefix
is emitted. Invalid/truncated/unsupported source, missing resolved type IDs,
malformed pointer/qualifier headers, and ambiguous selectors exit nonzero with
empty stdout. Success exits zero. Input I/O and inspection errors exit `2`.

Under the [Linux BTF format](https://docs.kernel.org/bpf/btf.html), ID zero denotes
void without a stored type record. The tool permits it at explicit return and
argument-pointee boundaries, including qualified void. A void descriptor has no
invented size or offset. The tool does not infer a pointer width, dereference
runtime pointers, or certify that a declared type matches the currently executing
machine code. It is a prototype inspector, not a complete BTF semantic verifier,
recursive layout validator, caller profiler, or runtime ABI acceptance gate.

Run the local contract tests with:

```bash
python3 -B scripts/test_inspect_btf_prototypes.py
```

The `BTF prototype reference` workflow additionally compiles a small C fixture,
extracts its raw `.BTF` section, and independently compares both selected
prototypes, scalar layouts and resolved types against `bpftool` output. A separate
ordinary C executable uses compiler `offsetof`/`sizeof` to check layout offsets
and widths, including nested and packed members; it does not parse BTF or load
probes. The BPF object is never loaded. The workflow selects a concrete
`bpftool` executable by availability and
version before comparing data; it does not depend on the hosted kernel's tools
wrapper or switch tools after a comparison failure. The native
[file-dump mode](https://github.com/libbpf/bpftool/blob/main/docs/bpftool-btf.rst)
accepts ELF or raw BTF files. That comparison validates
the tool's declared-format scope; it does not
qualify an original test node's kernel generation, complete observer budget, or
storage/business performance experiment.
