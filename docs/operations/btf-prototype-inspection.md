# Read-only BTF prototype inspection

Use `scripts/inspect_btf_prototypes.py` to inspect declared function types in a
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

Selected type records expose their common BTF headers. Kind-specific payloads
and recursive layouts are not decoded by this command; use an independent BTF
dump when those records are required. A reported declaration is not a complete
C type, layout proof or runtime calling convention.

The reader supports BTF kinds 1 through 19, indexes at most 500,000 records,
reads at most 32 MiB plus one overflow-detection byte, limits names to 4,096
bytes, follows at most 32 qualifier records, and accepts at most 16 parameters
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
declarations and resolved types against `bpftool` output. The object is never
loaded. That comparison validates the tool's declared-format scope; it does not
qualify an original test node's kernel generation, complete observer budget, or
storage/business performance experiment.
