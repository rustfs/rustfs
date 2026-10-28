// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0
// Compiled for debug declarations only. This object is never loaded as BPF.
typedef void rustfs_void;

__attribute__((used, noinline))
unsigned long rustfs_btf_probe(void *data, const void *other, unsigned long length)
{
    return data == other ? length : 0;
}

__attribute__((used, noinline))
rustfs_void rustfs_btf_void(const void *data)
{
    __asm__ volatile("" : : "r"(data) : "memory");
}

typedef unsigned long long rustfs_u64;
struct rustfs_btf_inner {
    unsigned int prefix;
    const rustfs_u64 count;
};
struct rustfs_btf_layout {
    unsigned int kind;
    struct rustfs_btf_inner nested;
    volatile unsigned long flags;
    unsigned short slot;
    unsigned int bits : 3;
};
struct __attribute__((packed)) rustfs_btf_packed {
    char tag;
    unsigned int value;
};
volatile struct rustfs_btf_layout rustfs_btf_layout_instance;
volatile struct rustfs_btf_packed rustfs_btf_packed_instance;

#ifdef RUSTFS_BTF_LAYOUT_REFERENCE
#include <stddef.h>
#include <stdio.h>

// Independent compiler layout oracle; no BTF parser or kernel operations.
int main(void)
{
    printf("{\"rustfs_btf_layout\":{\"size\":%zu,\"fields\":{"
           "\"kind\":[%zu,%zu],\"nested.count\":[%zu,%zu],"
           "\"flags\":[%zu,%zu],\"slot\":[%zu,%zu]}},"
           "\"rustfs_btf_packed\":{\"size\":%zu,\"fields\":{\"value\":[%zu,%zu]}}}\n",
           sizeof(struct rustfs_btf_layout),
           offsetof(struct rustfs_btf_layout, kind), sizeof(rustfs_btf_layout_instance.kind),
           offsetof(struct rustfs_btf_layout, nested.count), sizeof(rustfs_btf_layout_instance.nested.count),
           offsetof(struct rustfs_btf_layout, flags), sizeof(rustfs_btf_layout_instance.flags),
           offsetof(struct rustfs_btf_layout, slot), sizeof(rustfs_btf_layout_instance.slot),
           sizeof(struct rustfs_btf_packed),
           offsetof(struct rustfs_btf_packed, value), sizeof(rustfs_btf_packed_instance.value));
    return 0;
}
#endif
