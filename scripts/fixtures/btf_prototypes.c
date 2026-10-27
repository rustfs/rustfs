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
