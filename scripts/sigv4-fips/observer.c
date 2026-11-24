// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 RustFS Team

#include <openssl/crypto.h>
#include <openssl/service_indicator.h>
#include <stdio.h>
#include <string.h>

// Observe only the Rust probe on the calling thread, after native initialization.
#ifdef __APPLE__
extern int main(int, char **);
#define RUST_MAIN main
#define PROBE_MAIN fips_probe_main
#else
extern int __real_main(int, char **);
#define RUST_MAIN __real_main
#define PROBE_MAIN __wrap_main
#endif

int PROBE_MAIN(int argc, char **argv) {
    if (argc != 2) return 2;
    if (FIPS_mode() != 1) {
        fputs("AWS-LC FIPS must be active\n", stderr);
        return 1;
    }
    const uint64_t before = FIPS_service_indicator_before_call();
    const int status = RUST_MAIN(argc, argv);
    if (status != 0) return status;
    const int invoked = FIPS_service_indicator_after_call() != before;
    if (invoked != (strcmp(argv[1], "control") != 0)) {
        fprintf(stderr, "Unexpected AWS-LC service indicator for %s\n", argv[1]);
        return 1;
    }
    return 0;
}
