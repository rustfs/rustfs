use std::{fs::Metadata, os::unix::fs::MetadataExt};

pub fn same_file(f1: &Metadata, f2: &Metadata) -> bool {
    if f1.dev() != f2.dev() {
        return false;
    }

    if f1.ino() != f2.ino() {
        return false;
    }

    if f1.size() != f2.size() {
        return false;
    }
    if f1.permissions() != f2.permissions() {
        return false;
    }

    if f1.mtime() != f2.mtime() {
        return false;
    }

    true
}
