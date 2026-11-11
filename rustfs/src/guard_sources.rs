// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Test-only source reader for text guards that must survive a mechanical file split.
//!
//! A text guard asserts that some literal appears in production source. When it reads one file
//! with `include_str!`, moving the guarded function into another file either breaks the guard or,
//! worse, makes it stop seeing the code it guards. A guard built on [`SourceSet`] instead names
//! the file it reads today plus the sibling directory a split moves that file's code into, and
//! reads every `.rs` file of both at test time.
//!
//! The named file is required: a missing file panics with its path, so a guard never passes on
//! absent input. The directory is optional while the split has not happened; once it exists,
//! every `.rs` file under it (recursively) is read in byte order of its manifest-relative path,
//! after the named file.

use std::path::{Path, PathBuf};

/// One source file of a [`SourceSet`].
pub(crate) struct SourceFile {
    /// Path relative to the `rustfs` package root, with `/` separators.
    pub(crate) path: String,
    pub(crate) text: String,
}

/// The ordered files a text guard reads: one required file, then the split directory.
pub(crate) struct SourceSet {
    files: Vec<SourceFile>,
}

impl SourceSet {
    /// Reads `file` and every `.rs` file under `split_dir`, both relative to the `rustfs`
    /// package root.
    ///
    /// # Panics
    ///
    /// Panics, naming the path, when `file` is missing or unreadable, when `split_dir` exists but
    /// is not a directory, or when a file under `split_dir` cannot be read.
    pub(crate) fn read(file: &str, split_dir: &str) -> Self {
        // PROBE ONLY (DO NOT MERGE): a probe test may point the guards at a mirror tree.
        let root = probe::ROOT_OVERRIDE
            .with(|cell| cell.borrow().clone())
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")));
        Self::read_under(&root, file, split_dir)
    }

    fn read_under(root: &Path, file: &str, split_dir: &str) -> Self {
        let required = root.join(file);
        let text = std::fs::read_to_string(&required)
            .unwrap_or_else(|err| panic!("guard source {} must exist and be readable: {err}", required.display()));
        let mut files = vec![SourceFile {
            path: file.to_owned(),
            text,
        }];

        let dir = root.join(split_dir);
        if dir.exists() {
            assert!(dir.is_dir(), "guard split path {} must be a directory", dir.display());
            let mut paths = Vec::new();
            collect_rust_files(&dir, &mut paths);
            let mut split_files: Vec<SourceFile> = paths
                .into_iter()
                .map(|path| {
                    let text = std::fs::read_to_string(&path)
                        .unwrap_or_else(|err| panic!("guard source {} must be readable: {err}", path.display()));
                    let relative = path
                        .strip_prefix(root)
                        .unwrap_or_else(|_| panic!("guard source {} must be under {}", path.display(), root.display()));
                    let path = relative
                        .components()
                        .map(|component| component.as_os_str().to_string_lossy())
                        .collect::<Vec<_>>()
                        .join("/");
                    SourceFile { path, text }
                })
                .collect();
            split_files.sort_by(|left, right| left.path.cmp(&right.path));
            files.extend(split_files);
        }

        Self { files }
    }

    /// Every file of the set, the required file first.
    pub(crate) fn files(&self) -> &[SourceFile] {
        &self.files
    }

    /// Whether any one file of the set contains `needle`.
    pub(crate) fn contains(&self, needle: &str) -> bool {
        self.files.iter().any(|file| file.text.contains(needle))
    }

    /// Non-overlapping occurrences of `needle`, summed over the files; a match never spans two
    /// files.
    pub(crate) fn count(&self, needle: &str) -> usize {
        self.files.iter().map(|file| file.text.matches(needle).count()).sum()
    }

    /// The text after `marker` in the one file that contains it.
    ///
    /// # Panics
    ///
    /// Panics unless `marker` occurs exactly once across the whole set.
    pub(crate) fn after_unique_marker(&self, marker: &str) -> &str {
        let occurrences = self.count(marker);
        assert_eq!(occurrences, 1, "marker {marker:?} must occur exactly once across {}", self.describe());
        self.files
            .iter()
            .find_map(|file| file.text.split_once(marker).map(|(_, rest)| rest))
            .unwrap_or_else(|| unreachable!("a counted marker is found"))
    }

    /// Manifest-relative paths of the set, for panic messages.
    pub(crate) fn describe(&self) -> String {
        self.files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// PROBE ONLY (DO NOT MERGE): run a guard against a mirror of the package tree in which
/// guarded code has been moved into a split directory, with or without a break.
pub(crate) mod probe {
    use std::cell::RefCell;
    use std::path::PathBuf;

    thread_local! {
        pub(super) static ROOT_OVERRIDE: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
    }

    /// A real package file, as it is in this checkout.
    pub(crate) fn real(relative: &str) -> String {
        std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)).expect("real source")
    }

    /// `text` with the one occurrence of `from` replaced by `to`.
    pub(crate) fn replace_once(text: &str, from: &str, to: &str) -> String {
        assert_eq!(text.matches(from).count(), 1, "probe edit must apply exactly once: {from:?}");
        text.replacen(from, to, 1)
    }

    /// Cut the 4-space-indented method that contains `marker`, with its attributes and doc
    /// comments; returns (remaining text, moved method text).
    pub(crate) fn cut_method(text: &str, marker: &str) -> (String, String) {
        assert_eq!(text.matches(marker).count(), 1, "probe cut marker must be unique: {marker:?}");
        let idx = text.find(marker).expect("counted");
        let mut start = text[..idx].rfind('\n').map_or(0, |i| i + 1);
        while start > 0 {
            let prev = text[..start - 1].rfind('\n').map_or(0, |i| i + 1);
            let line = text[prev..start].trim_start();
            if line.starts_with("#[") || line.starts_with("///") {
                start = prev;
            } else {
                break;
            }
        }
        let end = idx + text[idx..].find("\n    }\n").expect("method end") + "\n    }\n".len();
        (format!("{}{}", &text[..start], &text[end..]), text[start..end].to_owned())
    }

    /// A temporary package root holding only the files a guard reads.
    pub(crate) struct Mirror {
        dir: tempfile::TempDir,
    }

    impl Mirror {
        pub(crate) fn new(files: &[(&str, &str)]) -> Self {
            let dir = tempfile::tempdir().expect("mirror dir");
            for (relative, text) in files {
                let path = dir.path().join(relative);
                std::fs::create_dir_all(path.parent().expect("parent")).expect("mirror parent");
                std::fs::write(path, text).expect("mirror file");
            }
            Self { dir }
        }

        fn run(&self, guard: fn()) -> std::thread::Result<()> {
            ROOT_OVERRIDE.with(|cell| *cell.borrow_mut() = Some(self.dir.path().to_path_buf()));
            let result = std::panic::catch_unwind(guard);
            ROOT_OVERRIDE.with(|cell| *cell.borrow_mut() = None);
            result
        }

        /// The guard must pass against this mirror.
        pub(crate) fn assert_passes(&self, guard: fn()) {
            if let Err(payload) = self.run(guard) {
                panic!("guard failed against the moved tree: {}", message(&*payload));
            }
        }

        /// The guard must fail against this mirror; returns its panic message.
        pub(crate) fn assert_fails(&self, guard: fn()) -> String {
            let payload = self.run(guard).expect_err("guard passed against the moved-and-broken tree");
            let text = message(&*payload);
            eprintln!("PROBE guard failed as expected: {text}");
            text
        }
    }

    fn message(payload: &(dyn std::any::Any + Send)) -> String {
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|text| (*text).to_owned()))
            .unwrap_or_default()
    }
}

fn collect_rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries =
        std::fs::read_dir(dir).unwrap_or_else(|err| panic!("guard split directory {} must be readable: {err}", dir.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|err| panic!("guard split directory {} must be listable: {err}", dir.display()));
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SourceSet;
    use std::fs;
    use std::path::Path;

    fn write(root: &Path, relative: &str, text: &str) {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().expect("fixture path has a parent")).expect("create fixture dir");
        fs::write(path, text).expect("write fixture");
    }

    fn panic_message(run: impl FnOnce() + std::panic::UnwindSafe) -> String {
        let payload = std::panic::catch_unwind(run).expect_err("the guard input must be rejected");
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|message| (*message).to_owned()))
            .unwrap_or_default()
    }

    #[test]
    fn absent_split_directory_reads_only_the_required_file() {
        let root = tempfile::tempdir().expect("tempdir");
        write(root.path(), "src/app/usecase.rs", "guarded();\n");

        let set = SourceSet::read_under(root.path(), "src/app/usecase.rs", "src/app/usecase");

        assert_eq!(set.describe(), "src/app/usecase.rs");
        assert!(set.contains("guarded();"));
        assert_eq!(set.count("guarded();"), 1);
    }

    #[test]
    fn split_directory_files_follow_the_required_file_in_path_order() {
        let root = tempfile::tempdir().expect("tempdir");
        write(root.path(), "src/app/usecase.rs", "guarded();\n");
        write(root.path(), "src/app/usecase/zeta.rs", "guarded();\n");
        write(root.path(), "src/app/usecase/alpha.rs", "guarded();\nguarded();\n");
        write(root.path(), "src/app/usecase/tests/nested.rs", "guarded();\n");
        write(root.path(), "src/app/usecase/notes.txt", "guarded();\n");

        let set = SourceSet::read_under(root.path(), "src/app/usecase.rs", "src/app/usecase");

        assert_eq!(
            set.describe(),
            "src/app/usecase.rs, src/app/usecase/alpha.rs, src/app/usecase/tests/nested.rs, src/app/usecase/zeta.rs"
        );
        assert_eq!(set.count("guarded();"), 5, "non-.rs files are not source");
    }

    #[test]
    fn missing_required_file_fails_even_when_the_split_directory_exists() {
        let root = tempfile::tempdir().expect("tempdir");
        write(root.path(), "src/app/usecase/moved.rs", "guarded();\n");
        let root_path = root.path().to_path_buf();

        let message = panic_message(move || {
            SourceSet::read_under(&root_path, "src/app/usecase.rs", "src/app/usecase");
        });

        assert!(message.contains("src/app/usecase.rs"), "panic must name the missing file: {message}");
    }

    #[test]
    fn split_path_that_is_a_file_fails() {
        let root = tempfile::tempdir().expect("tempdir");
        write(root.path(), "src/app/usecase.rs", "guarded();\n");
        write(root.path(), "src/app/usecase", "not a directory\n");
        let root_path = root.path().to_path_buf();

        let message = panic_message(move || {
            SourceSet::read_under(&root_path, "src/app/usecase.rs", "src/app/usecase");
        });

        assert!(message.contains("must be a directory"), "{message}");
    }

    #[test]
    fn count_does_not_join_a_needle_across_two_files() {
        let root = tempfile::tempdir().expect("tempdir");
        write(root.path(), "src/app/usecase.rs", "guard");
        write(root.path(), "src/app/usecase/tail.rs", "ed();\n");

        let set = SourceSet::read_under(root.path(), "src/app/usecase.rs", "src/app/usecase");

        assert_eq!(set.count("guarded();"), 0);
        assert!(!set.contains("guarded();"));
    }

    #[test]
    fn unique_marker_absent_from_the_set_fails() {
        let root = tempfile::tempdir().expect("tempdir");
        write(root.path(), "src/app/usecase.rs", "fn other() {}\n");
        let set = SourceSet::read_under(root.path(), "src/app/usecase.rs", "src/app/usecase");

        let message = panic_message(move || {
            set.after_unique_marker("fn guarded");
        });

        assert!(message.contains("exactly once"), "{message}");
    }

    #[test]
    fn unique_marker_in_two_files_fails() {
        let root = tempfile::tempdir().expect("tempdir");
        write(root.path(), "src/app/usecase.rs", "fn guarded() {}\n");
        write(root.path(), "src/app/usecase/copy.rs", "fn guarded() {}\n");
        let set = SourceSet::read_under(root.path(), "src/app/usecase.rs", "src/app/usecase");

        let message = panic_message(move || {
            set.after_unique_marker("fn guarded");
        });

        assert!(message.contains("src/app/usecase/copy.rs"), "panic must name the set: {message}");
    }

    #[test]
    fn unique_marker_twice_in_one_file_fails() {
        let root = tempfile::tempdir().expect("tempdir");
        write(root.path(), "src/app/usecase.rs", "fn guarded() {}\nfn guarded() {}\n");
        let set = SourceSet::read_under(root.path(), "src/app/usecase.rs", "src/app/usecase");

        let message = panic_message(move || {
            set.after_unique_marker("fn guarded");
        });

        assert!(message.contains("exactly once"), "{message}");
    }

    #[test]
    fn unique_marker_returns_the_rest_of_its_own_file_only() {
        let root = tempfile::tempdir().expect("tempdir");
        write(root.path(), "src/app/usecase.rs", "fn head() {}\n");
        write(root.path(), "src/app/usecase/moved.rs", "fn guarded() { body(); }\n");
        write(root.path(), "src/app/usecase/next.rs", "fn later() { other(); }\n");
        let set = SourceSet::read_under(root.path(), "src/app/usecase.rs", "src/app/usecase");

        let rest = set.after_unique_marker("fn guarded");

        assert_eq!(rest, "() { body(); }\n");
    }
}
