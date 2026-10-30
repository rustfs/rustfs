#!/usr/bin/env python3
"""Exercise the documentation guard against temporary Git repositories."""

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parent


class DocPathTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        (self.root / "scripts").mkdir()
        for name in ("check_doc_paths.sh", "check_doc_paths.py", "python_bin.sh"):
            shutil.copy2(SOURCE / name, self.root / "scripts" / name)
        self.write("AGENTS.md", "# Guide\n", tracked=True)

    def write(self, name, text, tracked=False):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        if tracked:
            subprocess.run(["git", "add", "--", name], cwd=self.root, check=True)

    def run_guard(self, success=True):
        result = subprocess.run(
            ["bash", str(self.root / "scripts/check_doc_paths.sh")],
            cwd=self.root.parent, capture_output=True, text=True,
            env=dict(os.environ, RUSTFS_PYTHON=sys.executable),
        )
        self.assertEqual(result.returncode, 0 if success else 1, result.stdout + result.stderr)
        if success:
            self.assertIn("doc path check passed", result.stdout)
        return result.stderr

    def test_existing_repository_paths_still_pass(self):
        self.write("scripts/current.sh", "#!/bin/sh\n")
        self.write("crates/example/src/lib.rs", "")
        self.write("docs/deep/nested/guide.md", "Use `scripts/current.sh` and crates/example/src/lib.rs.\n|scripts/current.sh|\n", tracked=True)
        self.run_guard()

    def test_missing_path_in_table_is_reported(self):
        self.write("docs/guide.md", "|Path|\n|---|\n|scripts/missing.sh|\n")
        self.assertIn("docs/guide.md:3 -> scripts/missing.sh", self.run_guard(False))

    def test_executable_code_paths_remain_checked(self):
        self.write("docs/guide.md", "```bash\nbash scripts/deleted.sh\n```\n")
        self.assertIn("docs/guide.md:2 -> scripts/deleted.sh", self.run_guard(False))

    def test_agent_json_configuration_paths_are_checked(self):
        self.write("AGENTS.md", "Read `.gemini/settings.json`.\n")
        self.assertIn("AGENTS.md:1 -> .gemini/settings.json", self.run_guard(False))
        self.write(".gemini/settings.json", "{}\n")
        self.run_guard()

    def test_url_does_not_hide_missing_path_on_same_line(self):
        self.write("docs/guide.md", "See https://example.org/docs/external.md and `scripts/missing.sh`.\n", tracked=True)
        error = self.run_guard(False)
        self.assertIn("docs/guide.md:1 -> scripts/missing.sh", error)
        self.assertNotIn("external.md", error)

    def test_new_instruction_and_contributor_docs_are_in_scope(self):
        for name in (".agents/skills/demo/references/guide.md", "CONTRIBUTING.md", "crates/example/AGENTS.md"):
            with self.subTest(name=name):
                self.write(name, "Read [rules](missing.md).\n")
                self.assertIn(f"{name}:1 -> missing.md", self.run_guard(False))
                (self.root / name).unlink()

    def test_tracked_working_tree_changes_are_checked(self):
        self.write("AGENTS.md", "Read [rules](missing.md).\n")
        self.assertIn("AGENTS.md:1 -> missing.md", self.run_guard(False))

    def test_relative_root_and_reference_links_resolve_from_document(self):
        self.write(".agents/references/style.md", "# Rust Style\n")
        self.write("docs/file with spaces.md", "# Checks\n")
        self.write("docs/file(draft).md", "# Checks\n")
        self.write(".agents/skills/demo/SKILL.md", """# Demo
[relative](../../references/style.md#rust-style)
[root](/docs/file%20with%20spaces.md#checks)
[angle](</docs/file with spaces.md#checks> "Title")
[parentheses](/docs/file(draft).md#checks)
[escaped](/docs/file\\(draft\\).md#checks)
[style]: ../../references/style.md#rust-style "Style rules"
""")
        self.run_guard()

    def test_missing_reference_definition_is_reported(self):
        self.write("AGENTS.md", "Read [rules][policy].\n[policy]: missing.md\n")
        self.assertIn("AGENTS.md:2 -> missing.md", self.run_guard(False))

    def test_heading_anchors_include_duplicates_formatting_and_explicit_ids(self):
        self.write("docs/guide.md", """# Repeat
## Repeat
## Repeat-1
## `cargo_test` & **Lint**
Setext title
------------
## 中文 检查
<a id="manual-id"></a>
""")
        self.write("AGENTS.md", "\n".join(
            f"[section](docs/guide.md#{anchor})"
            for anchor in ("repeat", "repeat-1", "repeat-1-1", "cargo_test--lint", "setext-title", "%E4%B8%AD%E6%96%87-%E6%A3%80%E6%9F%A5", "manual-id")
        ))
        self.run_guard()

    def test_missing_or_example_only_heading_fails(self):
        self.write("docs/guide.md", "---\ndescription: Metadata\n---\n# Exists\n```markdown\n# Example\n```\n")
        for anchor in ("missing", "example", "exists-1", "description-metadata"):
            with self.subTest(anchor=anchor):
                self.write("AGENTS.md", f"[section](docs/guide.md#{anchor})\n")
                self.assertIn(f"stale heading anchor: AGENTS.md:1 -> docs/guide.md#{anchor}", self.run_guard(False))

    def test_same_document_anchors_are_checked(self):
        self.write("AGENTS.md", "# Guide\n[here](#guide)\n")
        self.run_guard()
        self.write("AGENTS.md", "# Guide\n[here](#absent)\n")
        self.assertIn("AGENTS.md:2 -> #absent", self.run_guard(False))

    def test_examples_placeholders_and_external_links_are_ignored(self):
        self.write("AGENTS.md", """# Guide
````markdown
[example](docs/missing.md)
```
scripts/missing.sh
````
~~~markdown
[example](docs/another.md)
~~~
    [indented](docs/example.md)
`[literal link](docs/not-a-reference.md)`
<!-- [comment](docs/comment.md) -->
[placeholder](docs/<version>.md)
`docs/*.md`, `docs/<version>.md`, `docs/${NAME}.md`
[external](https://example.org/docs/not-local.md)
[mail](mailto:docs@example.org)
foreign/docs/not-local.md
""")
        self.run_guard()

    def test_ignored_and_deleted_documents_are_not_checked(self):
        self.write(".gitignore", "docs/ignored.md\n")
        self.write("docs/ignored.md", "scripts/missing.sh\n")
        self.write("docs/deleted.md", "scripts/missing.sh\n", tracked=True)
        (self.root / "docs/deleted.md").unlink()
        self.run_guard()

    def test_deleted_link_target_fails(self):
        self.write("docs/guide.md", "# Guide\n", tracked=True)
        self.write("AGENTS.md", "[guide](docs/guide.md)\n")
        (self.root / "docs/guide.md").unlink()
        self.assertIn("AGENTS.md:1 -> docs/guide.md", self.run_guard(False))

    def test_links_cannot_escape_repository(self):
        self.write("AGENTS.md", "[outside](../outside.md)\n")
        self.assertIn("AGENTS.md:1 -> ../outside.md", self.run_guard(False))


if __name__ == "__main__":
    unittest.main()
