from pathlib import Path
import shlex
import tempfile
import unittest

from check_version import validate_tag


class ReleaseTagTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.manifest = Path(self.directory.name) / "Cargo.toml"
        self.write_package("0.0.1")

    def write_package(self, version, name="pikachu-rs", library_version=None):
        library_version = library_version or version
        self.manifest.write_text(
            f'[package]\nname = "{name}"\nversion = "{version}"\n'
            '[dependencies]\nmysql-replication = { path = "crates/mysql-replication", '
            f'version = "={library_version}" }}\n', encoding="utf-8"
        )
        self.library = self.manifest.parent / "crates/mysql-replication/Cargo.toml"
        self.library.parent.mkdir(parents=True, exist_ok=True)
        self.library.write_text(
            f'[package]\nname = "mysql-replication"\nversion = "{library_version}"\n', encoding="utf-8"
        )

    def test_matching_tag(self):
        self.assertEqual(validate_tag(self.manifest, "v0.0.1"), "0.0.1")

    def test_mismatches_and_non_version_refs(self):
        for tag in ["v0.0.2", "v0.1.0", "0.0.1", "main", "v0.0.1-rc.1", "v0.0.1\n"]:
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                validate_tag(self.manifest, tag)

    def test_prerelease_requires_exact_match(self):
        self.write_package("0.0.2-rc.1")
        self.assertEqual(validate_tag(self.manifest, "v0.0.2-rc.1"), "0.0.2-rc.1")
        with self.assertRaises(ValueError):
            validate_tag(self.manifest, "v0.0.2")

    def test_renamed_package_is_rejected(self):
        for name in ["another-crate", "pikachu-cdc"]:
            with self.subTest(name=name):
                self.write_package("0.0.1", name=name)
                with self.assertRaises(ValueError):
                    validate_tag(self.manifest, "v0.0.1")

    def test_library_name_and_dependency_version_must_match(self):
        for name, version in [("another-crate", "0.0.1"), ("mysql-replication", "0.0.2")]:
            with self.subTest(name=name, version=version):
                self.library.write_text(
                    f'[package]\nname = "{name}"\nversion = "{version}"\n', encoding="utf-8"
                )
                with self.assertRaises(ValueError):
                    validate_tag(self.manifest, "v0.0.1")

    def test_cli_and_library_must_share_the_release_version(self):
        self.write_package("0.0.2", library_version="0.0.1")
        with self.assertRaises(ValueError):
            validate_tag(self.manifest, "v0.0.2")
        with self.assertRaises(ValueError):
            validate_tag(self.manifest, "v0.0.1")

    def test_cli_dependency_must_pin_the_workspace_library(self):
        for old, new in [('version = "=0.0.1"', 'version = "=0.0.2"'),
                         ('path = "crates/mysql-replication"', 'path = "other"')]:
            with self.subTest(new=new):
                self.write_package("0.0.1")
                self.manifest.write_text(self.manifest.read_text().replace(old, new), encoding="utf-8")
                with self.assertRaises(ValueError):
                    validate_tag(self.manifest, "v0.0.1")

    def test_missing_library_is_rejected(self):
        self.library.unlink()
        with self.assertRaises(FileNotFoundError):
            validate_tag(self.manifest, "v0.0.1")


class ReleaseWorkflowTests(unittest.TestCase):
    def test_automatic_uploads_publish_library_then_cli_with_scoped_credentials(self):
        workflows = Path(__file__).resolve().parents[2] / ".github/workflows"
        uploads = []
        for workflow in workflows.glob("*.yml"):
            for line in workflow.read_text(encoding="utf-8").splitlines():
                if "cargo publish " not in line or line.lstrip().startswith("#"):
                    continue
                command = shlex.split(line[line.index("cargo publish "):])
                if "--dry-run" in command:
                    continue
                uploads.append(command)
                with self.subTest(workflow=workflow.name, command=command):
                    self.assertNotIn("--workspace", command)
                    self.assertIn("--package", command)
        self.assertEqual(
            [command[command.index("--package") + 1] for command in uploads],
            ["mysql-replication", "pikachu-rs"],
        )
        release = (workflows / "release.yml").read_text(encoding="utf-8")
        library = release.split("- name: Publish mysql-replication using an API token", 1)[1]
        library, remaining = library.split("- name: Authenticate with crates.io using OIDC", 1)
        self.assertIn("CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}", library)
        self.assertIn("--package mysql-replication", library)
        self.assertNotIn("secrets.CARGO_REGISTRY_TOKEN", remaining)
        self.assertIn("CARGO_REGISTRY_TOKEN: ${{ steps.auth.outputs.token }}", remaining)
        self.assertIn("--package pikachu-rs", remaining)


if __name__ == "__main__":
    unittest.main()
