from pathlib import Path
import shlex
import tempfile
import unittest

from check_version import validate_tag


class ReleaseTagTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.manifest = Path(self.directory.name) / "crates/pikachu/Cargo.toml"
        self.manifest.parent.mkdir(parents=True)
        self.write_package("0.0.1")

    def write_package(self, version, name="pikachu", library_version=None):
        library_version = library_version or version
        self.manifest.write_text(
            f'[package]\nname = "{name}"\nversion = "{version}"\n'
            '[dependencies]\nmysql-canal = { path = "../mysql-canal", '
            f'version = "={library_version}" }}\n', encoding="utf-8"
        )
        self.library = self.manifest.parent.parent / "mysql-replication/Cargo.toml"
        self.library.parent.mkdir(parents=True, exist_ok=True)
        self.library.write_text(
            f'[package]\nname = "mysql-replication"\nversion = "{library_version}"\n', encoding="utf-8"
        )
        self.canal = self.manifest.parent.parent / "mysql-canal/Cargo.toml"
        self.canal.parent.mkdir(parents=True, exist_ok=True)
        self.canal.write_text(
            f'[package]\nname = "mysql-canal"\nversion = "{library_version}"\n'
            '[dependencies]\nmysql-replication = { path = "../mysql-replication", '
            f'version = "={library_version}" }}\n', encoding="utf-8"
        )
        self.facade = self.manifest.parent.parent / "mysql-cdc/Cargo.toml"
        self.facade.parent.mkdir(parents=True, exist_ok=True)
        self.facade.write_text(
            f'[package]\nname = "mysql-cdc-rs"\nversion = "{library_version}"\n'
            '[dependencies]\nmysql-canal = { path = "../mysql-canal", '
            f'version = "={library_version}" }}\n'
            'mysql-replication = { path = "../mysql-replication", '
            f'version = "={library_version}" }}\n', encoding="utf-8"
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
        for name in ["another-crate", "pikachu-cdc", "pikachu-rs"]:
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
                         ('path = "../mysql-canal"', 'path = "other"')]:
            with self.subTest(new=new):
                self.write_package("0.0.1")
                self.manifest.write_text(self.manifest.read_text().replace(old, new), encoding="utf-8")
                with self.assertRaises(ValueError):
                    validate_tag(self.manifest, "v0.0.1")

    def test_canal_identity_version_and_dependency_are_checked(self):
        for old, new in [
            ('name = "mysql-canal"', 'name = "another-crate"'),
            ('version = "0.0.1"', 'version = "0.0.2"'),
            ('version = "=0.0.1"', 'version = "=0.0.2"'),
            ('path = "../mysql-replication"', 'path = "other"'),
        ]:
            with self.subTest(new=new):
                self.write_package("0.0.1")
                self.canal.write_text(self.canal.read_text().replace(old, new), encoding="utf-8")
                with self.assertRaises(ValueError):
                    validate_tag(self.manifest, "v0.0.1")

    def test_missing_library_is_rejected(self):
        self.library.unlink()
        with self.assertRaises(FileNotFoundError):
            validate_tag(self.manifest, "v0.0.1")

    def test_facade_identity_version_and_both_dependencies_are_checked(self):
        for old, new in [
            ('name = "mysql-cdc-rs"', 'name = "mysql-cdc"'),
            ('version = "0.0.1"', 'version = "0.0.2"'),
            ('path = "../mysql-canal"', 'path = "other"'),
            ('path = "../mysql-replication"', 'path = "other"'),
            ('mysql-canal = { path = "../mysql-canal", version = "=0.0.1"',
             'mysql-canal = { path = "../mysql-canal", version = "0.0.1"'),
            ('mysql-replication = { path = "../mysql-replication", version = "=0.0.1"',
             'mysql-replication = { path = "../mysql-replication", version = "=0.0.2"'),
        ]:
            with self.subTest(new=new):
                self.write_package("0.0.1")
                self.facade.write_text(self.facade.read_text().replace(old, new), encoding="utf-8")
                with self.assertRaises(ValueError):
                    validate_tag(self.manifest, "v0.0.1")

    def test_missing_facade_is_rejected(self):
        self.facade.unlink()
        with self.assertRaises(FileNotFoundError):
            validate_tag(self.manifest, "v0.0.1")


class ReleaseWorkflowTests(unittest.TestCase):
    def test_automatic_uploads_publish_libraries_then_cli_using_only_oidc(self):
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
            ["mysql-replication", "mysql-canal", "mysql-cdc-rs", "pikachu"],
        )
        release = (workflows / "release.yml").read_text(encoding="utf-8")
        self.assertNotIn("secrets.", release)
        self.assertEqual(release.count("uses: rust-lang/crates-io-auth-action@v1"), 1)
        self.assertLess(release.index("id: auth"), release.index("run: cargo publish "))
        publish = release.split("\n  publish:\n", 1)[1]
        self.assertIn("needs: [version, ci]", publish)
        self.assertIn("id-token: write", publish)
        self.assertNotIn("id-token: write", release.split("\n  publish:\n", 1)[0])
        publish_steps = [step for step in publish.split("\n      - ") if "run: cargo publish " in step]
        self.assertEqual(len(publish_steps), 4)
        for step in publish_steps:
            with self.subTest(step=step.splitlines()[0]):
                self.assertIn("CARGO_REGISTRY_TOKEN: ${{ steps.auth.outputs.token }}", step)


if __name__ == "__main__":
    unittest.main()
