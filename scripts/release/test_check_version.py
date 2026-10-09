from pathlib import Path
import tempfile
import unittest

from check_version import validate_tag


class ReleaseTagTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.manifest = Path(self.directory.name) / "Cargo.toml"
        self.write_package("0.1.0")

    def write_package(self, version, name="pikachu-rs"):
        self.manifest.write_text(
            f'[package]\nname = "{name}"\nversion = "{version}"\n', encoding="utf-8"
        )

    def test_matching_tag(self):
        self.assertEqual(validate_tag(self.manifest, "v0.1.0"), "0.1.0")

    def test_mismatches_and_non_version_refs(self):
        for tag in ["v0.2.0", "0.1.0", "main", "v0.1.0-rc.1", "v0.1.0\n"]:
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                validate_tag(self.manifest, tag)

    def test_prerelease_requires_exact_match(self):
        self.write_package("0.2.0-rc.1")
        self.assertEqual(validate_tag(self.manifest, "v0.2.0-rc.1"), "0.2.0-rc.1")
        with self.assertRaises(ValueError):
            validate_tag(self.manifest, "v0.2.0")

    def test_renamed_package_is_rejected(self):
        for name in ["another-crate", "pikachu-cdc"]:
            with self.subTest(name=name):
                self.write_package("0.1.0", name=name)
                with self.assertRaises(ValueError):
                    validate_tag(self.manifest, "v0.1.0")


if __name__ == "__main__":
    unittest.main()
