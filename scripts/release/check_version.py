"""Reject release tags that do not identify the crate version being published."""

import argparse
from pathlib import Path
import sys
import tomllib


def validate_tag(manifest: Path, tag: str) -> str:
    with manifest.open("rb") as source:
        data = tomllib.load(source)
    package = data["package"]
    if package["name"] != "pikachu":
        raise ValueError("release.yml publishes pikachu; update it before renaming the crate")
    version = package["version"]
    manifests = {"pikachu": manifest}
    packages = {"pikachu": data}
    for name, directory in [
        ("mysql-canal", "mysql-canal"),
        ("mysql-replication", "mysql-replication"),
        ("mysql-cdc-rs", "mysql-cdc"),
    ]:
        manifests[name] = manifest.parent.parent / directory / "Cargo.toml"
        with manifests[name].open("rb") as source:
            packages[name] = tomllib.load(source)
        library = packages[name]["package"]
        if library["name"] != name or library["version"] != version:
            raise ValueError(f"{name} and pikachu must have matching release versions")
    for parent, name, dependency_path in [
        ("pikachu", "mysql-canal", "../mysql-canal"),
        ("mysql-canal", "mysql-replication", "../mysql-replication"),
        ("mysql-cdc-rs", "mysql-canal", "../mysql-canal"),
        ("mysql-cdc-rs", "mysql-replication", "../mysql-replication"),
    ]:
        dependency = packages[parent]["dependencies"][name]
        if dependency.get("version") != f"={version}" or dependency.get("path") != dependency_path:
            raise ValueError(f"{manifests[parent]} must depend on the exact workspace {name} version")
    expected = f"v{version}"
    if tag != expected:
        raise ValueError(f"release tag {tag!r} does not match Cargo.toml; expected {expected!r}")
    return version


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag", help="Git tag, e.g. v0.0.1")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "crates/pikachu/Cargo.toml",
    )
    args = parser.parse_args()
    try:
        version = validate_tag(args.manifest, args.tag)
    except (OSError, ValueError, KeyError) as error:
        print(f"Release rejected: {error}", file=sys.stderr)
        return 1
    print(f"Validated mysql-replication, mysql-canal, mysql-cdc-rs and pikachu {version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
