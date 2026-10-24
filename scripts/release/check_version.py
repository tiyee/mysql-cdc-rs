"""Reject release tags that do not identify the crate version being published."""

import argparse
from pathlib import Path
import sys
import tomllib


def validate_tag(workspace: Path, tag: str) -> str:
    with workspace.open("rb") as source:
        root = tomllib.load(source)
    if "package" in root:
        raise ValueError("release validation requires the pure workspace manifest")
    expected_members = {
        "crates/mysql-replication": "mysql-replication",
        "crates/mysql-canal": "mysql-canal",
        "crates/mysql-cdc": "mysql-cdc-rs",
        "crates/pikachu": "pikachu",
    }
    members = root["workspace"]["members"]
    if len(members) != len(expected_members) or set(members) != set(expected_members):
        raise ValueError("release workspace must contain exactly the four project packages")
    packages = {}
    manifests = {}
    for directory, name in expected_members.items():
        manifest = workspace.parent / directory / "Cargo.toml"
        with manifest.open("rb") as source:
            data = tomllib.load(source)
        if data["package"]["name"] != name:
            raise ValueError(f"{manifest} must declare package {name}")
        packages[name] = data
        manifests[name] = manifest
    versions = {data["package"]["version"] for data in packages.values()}
    if len(versions) != 1:
        raise ValueError("all four packages must have the same release version")
    version = versions.pop()
    for parent, name, dependency_path in [
        ("pikachu", "mysql-canal", "../mysql-canal"),
        ("mysql-canal", "mysql-replication", "../mysql-replication"),
        ("mysql-cdc-rs", "mysql-canal", "../mysql-canal"),
        ("mysql-cdc-rs", "mysql-replication", "../mysql-replication"),
    ]:
        dependency = packages[parent]["dependencies"][name]
        if dependency.get("version") != f"={version}" or dependency.get("path") != dependency_path:
            raise ValueError(f"{manifests[parent]} must depend on the exact workspace {name} version")
    with workspace.with_name("Cargo.lock").open("rb") as source:
        lock = tomllib.load(source)
    for name in packages:
        entries = [entry for entry in lock["package"] if entry["name"] == name and "source" not in entry]
        if len(entries) != 1 or entries[0]["version"] != version:
            raise ValueError(f"Cargo.lock must contain workspace {name} {version}")
    expected = f"v{version}"
    if tag != expected:
        raise ValueError(f"release tag {tag!r} does not match Cargo.toml; expected {expected!r}")
    return version


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag", help="Git tag, e.g. v2.0.1")
    parser.add_argument(
        "--workspace",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "Cargo.toml",
    )
    args = parser.parse_args()
    try:
        version = validate_tag(args.workspace, args.tag)
    except (OSError, ValueError, KeyError) as error:
        print(f"Release rejected: {error}", file=sys.stderr)
        return 1
    print(f"Validated mysql-replication, mysql-canal, mysql-cdc-rs and pikachu {version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
