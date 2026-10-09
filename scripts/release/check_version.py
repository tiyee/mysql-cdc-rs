"""Reject release tags that do not identify the crate version being published."""

import argparse
from pathlib import Path
import sys
import tomllib


def validate_tag(manifest: Path, tag: str) -> str:
    with manifest.open("rb") as source:
        package = tomllib.load(source)["package"]
    if package["name"] != "pikachu-rs":
        raise ValueError("release.yml publishes pikachu-rs; update it before renaming the crate")
    version = package["version"]
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
        default=Path(__file__).resolve().parents[2] / "Cargo.toml",
    )
    args = parser.parse_args()
    try:
        version = validate_tag(args.manifest, args.tag)
    except (OSError, ValueError, KeyError) as error:
        print(f"Release rejected: {error}", file=sys.stderr)
        return 1
    print(f"Validated pikachu-rs {version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
