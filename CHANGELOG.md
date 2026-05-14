#!/usr/bin/env bash
set -euo pipefail

CHANGELOG_FILE="${1:-CHANGELOG.md}"

if [[ ! -f "$CHANGELOG_FILE" ]]; then
  echo "error: changelog file not found: $CHANGELOG_FILE" >&2
  exit 1
fi

python3 - "$CHANGELOG_FILE" <<'PY'
import os
import re
import subprocess
import sys
from datetime import date

path = sys.argv[1]

recognized_sections = {
    "🚀 Features",
    "🐛 Bug Fixes",
    "⚙️ Miscellaneous Tasks",
    "🧪 Testing",
    "📚 Documentation",
    "⚡ Performance",
    "🚜 Refactor",
    "💼 Other",
}

release_header_re = re.compile(r"^## \[(\d+)\.(\d+)\.(\d+)\] - (\d{4}-\d{2}-\d{2})\s*$")
section_header_re = re.compile(r"^### (.+?)\s*$")
bullet_re = re.compile(r"^- .+\S\s*$")
issue_ref_re = re.compile(r"\(#\d+\)|#\d+")
semver_tag_re = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)$")

require_issue_refs = os.environ.get("CHANGELOG_REQUIRE_ISSUE_REFS", "").lower() in {
    "1",
    "true",
    "yes",
}

check_latest_tag = os.environ.get("CHANGELOG_CHECK_LATEST_TAG", "").lower() in {
    "1",
    "true",
    "yes",
}

with open(path, "r", encoding="utf-8") as f:
    lines = f.read().splitlines()

errors = []
releases = []
current = None

for line_number, line in enumerate(lines, start=1):
    header_match = release_header_re.match(line)

    if line.startswith("## [") and not header_match:
        errors.append(
            f"{path}:{line_number}: malformed release header. Expected: ## [MAJOR.MINOR.PATCH] - YYYY-MM-DD"
        )
        current = None
        continue

    if header_match:
        major, minor, patch, release_date = header_match.groups()
        version_tuple = (int(major), int(minor), int(patch))
        version = f"{major}.{minor}.{patch}"

        try:
            parsed_date = date.fromisoformat(release_date)
        except ValueError:
            errors.append(f"{path}:{line_number}: invalid release date: {release_date}")
            parsed_date = None

        current = {
            "version": version,
            "version_tuple": version_tuple,
            "date": parsed_date,
            "date_text": release_date,
            "line": line_number,
            "sections": [],
            "bullets": [],
        }
        releases.append(current)
        continue

    if current is None:
        continue

    section_match = section_header_re.match(line)
    if section_match:
        section = section_match.group(1).strip()
        current["sections"].append((section, line_number))
        if section not in recognized_sections:
            errors.append(
                f"{path}:{line_number}: unrecognized changelog section '{section}'"
            )
        continue

    stripped = line.strip()
    if stripped.startswith("-"):
        if not bullet_re.match(stripped):
            errors.append(
                f"{path}:{line_number}: malformed bullet. Expected '- ' followed by non-empty text"
            )
        current["bullets"].append((stripped, line_number))

        if require_issue_refs and not issue_ref_re.search(stripped):
            errors.append(
                f"{path}:{line_number}: bullet is missing an issue/PR reference like #123"
            )

if not releases:
    errors.append(f"{path}: no release entries found")

seen_versions = {}
for release in releases:
    version = release["version"]
    if version in seen_versions:
        errors.append(
            f"{path}:{release['line']}: duplicate version header '{version}' "
            f"(first seen on line {seen_versions[version]})"
        )
    else:
        seen_versions[version] = release["line"]

for previous, current_release in zip(releases, releases[1:]):
    if previous["version_tuple"] <= current_release["version_tuple"]:
        errors.append(
            f"{path}:{current_release['line']}: versions must be in descending semantic-version order. "
            f"Found {previous['version']} before {current_release['version']}"
        )

    if previous["date"] and current_release["date"] and previous["date"] < current_release["date"]:
        errors.append(
            f"{path}:{current_release['line']}: release dates must be descending. "
            f"Found {previous['date_text']} before newer date {current_release['date_text']}"
        )

for release in releases:
    recognized = [
        section
        for section, _ in release["sections"]
        if section in recognized_sections
    ]

    if not recognized:
        errors.append(
            f"{path}:{release['line']}: release {release['version']} must contain at least one recognized section"
        )

    if not release["bullets"]:
        errors.append(
            f"{path}:{release['line']}: release {release['version']} must contain at least one bullet entry"
        )

if check_latest_tag and releases:
    latest_changelog_version = releases[0]["version"]

    try:
        latest_tag = subprocess.check_output(
            ["git", "describe", "--tags", "--abbrev=0"],
            stderr=subprocess.STDOUT,
            text=True,
        ).strip()
    except subprocess.CalledProcessError as exc:
        errors.append(
            "failed to determine latest Git tag with 'git describe --tags --abbrev=0': "
            + exc.output.strip()
        )
        latest_tag = ""

    if latest_tag:
        tag_match = semver_tag_re.match(latest_tag)
        if not tag_match:
            errors.append(
                f"latest Git tag '{latest_tag}' is not a semantic version tag"
            )
        else:
            latest_tag_version = ".".join(tag_match.groups())
            if latest_tag_version != latest_changelog_version:
                errors.append(
                    f"latest Git tag '{latest_tag}' does not match latest changelog entry "
                    f"'{latest_changelog_version}'"
                )

if errors:
    print("Changelog validation failed:", file=sys.stderr)
    for error in errors:
        print(f"- {error}", file=sys.stderr)
    sys.exit(1)

print(f"Changelog validation passed: {len(releases)} release entries checked.")
PY