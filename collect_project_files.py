#!/usr/bin/env python3
"""
Panopticon Project File Aggregator

Collects all relevant project source code, documentation, configuration, and schema
files into unified review bundles:
  - 'project_for_review.md': Rich Markdown document with syntax highlighting,
                             clickable Table of Contents, and file anchors.
  - 'project_for_review.txt': Clean plain-text compilation with standard headers.

Excludes test suites, raw data payloads/samples, and redundant .docx folders.
"""

# ==============================================================================
# PIPELINE DEPENDENCY & IMPACT TRACKING
# ==============================================================================
# Upstream Dependencies:
#   - Project source files (.py, .md, .json, .txt) across notebooks, Docs, data/schemas
# Downstream Impact:
#   - Generates: project_for_review.md (rich markdown review bundle with TOC)
#   - Generates: project_for_review.txt (plain-text compilation)
# Risk & Side Effects:
#   - None. Read-only aggregation utility. Does not alter lakehouse data or schemas.
# ==============================================================================

import argparse
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Set, Tuple


DEFAULT_TARGET_EXTENSIONS: Set[str] = {
    ".py",    # Python source/code files
    ".md",    # Markdown/documentation files
    ".json",  # Configuration and schema files
    ".txt",   # Text and document files
}

DEFAULT_EXCLUDED_DIRS: Set[str] = {
    ".git",
    ".github",
    ".idea",
    ".vscode",
    "venv",
    ".venv",
    "env",
    ".env",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "node_modules",
    ".ipynb_checkpoints",
    ".agents",
    ".gemini",
    ".system_generated",
    "build",
    "dist",
    ".docx",   # Redundant academic duplicates per project directives
    "assets",  # Binary media / raster assets
    # Tests excluded for code review
    "tests",
    "test",
    # Actual data payloads / sample dumps excluded (schemas are retained under data/schemas)
    "samples",
    "raw",
    "bronze",
    "silver",
    "quarantine",
    "logs",
    "scratch",
}

DEFAULT_EXCLUDED_FILES: Set[str] = {
    "project_for_review.txt",  # Prevent self-inclusion / recursion
    "project_for_review.md",   # Prevent self-inclusion / recursion
}

LANGUAGE_MAP = {
    ".py": "python",
    ".json": "json",
    ".md": "markdown",
    ".txt": "text",
    ".sql": "sql",
    ".yml": "yaml",
    ".yaml": "yaml",
}


def should_exclude_dir(dir_name: str, excluded_dirs: Set[str]) -> bool:
    """Determine whether a directory should be skipped during traversal."""
    if dir_name in excluded_dirs:
        return True
    if dir_name.startswith(".") and dir_name != ".":
        return True
    return False


def make_anchor(rel_path: str) -> str:
    """Creates a URL-safe markdown anchor slug for a relative file path."""
    slug = re.sub(r"[^a-zA-Z0-9_-]", "-", rel_path.lower().replace("/", "-").replace(" ", "-"))
    slug = re.sub(r"-+", "-", slug).strip("-")
    return f"file-{slug}"


def collect_files(
    root_dir: Path,
    target_extensions: Set[str],
    excluded_dirs: Set[str],
    excluded_files: Set[str],
) -> List[Path]:
    """
    Recursively scans the root directory and collects all matching files
    while pruning excluded directories, tests, data payloads, and ignored files.
    """
    matched_files: List[Path] = []

    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Prune excluded directories in-place to prevent traversing them
        dirnames[:] = [d for d in dirnames if not should_exclude_dir(d, excluded_dirs)]

        for filename in filenames:
            # Skip temporary or lock files
            if filename.startswith("~$") or filename.endswith(".tmp"):
                continue

            # Skip standalone test scripts
            if filename.startswith("test_") or filename.endswith("_test.py"):
                continue

            file_path = Path(dirpath) / filename

            # Check if file name is specifically excluded
            if filename in excluded_files or file_path.name in excluded_files:
                continue

            # Check extension (case-insensitive)
            if file_path.suffix.lower() in target_extensions:
                matched_files.append(file_path)

    # Sort deterministically by POSIX relative path
    matched_files.sort(key=lambda p: p.relative_to(root_dir).as_posix())
    return matched_files


def aggregate_files_txt(
    root_dir: Path,
    files_to_collect: List[Path],
    output_path: Path,
) -> Tuple[int, int]:
    """
    Writes all collected files into a single plain text file with clear headers
    and an explicit review exclusion notice.
    Returns (total_files_written, total_lines_written).
    """
    total_lines = 0

    with open(output_path, "w", encoding="utf-8", newline="\n") as out:
        # Header banner for the entire bundle with review exclusion notes
        banner = (
            "=" * 80 + "\n"
            + "PROJECT PANOPTICON - CODEBASE REVIEW BUNDLE\n"
            + f"Root Directory: {root_dir.name}\n"
            + f"Total Files Collected: {len(files_to_collect)}\n"
            + "\n"
            + "REVIEW EXCLUSION NOTICES:\n"
            + "  - Tests: We didn't include tests because we thought tests for review will not be needed.\n"
            + "  - Data: Actual data payloads and samples (data/samples/, data/raw/) are excluded;\n"
            + "          only schemas (data/schemas/) and configuration files are retained.\n"
            + "  - Documents (.docx): The Docs/.docx folder is excluded because it contains the\n"
            + "          same content as the markdown (.md) documentation files.\n"
            + "=" * 80 + "\n\n"
        )
        out.write(banner)
        total_lines += banner.count("\n")

        for file_path in files_to_collect:
            rel_path = file_path.relative_to(root_dir).as_posix()

            try:
                content = file_path.read_text(encoding="utf-8", errors="replace")
            except Exception as e:
                content = f"[ERROR: Unable to read file {rel_path} - {e}]\n"

            header = (
                "=" * 80 + "\n"
                + f"FILE: {rel_path}\n"
                + "=" * 80 + "\n"
            )
            out.write(header)
            out.write(content)

            if not content.endswith("\n"):
                out.write("\n")
            out.write("\n")

            total_lines += header.count("\n") + content.count("\n") + (1 if not content.endswith("\n") else 0) + 1

    return len(files_to_collect), total_lines


def aggregate_files_md(
    root_dir: Path,
    files_to_collect: List[Path],
    output_path: Path,
) -> Tuple[int, int]:
    """
    Writes all collected files into a rich Markdown (.md) document featuring:
      - Clean styling and review metadata
      - Clickable Table of Contents
      - Syntax-highlighted code fences
      - Fail-safe outer backticks to prevent markdown escaping
      - Navigation links back to the Table of Contents
    Returns (total_files_written, total_lines_written).
    """
    total_lines = 0

    with open(output_path, "w", encoding="utf-8", newline="\n") as out:
        # Title and review notices
        header_section = (
            "# Project Panopticon — Codebase & Documentation Review Bundle\n\n"
            f"> **Project Root**: `{root_dir.name}`  \n"
            f"> **Total Files Collected**: `{len(files_to_collect)}`  \n"
            f"> **Generated**: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`  \n\n"
            "## Review Exclusion Notices\n\n"
            "- **Tests Excluded**: We didn't include tests because we thought tests for review will not be needed.\n"
            "- **Data Excluded**: Actual data payloads and samples (`data/samples/`, `data/raw/`) are excluded; only schemas (`data/schemas/`) and configuration files are retained.\n"
            "- **Documents (`.docx`) Excluded**: The `Docs/.docx/` directory is excluded because it contains the exact same content as the Markdown (`.md`) documentation files.\n\n"
            "---\n\n"
            '<a id="table-of-contents"></a>\n'
            "## Table of Contents\n\n"
        )
        out.write(header_section)
        total_lines += header_section.count("\n")

        # Build clickable Table of Contents
        toc_lines = []
        for file_path in files_to_collect:
            rel_path = file_path.relative_to(root_dir).as_posix()
            anchor = make_anchor(rel_path)
            toc_lines.append(f"- [`{rel_path}`](#{anchor})\n")

        toc_block = "".join(toc_lines) + "\n---\n\n"
        out.write(toc_block)
        total_lines += toc_block.count("\n")

        # Output each file with syntax-highlighted code fencing
        for file_path in files_to_collect:
            rel_path = file_path.relative_to(root_dir).as_posix()
            anchor = make_anchor(rel_path)
            lang = LANGUAGE_MAP.get(file_path.suffix.lower(), "text")

            try:
                content = file_path.read_text(encoding="utf-8", errors="replace")
            except Exception as e:
                content = f"[ERROR: Unable to read file {rel_path} - {e}]\n"

            # Determine outer backtick count to prevent nested markdown fences from breaking
            backtick_matches = re.findall(r"`+", content)
            max_backticks = max([len(m) for m in backtick_matches] or [0])
            fence = "`" * max(4, max_backticks + 1)

            file_header = (
                f'<a id="{anchor}"></a>\n'
                f"## File: `{rel_path}`\n\n"
                f"[↑ Back to Table of Contents](#table-of-contents)\n\n"
                f"{fence}{lang}\n"
            )
            out.write(file_header)

            out.write(content)
            if not content.endswith("\n"):
                out.write("\n")

            file_footer = f"{fence}\n\n---\n\n"
            out.write(file_footer)

            total_lines += (
                file_header.count("\n")
                + content.count("\n")
                + (1 if not content.endswith("\n") else 0)
                + file_footer.count("\n")
            )

    return len(files_to_collect), total_lines


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect project source files, documentation, and schemas into project_for_review.md / .txt"
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="Root directory of the project (defaults to script directory).",
    )
    parser.add_argument(
        "--format",
        choices=["both", "md", "txt"],
        default="both",
        help="Output format: 'md' (Markdown with TOC), 'txt' (plain text), or 'both' (default).",
    )

    args = parser.parse_args()
    root_dir = args.root.resolve()

    # Base paths
    md_output_path = root_dir / "project_for_review.md"
    txt_output_path = root_dir / "project_for_review.txt"

    # Ensure output files are in the excluded set
    excluded_files = set(DEFAULT_EXCLUDED_FILES)
    excluded_files.add(md_output_path.name)
    excluded_files.add(txt_output_path.name)

    print(f"Scanning project root: {root_dir}")
    print(f"Target extensions: {', '.join(sorted(DEFAULT_TARGET_EXTENSIONS))}")
    print("Excluded: tests/, test_*.py, data/samples/ (actual data), data/raw/, Docs/.docx/, assets/")

    collected = collect_files(
        root_dir=root_dir,
        target_extensions=DEFAULT_TARGET_EXTENSIONS,
        excluded_dirs=DEFAULT_EXCLUDED_DIRS,
        excluded_files=excluded_files,
    )

    print(f"\nFound {len(collected)} matching files:")
    for p in collected:
        print(f"  - {p.relative_to(root_dir).as_posix()}")

    print("\nGenerating review bundles...")

    if args.format in ("both", "md"):
        file_count_md, line_count_md = aggregate_files_md(
            root_dir=root_dir,
            files_to_collect=collected,
            output_path=md_output_path,
        )
        md_size_kb = md_output_path.stat().st_size / 1024
        print(f"  - [Markdown] {md_output_path} ({file_count_md} files, {line_count_md:,} lines, {md_size_kb:.1f} KB)")

    if args.format in ("both", "txt"):
        file_count_txt, line_count_txt = aggregate_files_txt(
            root_dir=root_dir,
            files_to_collect=collected,
            output_path=txt_output_path,
        )
        txt_size_kb = txt_output_path.stat().st_size / 1024
        print(f"  - [Plain Text] {txt_output_path} ({file_count_txt} files, {line_count_txt:,} lines, {txt_size_kb:.1f} KB)")

    print("\nAggregation Complete! Both Markdown (.md) and Plain Text (.txt) formats are ready.")


if __name__ == "__main__":
    main()
