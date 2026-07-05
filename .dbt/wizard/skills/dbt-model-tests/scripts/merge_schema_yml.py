#!/usr/bin/env python3
"""Idempotently merge dbt tests into a co-located schema.yml.

Preserves existing description/columns/tests; only adds tests that are not
already present (compared as normalized structures). Creates the file and the
model entry if missing. Writes only when there is a real diff.

Usage:
    python3 merge_schema_yml.py \
        --schema-path path/to/schema.yml \
        --model-name my_model \
        --spec-json '{"columns": [{"name": "id", "tests": ["not_null", "unique"]}], "model_tests": []}'

Exit codes:
    0 - success (may be no-op)
    1 - invalid input / malformed existing YAML
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from ruamel.yaml import YAML
except ImportError:  # pragma: no cover
    sys.stderr.write(
        "ruamel.yaml is required. Install with: pip install ruamel.yaml\n"
    )
    sys.exit(1)


def _normalize(test):
    """Return a hashable canonical form for a dbt test spec."""
    if isinstance(test, str):
        return ("str", test)
    if isinstance(test, dict):
        if len(test) != 1:
            return ("dict", json.dumps(test, sort_keys=True, default=str))
        (key, val) = next(iter(test.items()))
        if isinstance(val, dict):
            return (
                "dict",
                key,
                json.dumps(val, sort_keys=True, default=str),
            )
        return ("dict", key, json.dumps(val, sort_keys=True, default=str))
    return ("other", json.dumps(test, sort_keys=True, default=str))


def _merge_tests(existing, incoming):
    """Return a merged test list; only appends items whose normalized form is new."""
    existing = list(existing or [])
    seen = {_normalize(t) for t in existing}
    added = 0
    for test in incoming or []:
        key = _normalize(test)
        if key in seen:
            continue
        seen.add(key)
        existing.append(test)
        added += 1
    return existing, added


def _find_model_entry(models_list, model_name):
    for entry in models_list or []:
        if isinstance(entry, dict) and entry.get("name") == model_name:
            return entry
    return None


def merge(schema_path: Path, model_name: str, spec: dict):
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=2)

    if schema_path.exists():
        with schema_path.open("r") as fh:
            data = yaml.load(fh) or {}
    else:
        data = {}

    if not isinstance(data, dict):
        raise ValueError(f"Top-level YAML in {schema_path} is not a mapping.")

    data.setdefault("version", 2)
    models = data.setdefault("models", [])

    entry = _find_model_entry(models, model_name)
    created_entry = False
    if entry is None:
        entry = {"name": model_name}
        models.append(entry)
        created_entry = True

    total_added = 0

    # Model-level tests (e.g. dbt_utils.unique_combination_of_columns)
    incoming_model_tests = spec.get("model_tests") or []
    if incoming_model_tests:
        merged, added = _merge_tests(entry.get("tests"), incoming_model_tests)
        if added:
            entry["tests"] = merged
        total_added += added

    # Column-level tests
    incoming_columns = spec.get("columns") or []
    if incoming_columns:
        existing_columns = entry.setdefault("columns", [])
        by_name = {
            col.get("name"): col
            for col in existing_columns
            if isinstance(col, dict) and col.get("name")
        }
        for col_spec in incoming_columns:
            name = col_spec.get("name")
            if not name:
                continue
            tests = col_spec.get("tests") or []
            if name not in by_name:
                new_col = {"name": name}
                if tests:
                    new_col["tests"] = list(tests)
                    total_added += len(tests)
                existing_columns.append(new_col)
                by_name[name] = new_col
                continue
            col = by_name[name]
            merged, added = _merge_tests(col.get("tests"), tests)
            if added:
                col["tests"] = merged
            total_added += added

    if not created_entry and total_added == 0:
        return {"status": "noop", "added": 0, "path": str(schema_path)}

    schema_path.parent.mkdir(parents=True, exist_ok=True)
    with schema_path.open("w") as fh:
        yaml.dump(data, fh)

    return {
        "status": "wrote",
        "added": total_added,
        "created_entry": created_entry,
        "path": str(schema_path),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema-path", required=True, type=Path)
    parser.add_argument("--model-name", required=True)
    parser.add_argument(
        "--spec-json",
        required=True,
        help='JSON: {"columns": [...], "model_tests": [...]}',
    )
    args = parser.parse_args(argv)

    try:
        spec = json.loads(args.spec_json)
    except json.JSONDecodeError as exc:
        sys.stderr.write(f"Invalid --spec-json: {exc}\n")
        return 1

    if not isinstance(spec, dict):
        sys.stderr.write("--spec-json must decode to an object.\n")
        return 1

    try:
        result = merge(args.schema_path, args.model_name, spec)
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"Merge failed: {exc}\n")
        return 1

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
