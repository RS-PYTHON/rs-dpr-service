# Copyright 2023-2026 Airbus, CS Group
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Semantic checks for TaskTable files (complements the JSON Schema validation).

Usage:
    python scripts/check_tasktable_semantics.py config/TaskTable_*.json
"""

import argparse
import json
import re
import sys
from pathlib import Path


def validate_query_parameters(config: dict) -> list[str]:
    """
    Validate that each query call in IO alternatives uses exactly
    the parameters declared in the corresponding query definition.
    """
    errors = []

    # Index query definitions by name for fast lookup
    query_definitions = {
        q.get("name"): q.get("parameters", []) for q in config.get("queries", []) if isinstance(q, dict)
    }

    # Walk through IO entries and inspect their alternatives
    for io_type in ["input", "output", "adfs"]:
        for io_entry in config.get("io", {}).get(io_type, []):
            io_name = io_entry.get("name")

            for alt in io_entry.get("alternatives", []):
                query = alt.get("query", {})
                query_name = query.get("name")
                params = query.get("parameters", {})

                # Skip alternatives without a query
                if not query_name:
                    continue

                # Check that the query is declared
                if query_name not in query_definitions:
                    errors.append(
                        f'Query "{query_name}" referenced in IO "{io_name}" (type: {io_type}) '
                        f'is not defined in the "queries" section.',
                    )
                    continue

                expected = set(query_definitions[query_name])
                provided = set(params.keys())

                # Missing parameters
                missing = expected - provided
                if missing:
                    errors.append(
                        f'Query "{query_name}" in IO "{io_name}" (type: {io_type}) '
                        f"is missing parameters: {sorted(missing)}",
                    )

                # Extra parameters
                extra = provided - expected
                if extra:
                    errors.append(
                        f'Query "{query_name}" in IO "{io_name}" (type: {io_type}) '
                        f"has unexpected parameters: {sorted(extra)}",
                    )

    return errors


def validate_pipeline_input_references_to_outputs(tasktable_json: dict) -> list[str]:
    """
    Check that each input_products value in pipelines.steps is either:
    - "pipeline_input"
    - OR "UnitName.OutputName" where UnitName exists (optionally with a
      "_<step_id>" suffix) and OutputName is a valid output of that unit.
    """
    errors = []

    # Build: unit_name -> set(output_names)
    unit_outputs = {
        unit["name"]: {out["name"] for out in unit.get("output_products", [])}
        for unit in tasktable_json.get("units", [])
    }

    for pipeline in tasktable_json.get("pipelines", []):
        pname = pipeline["name"]

        for step in pipeline.get("steps", []):
            step_unit = step["unit_name"]

            for input_name, ref in step.get("input_products", {}).items():
                if ref == "pipeline_input":
                    continue

                if "." not in ref:
                    errors.append(
                        f"[Pipeline {pname}] Step '{step_unit}' input '{input_name}' "
                        f"has invalid reference '{ref}'. Expected 'Unit.Output'.",
                    )
                    continue

                unit_ref, output_ref = ref.split(".", 1)

                # Try the exact unit name first, otherwise strip a trailing
                # "_<number>" step suffix (e.g. "Calibration_1" -> "Calibration").
                # This avoids breaking unit names that contain underscores.
                if unit_ref in unit_outputs:
                    base_unit_ref = unit_ref
                else:
                    base_unit_ref = re.sub(r"_\d+$", "", unit_ref)

                if base_unit_ref not in unit_outputs:
                    errors.append(
                        f"[Pipeline {pname}] Step '{step_unit}' input '{input_name}' "
                        f"references unknown unit '{unit_ref}'.",
                    )
                    continue

                if output_ref not in unit_outputs[base_unit_ref]:
                    errors.append(
                        f"[Pipeline {pname}] Step '{step_unit}' input '{input_name}' "
                        f"references '{unit_ref}.{output_ref}', but this output does not exist.",
                    )

    return errors


def validate_pipeline_inputs(tasktable_json: dict) -> list[str]:
    """
    Check consistency between:
    - units[*].input_products[*].name
    - pipelines[*].steps[*].input_products
    """
    errors = []

    # Build: unit_name -> set(input_names)
    unit_inputs = {
        unit["name"]: {inp["name"] for inp in unit.get("input_products", [])}
        for unit in tasktable_json.get("units", [])
    }

    # Track which unit inputs are actually used by pipelines
    used_inputs = {unit: set() for unit in unit_inputs}

    # Validate pipeline -> unit
    for pipeline in tasktable_json.get("pipelines", []):
        pname = pipeline["name"]

        for step in pipeline.get("steps", []):
            unit_name = step["unit_name"]

            if unit_name not in unit_inputs:
                errors.append(f"[Pipeline {pname}] Unknown unit '{unit_name}'")
                continue

            step_inputs = set(step.get("input_products", {}).keys())
            used_inputs[unit_name].update(step_inputs)

            missing = step_inputs - unit_inputs[unit_name]
            if missing:
                errors.append(
                    f"[Pipeline {pname}] Unit '{unit_name}' does not declare input(s): "
                    f"{', '.join(sorted(missing))}",
                )

    # Validate unit -> pipeline
    for unit_name, declared in unit_inputs.items():
        unused = declared - used_inputs[unit_name]
        if unused:
            errors.append(f"[Unit {unit_name}] Declares unused input(s): {', '.join(sorted(unused))}")

    return errors


def validate_unit_io_consistency(tasktable_json: dict) -> list[str]:
    """
    Check that:
    - Every input_products.name in units is defined in io.input.
    - Every input_adfs.name in units is defined in io.adfs.
    - Every output_products.name in units is defined in io.output.
    - Every io.input / io.adfs / io.output entry is used by at least one unit.
    """
    errors = []
    io = tasktable_json.get("io", {})

    # Names defined in the io section
    io_input_names = {e.get("name") for e in io.get("input", [])}
    io_adfs_names = {e.get("name") for e in io.get("adfs", [])}
    io_output_names = {e.get("name") for e in io.get("output", [])}

    # Names used by the units
    unit_input_names, unit_adfs_names, unit_output_names = set(), set(), set()

    for unit in tasktable_json.get("units", []):
        unit_name = unit.get("name")

        for inp in unit.get("input_products", []):
            name = inp.get("name")
            unit_input_names.add(name)
            if name not in io_input_names:
                errors.append(f"[Unit {unit_name}] input_products '{name}' is not defined in io.input.")

        for adfs in unit.get("input_adfs", []):
            name = adfs.get("name")
            unit_adfs_names.add(name)
            if name not in io_adfs_names:
                errors.append(f"[Unit {unit_name}] input_adfs '{name}' is not defined in io.adfs.")

        for out in unit.get("output_products", []):
            name = out.get("name")
            unit_output_names.add(name)
            if name not in io_output_names:
                errors.append(f"[Unit {unit_name}] output_products '{name}' is not defined in io.output.")

    # Check that every io entry is used by at least one unit
    for label, io_names, used in [
        ("io.input", io_input_names, unit_input_names),
        ("io.adfs", io_adfs_names, unit_adfs_names),
        ("io.output", io_output_names, unit_output_names),
    ]:
        unused = io_names - used
        if unused:
            errors.append(f"{label} names not used in any unit: {', '.join(sorted(map(str, unused)))}")

    return errors


def validate_external_variables(tasktable_json: dict) -> list[str]:
    """
    Check that:
    - All declared external_variables are used at least once with the
      syntax {external_variable.XX}.
    - No external variable is used without being declared.
    """
    errors = []

    # Declared external variables (expected to be a list of strings)
    raw = tasktable_json.get("external_variables", [])
    declared = set(raw) if isinstance(raw, list) else set()

    # Find all usages in the whole document
    used = set(re.findall(r"\{external_variable\.(\w+)\}", json.dumps(tasktable_json)))

    unused = declared - used
    if unused:
        errors.append(f"Declared external variables not used: {', '.join(sorted(unused))}")

    undeclared = used - declared
    if undeclared:
        errors.append(f"Used external variables not declared: {', '.join(sorted(undeclared))}")

    return errors


# (success message, validator function)
VALIDATORS = [
    ("Pipeline inputs match unit input definitions", validate_pipeline_inputs),
    ("Pipeline inputs correctly reference unit outputs", validate_pipeline_input_references_to_outputs),
    ("Unit inputs/outputs are consistent with io", validate_unit_io_consistency),
    ("External variables are correctly declared and used", validate_external_variables),
    ("CQL2 query parameters are valid", validate_query_parameters),
]


def check_file(path: Path) -> int:
    """Run all semantic checks on a file and return the number of errors."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"::error file={path}::Invalid JSON: {exc}")
        return 1

    total = 0
    for label, validator in VALIDATORS:
        errors = validator(payload)
        if errors:
            print(f"❌ {label}")
            for e in errors:
                print(f"   - {e}")
                # GitHub Actions annotation (shown in the PR "Files changed" / checks)
                print(f"::error file={path}::{e}")
            total += len(errors)
        else:
            print(f"✔️ {label}")
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description="Semantic checks for TaskTable JSON files.")
    parser.add_argument("files", nargs="+", type=Path, help="TaskTable JSON files to check")
    args = parser.parse_args()

    failed = 0
    for path in args.files:
        print(f"\n=== {path} ===")
        if check_file(path):
            failed += 1

    # Non-zero exit code makes the CI job fail
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
