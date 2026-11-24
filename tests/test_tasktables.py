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
Semantic checks of the task tables, complementing the JSON schema validation.

The rules follow how rs-client-libraries (rs_workflows.payload_builder) consumes the task tables
to build the processor payloads, so that an inconsistent task table is caught here and not at runtime.
"""

import json
import re
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from rs_dpr_service.processors.eopf_processors import CONFIG_DIR

IO_SECTIONS = ("input", "adfs", "output")

# Unit I/O field -> "io" section where the product must be described
UNIT_IO_FIELDS = {"input_products": "input", "input_adfs": "adfs", "output_products": "output"}

# Query placeholder: the whole string is "{parameter}"
QUERY_PARAMETER_PATTERN = re.compile(r"^\{([^.{}]+)\}$")

# External variable placeholder, anywhere in a string: "{external_variable.name}"
EXTERNAL_VARIABLE_PATTERN = re.compile(r"\{external_variable\.(\w+)\}")

# Origin of a pipeline step input coming from another step: "unit.product" or "unit.step_id.product"
STEP_ORIGIN_PATTERN = re.compile(r"^(?P<unit>[^.]+)(\.(?P<step_id>\d+))?\.(?P<product>[^.]+)$")


def _strings(obj: Any) -> Iterator[str]:
    """Yield all the string values of a JSON object, recursively."""
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, list):
        for item in obj:
            yield from _strings(item)
    elif isinstance(obj, dict):
        for item in obj.values():
            yield from _strings(item)


def _query_calls(tasktable: dict) -> Iterator[tuple[str, dict]]:
    """Yield (io product name, query call) for each query called by an "io" product alternative."""
    for section in IO_SECTIONS:
        for product in tasktable.get("io", {}).get(section, []):
            for alternative in product.get("alternatives", []):
                if "query" in alternative:
                    yield product["name"], alternative["query"]


def _duplicates(names: list[str]) -> list[str]:
    """Return the names given several times, sorted."""
    return sorted({name for name in names if names.count(name) > 1})


def check_unique_names(tasktable: dict) -> list[str]:
    """The "io" products of each section, the queries, the units and the pipelines have unique names."""
    named_lists = {f"io.{section}": tasktable.get("io", {}).get(section, []) for section in IO_SECTIONS}
    named_lists |= {section: tasktable.get(section, []) for section in ("queries", "units", "pipelines")}
    return [
        f"Several {label} are named '{name}'"
        for label, items in named_lists.items()
        for name in _duplicates([item["name"] for item in items])
    ]


def check_query_definitions(tasktable: dict) -> list[str]:
    """Each query declares exactly the {placeholders} used by its CQL2 filter."""
    errors = []
    for query in tasktable.get("queries", []):
        declared = set(query.get("parameters", []))
        used = {m.group(1) for s in _strings(query.get("stac")) if (m := QUERY_PARAMETER_PATTERN.match(s))}
        if unused := declared - used:
            errors.append(f"Query '{query['name']}' declares parameters not used in its filter: {sorted(unused)}")
        if undeclared := used - declared:
            errors.append(f"Query '{query['name']}' uses undeclared parameters in its filter: {sorted(undeclared)}")
    return errors


def check_query_calls(tasktable: dict) -> list[str]:
    """Each query called by an "io" product is defined, and called with exactly its declared parameters."""
    errors = []
    definitions = {query["name"]: set(query.get("parameters", [])) for query in tasktable.get("queries", [])}

    for product_name, call in _query_calls(tasktable):
        prefix = f"Product '{product_name}' calls query '{call['name']}'"
        if call["name"] not in definitions:
            errors.append(f"{prefix}, which is not defined")
            continue
        provided = set(call.get("parameters", {}))
        if missing := definitions[call["name"]] - provided:
            errors.append(f"{prefix} without parameters: {sorted(missing)}")
        if extra := provided - definitions[call["name"]]:
            errors.append(f"{prefix} with unexpected parameters: {sorted(extra)}")
    return errors


def check_external_variables(tasktable: dict) -> list[str]:
    """Each declared external variable is used, and each used external variable is declared."""
    errors = []
    declared = set(tasktable.get("external_variables", []))
    used = {var for s in _strings(tasktable) for var in EXTERNAL_VARIABLE_PATTERN.findall(s)}
    if unused := declared - used:
        errors.append(f"External variables declared but not used: {sorted(unused)}")
    if undeclared := used - declared:
        errors.append(f"External variables used but not declared: {sorted(undeclared)}")
    return errors


def check_unit_products(tasktable: dict) -> list[str]:
    """Each product of a unit is described in the matching "io" section."""
    errors = []
    io_names = {section: {p["name"] for p in tasktable.get("io", {}).get(section, [])} for section in IO_SECTIONS}

    for unit in tasktable.get("units", []):
        for field, section in UNIT_IO_FIELDS.items():
            errors += [
                f"Unit '{unit['name']}' {field} '{product['name']}' is not described in io.{section}"
                for product in unit.get(field, [])
                if product["name"] not in io_names[section]
            ]
    return errors


def _produces(step: dict, unit: dict | None, product: str) -> bool:
    """
    Whether the step produces the given product. In memory, the processor keeps the products under the
    names returned by the processing unit: the "regex" of the unit output if any, else its name.
    """
    unit_outputs = {output["name"]: output for output in (unit or {}).get("output_products", [])}
    patterns = [unit_outputs.get(name, {}).get("regex") or re.escape(name) for name in step.get("output_products", {})]
    return any(re.fullmatch(pattern, product) for pattern in patterns)


def _check_step_origins(pipeline: dict, step: dict, previous_steps: list[dict], units: dict[str, dict]) -> list[str]:
    """Check that the step inputs come from the pipeline input or from a product of a previous step."""
    errors = []
    for product_name, origin in step.get("input_products", {}).items():
        if origin == "pipeline_input":
            continue
        prefix = f"Pipeline '{pipeline['name']}' step {step['step_id']} input '{product_name}' origin '{origin}'"
        if not (match := STEP_ORIGIN_PATTERN.match(origin)):
            errors.append(f"{prefix} is neither 'pipeline_input' nor 'unit[.step_id].product'")
            continue
        origin_steps = [
            s
            for s in previous_steps
            if s["unit_name"] == match["unit"] and match["step_id"] in (None, str(s["step_id"]))
        ]
        if not origin_steps:
            errors.append(f"{prefix} does not match any previous step")
        elif not _produces(origin_steps[0], units.get(match["unit"]), match["product"]):
            errors.append(f"{prefix} is not a product of this step")
    return errors


def check_pipeline_steps(tasktable: dict) -> list[str]:
    """
    Each pipeline step:
    - has a unique step_id in its pipeline and runs a defined unit,
    - gives an origin to all the input and output products of its unit,
    - gives no input product that its unit does not declare,
    - takes its inputs from the pipeline input or from a product of a previous step.
    """
    errors = []
    units = {unit["name"]: unit for unit in tasktable.get("units", [])}

    for pipeline in tasktable.get("pipelines", []):
        errors += [
            f"Pipeline '{pipeline['name']}' has several steps with step_id {step_id}"
            for step_id in _duplicates([step["step_id"] for step in pipeline["steps"]])
        ]

        for index, step in enumerate(pipeline["steps"]):
            prefix = f"Pipeline '{pipeline['name']}' step {step['step_id']} ({step['unit_name']})"
            if not (unit := units.get(step["unit_name"])):
                errors.append(f"{prefix} runs an undefined unit")
                continue

            for field in ("input_products", "output_products"):
                declared = {product["name"] for product in unit.get(field, [])}
                if missing := declared - set(step.get(field, {})):
                    errors.append(f"{prefix} gives no origin to the unit {field}: {sorted(missing)}")

            unit_inputs = {product["name"] for product in unit.get("input_products", [])}
            if extra := set(step.get("input_products", {})) - unit_inputs:
                errors.append(f"{prefix} gives input_products not declared by the unit: {sorted(extra)}")

            errors += _check_step_origins(pipeline, step, pipeline["steps"][:index], units)
    return errors


CHECKS: list[Callable[[dict], list[str]]] = [
    check_unique_names,
    check_query_definitions,
    check_query_calls,
    check_external_variables,
    check_unit_products,
    check_pipeline_steps,
]

TASKTABLES = sorted(CONFIG_DIR.glob("TaskTable_*.json"))


def test_tasktables_found():
    """Make sure that the task tables are found, else the parametrized tests below would silently be skipped."""
    assert TASKTABLES


@pytest.mark.parametrize(
    "path, check",
    [pytest.param(path, check, id=f"{path.stem}-{check.__name__}") for path in TASKTABLES for check in CHECKS],
)
def test_tasktable_semantics(path: Path, check: Callable[[dict], list[str]]):
    """Run a semantic check on a task table: the error list must be empty."""
    tasktable = json.loads(path.read_text(encoding="utf-8"))
    assert check(tasktable) == []
