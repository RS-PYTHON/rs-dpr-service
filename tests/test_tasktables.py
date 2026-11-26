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
from collections import defaultdict
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
STEP_ORIGIN_PATTERN = re.compile(r"(?P<unit>[^.]+)(?:\.(?P<step_id>[1-9][0-9]*))?\.(?P<product>.+)")
STORE_ORIGIN_PREFIX = "pipeline_store."


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
    """Each product is described in "io", except inputs supplied only by other steps."""
    errors = []
    io_names = {section: {p["name"] for p in tasktable.get("io", {}).get(section, [])} for section in IO_SECTIONS}
    input_origins: dict[tuple[str, str], set[str]] = defaultdict(set)
    for pipeline in tasktable.get("pipelines", []):
        for step in pipeline["steps"]:
            for name, origin in step.get("input_products", {}).items():
                input_origins[step["unit_name"], name].add(origin)

    for unit in tasktable.get("units", []):
        for field, section in UNIT_IO_FIELDS.items():
            for product in unit.get(field, []):
                name = product["name"]
                origins = input_origins.get((unit["name"], name))
                if field == "input_products" and origins and "pipeline_input" not in origins:
                    continue
                if name not in io_names[section]:
                    errors.append(f"Unit '{unit['name']}' {field} '{name}' is not described in io.{section}")
    return errors


def _check_step_origins(pipeline: dict, step: dict, previous_steps: list[dict]) -> list[str]:
    """
    Check that inputs come from the pipeline input, a previous unit, or an earlier internal output.
    Lazy product selectors do not have to match a declared output; the unit supplies the names at runtime.
    """
    errors = []
    for product_name, origin in step.get("input_products", {}).items():
        if origin == "pipeline_input":
            continue
        prefix = f"Pipeline '{pipeline['name']}' step {step['step_id']} input '{product_name}' origin '{origin}'"
        if origin.startswith(STORE_ORIGIN_PREFIX):
            store_id, _, input_key = origin.removeprefix(STORE_ORIGIN_PREFIX).partition(".")
            if not store_id or not input_key:
                errors.append(f"{prefix} must use 'pipeline_store.output_id.input_key'")
                continue
            producers = [
                earlier
                for earlier in previous_steps
                if earlier["step_id"] < step["step_id"]
                and earlier.get("output_products", {}).get(store_id) == "pipeline_internal"
            ]
            if len(producers) != 1:
                errors.append(f"{prefix} must reference exactly one earlier internal output")
            continue
        if not (match := STEP_ORIGIN_PATTERN.fullmatch(origin)):
            errors.append(f"{prefix} is not 'pipeline_input', 'unit[.step_id].product' or a store reference")
            continue
        if not any(
            s["unit_name"] == match["unit"] and match["step_id"] in (None, str(s["step_id"])) for s in previous_steps
        ):
            errors.append(f"{prefix} does not match any previous step")
    return errors


def check_pipeline_steps(tasktable: dict) -> list[str]:
    """
    Each pipeline step:
    - has a unique step_id in its pipeline and runs a defined unit,
    - gives an origin to exactly the inputs declared by its unit,
    - takes its inputs from the pipeline input or from a previous step.

    The step selects which outputs to materialize, even when the unit declares
    an output as mandatory. Unmapped outputs are not part of the payload.
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

            unit_inputs = {product["name"] for product in unit.get("input_products", [])}
            step_inputs = set(step.get("input_products", {}))
            if missing := unit_inputs - step_inputs:
                errors.append(f"{prefix} gives no origin to the unit input_products: {sorted(missing)}")
            if extra := step_inputs - unit_inputs:
                errors.append(f"{prefix} gives input_products not declared by the unit: {sorted(extra)}")

            errors += _check_step_origins(pipeline, step, pipeline["steps"][:index])
    return errors


@pytest.mark.parametrize(
    "origin, writer, expected_error",
    [
        ("Producer.1..*", {}, None),
        ("pipeline_store.snapshot.granule", {"snapshot": "pipeline_internal"}, None),
        ("pipeline_store.snapshot.granule", {"snapshot": "pipeline_output"}, "earlier internal output"),
        ("pipeline_store.snapshot", {"snapshot": "pipeline_internal"}, "output_id.input_key"),
        ("Producer.3..*", {}, "does not match any previous step"),
    ],
)
def test_step_input_origins(origin: str, writer: dict[str, str], expected_error: str | None):
    """Accept valid lazy and store references and reject invalid origins."""
    errors = _check_step_origins(
        {"name": "test"},
        {"step_id": 2, "input_products": {"products": origin}},
        [{"unit_name": "Producer", "step_id": 1, "output_products": writer}],
    )
    if expected_error is None:
        assert not errors
    else:
        assert len(errors) == 1 and expected_error in errors[0]


def test_unit_input_used_externally_requires_io_description():
    """Require an IO description when any pipeline uses the input externally."""
    tasktable = {
        "units": [{"name": "Consumer", "input_products": [{"name": "products"}]}],
        "pipelines": [
            {"steps": [{"unit_name": "Consumer", "input_products": {"products": origin}}]}
            for origin in ("Producer.1..*", "pipeline_input")
        ],
    }
    assert check_unit_products(tasktable) == ["Unit 'Consumer' input_products 'products' is not described in io.input"]


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
