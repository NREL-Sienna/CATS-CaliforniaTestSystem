"""Validate the psy6 OpenAPI export of CATS against the power_openapi_models pydantic models.

power_openapi_models exposes no top-level SystemDocument loader, so the type registry is
built by reflection over the four domain modules, as in that package's own
scripts/check_json_compat.py. That script is the fuller gate (it also round-trips each
payload to catch field drift); this one adds the per-type OK/FAIL breakdown.
"""

import argparse
import json
import sys
from pathlib import Path

from pydantic import BaseModel

from power_openapi_models.core import models as core_models
from power_openapi_models.dynamics import models as dynamics_models
from power_openapi_models.investments import models as investments_models
from power_openapi_models.operations import models as operations_models

MODULES = (core_models, operations_models, dynamics_models, investments_models)


def build_type_registry() -> dict[str, type[BaseModel]]:
    """Map component type name -> pydantic model class, scanning all four modules."""
    registry: dict[str, type[BaseModel]] = {}
    for module in MODULES:
        for name, obj in vars(module).items():
            if isinstance(obj, type) and issubclass(obj, BaseModel) and obj is not BaseModel:
                registry[name] = obj
    return registry


def validate_system(system_json: Path) -> int:
    """Validate every component in `system_json` against its matching pydantic model.

    Returns the number of validation failures (0 = clean).
    """
    doc = json.loads(system_json.read_bytes())
    registry = build_type_registry()
    components = doc.get("components", {})

    failures = []
    counts = {}
    for type_name, rows in components.items():
        model = registry.get(type_name)
        if model is None:
            failures.append((type_name, None, f"no pydantic model registered for type {type_name!r}"))
            # Count the rows so an unmodelled type shows up in the headline total.
            counts[type_name] = (0, len(rows))
            continue
        ok = 0
        for row in rows:
            try:
                model.model_validate(row)
                ok += 1
            except Exception as e:  # noqa: BLE001 - collect every failure, don't stop at the first
                failures.append((type_name, row.get("id"), str(e)))
        counts[type_name] = (ok, len(rows))

    total_ok = sum(ok for ok, _ in counts.values())
    total = sum(n for _, n in counts.values())
    print(f"base_power={doc.get('base_power')}  unit_system={doc.get('unit_system')}")
    print(f"{total_ok}/{total} components validated across {len(counts)} types:")
    for type_name in sorted(counts):
        ok, n = counts[type_name]
        marker = "OK" if ok == n else "FAIL"
        print(f"  {marker:4s} {type_name:24s} {ok}/{n}")

    if failures:
        print(f"\n{len(failures)} validation failure(s):")
        for type_name, comp_id, message in failures[:20]:
            print(f"  {type_name} id={comp_id}: {message}")
        if len(failures) > 20:
            print(f"  ... and {len(failures) - 20} more")

    return len(failures)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "system_json",
        nargs="?",
        default=Path(__file__).resolve().parent.parent / "CATS_openapi" / "system.json",
        type=Path,
        help="Path to the exported OpenAPI system.json (default: ../CATS_openapi/system.json)",
    )
    args = parser.parse_args()

    if not args.system_json.is_file():
        sys.exit(f"error: {args.system_json} does not exist")

    n_failures = validate_system(args.system_json)
    sys.exit(1 if n_failures else 0)


if __name__ == "__main__":
    main()
