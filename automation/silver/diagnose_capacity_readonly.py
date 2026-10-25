"""Read-only Silver VM eligibility report. No Terraform, cluster or Job actions."""
import argparse
import json
import subprocess
import sys
from decimal import Decimal, InvalidOperation

ALTERNATES = ("Standard_D4ads_v5", "Standard_D4ds_v4")


def cli(*args):
    result = subprocess.run(args, capture_output=True, text=True, encoding="utf-8", errors="strict", timeout=120)
    if result.returncode:
        raise RuntimeError("READ_ONLY_QUERY_FAILED: " + args[0] + " " + args[1])
    return json.loads(result.stdout)


def number(value):
    try:
        n = Decimal(str(value))
        return n if n.is_finite() else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def quota_summary(usages, family):
    lookup = {}
    for entry in usages:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if isinstance(name, dict) and isinstance(name.get("value"), str):
            lookup[name["value"].lower()] = entry
    def remaining(key):
        item = lookup.get(key.lower())
        if item is None:
            return None
        limit, used = number(item.get("limit")), number(item.get("currentValue"))
        return limit - used if limit is not None and used is not None else None
    return remaining("cores"), remaining(family) if isinstance(family, str) else None


def evaluate(sku, listed, usages):
    if sku is None:
        return {"workspace_listed": listed, "azure_listed": False, "eligible": False, "reasons": ["NOT_IN_AZURE_SKU_CATALOG"]}
    caps = {x.get("name"): x.get("value") for x in (sku.get("capabilities") or []) if isinstance(x, dict)}
    restrictions = sku.get("restrictions") or []
    regional, family = quota_summary(usages, sku.get("family"))
    checks = {
        "workspace_listed": listed,
        "azure_listed": True,
        "eastus_listed": "eastus" in (sku.get("locations") or []),
        "virtual_machine": sku.get("resourceType") == "virtualMachines",
        "four_vcpu": number(caps.get("vCPUs")) == 4,
        "sixteen_gib": number(caps.get("MemoryGB")) == 16,
        "no_location_restriction": not any(isinstance(r, dict) and r.get("type") == "Location" for r in restrictions),
        "regional_quota_at_least_4": regional is not None and regional >= 4,
        "family_quota_at_least_4": family is not None and family >= 4,
    }
    return {
        **checks,
        "regional_quota_remaining": str(regional) if regional is not None else "UNKNOWN",
        "family_quota_remaining": str(family) if family is not None else "UNKNOWN",
        "family": sku.get("family"),
        "eligible": all(checks.values()),
        "reasons": [name for name, ok in checks.items() if not ok],
    }


def main():
    parser = argparse.ArgumentParser(description="Read-only Azure/Databricks Silver capacity evidence")
    parser.add_argument("--profile", default="admm-workspace")
    args = parser.parse_args()
    subscription = cli("az", "account", "show", "--output", "json")["id"]
    skus = cli("az", "vm", "list-skus", "--location", "eastus", "--resource-type", "virtualMachines",
               "--subscription", subscription, "--all", "--output", "json")
    usages = cli("az", "vm", "list-usage", "--location", "eastus",
                 "--subscription", subscription, "--output", "json")
    nodes = cli("databricks", "api", "get", "/api/2.0/clusters/list-node-types",
                "--profile", args.profile)
    listed = {n.get("node_type_id") for n in nodes.get("node_types", []) if isinstance(n, dict)}
    catalog = {s.get("name"): s for s in skus if isinstance(s, dict)}
    print(json.dumps({
        "status": "READ_ONLY_CAPACITY_DIAGNOSTIC",
        "region": "eastus",
        "note": "Catalog and quota snapshots do not guarantee physical VM stock. No Job was run.",
        "alternates": {name: evaluate(catalog.get(name), name in listed, usages) for name in ALTERNATES},
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, KeyError, ValueError, TypeError, subprocess.TimeoutExpired, UnicodeError, OSError) as exc:
        print(json.dumps({"status": "BLOCKED", "error_type": type(exc).__name__,
                          "code": "READ_ONLY_CAPACITY_DIAGNOSTIC_FAILED"}), file=sys.stderr)
        sys.exit(1)
