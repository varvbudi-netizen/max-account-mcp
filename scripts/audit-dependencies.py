"""Проверка закреплённых версий по публичной базе известных уязвимостей OSV."""

import json
from pathlib import Path
import urllib.request


# В запрос попадают только имена общедоступных пакетов и версии, без настроек пользователя.
packages = [line.strip().split("==", 1) for line in Path("requirements.lock").read_text().splitlines()
            if line.strip() and not line.startswith("#")]
request = urllib.request.Request(
    "https://api.osv.dev/v1/querybatch",
    data=json.dumps({"queries": [{"package": {"name": name, "ecosystem": "PyPI"}, "version": version}
                                  for name, version in packages]}).encode("utf-8"),
    headers={"Content-Type": "application/json"}, method="POST",
)
with urllib.request.urlopen(request, timeout=30) as response:
    results = json.load(response)["results"]
findings = [{"package": name, "version": version, "ids": [item["id"] for item in result.get("vulns", [])]}
            for (name, version), result in zip(packages, results) if result.get("vulns")]
print(json.dumps({"checked": len(packages), "findings": findings}, ensure_ascii=False))
raise SystemExit(1 if findings else 0)
