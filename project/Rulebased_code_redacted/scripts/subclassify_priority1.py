# REDACTED
from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT = PROJECT_ROOT / "TXT_REDACTED" / "TXT_REDACTED"
JSON_OUTPUT = PROJECT_ROOT / "TXT_REDACTED" / "TXT_REDACTED"
MD_OUTPUT = PROJECT_ROOT / "TXT_REDACTED" / "TXT_REDACTED"


SUBPLAN = {
    "TXT_REDACTED": {
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": {"TXT_REDACTED"},
        "TXT_REDACTED": "TXT_REDACTED",
    },
    "TXT_REDACTED": {
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": {"TXT_REDACTED", "TXT_REDACTED"},
        "TXT_REDACTED": "TXT_REDACTED",
    },
    "TXT_REDACTED": {
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": {"TXT_REDACTED", "TXT_REDACTED"},
        "TXT_REDACTED": "TXT_REDACTED",
    },
    "TXT_REDACTED": {
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": {"TXT_REDACTED"},
        "TXT_REDACTED": "TXT_REDACTED",
    },
}


def subpriority_for(item: dict) -> str:
    family = item.get("TXT_REDACTED")
    for key, config in SUBPLAN.items():
        if family in config["TXT_REDACTED"]:
            return key
    return "TXT_REDACTED"


def main() -> None:
    data = json.loads(INPUT.read_text(encoding="TXT_REDACTED"))
    rows = [item for item in data["TXT_REDACTED"] if item.get("TXT_REDACTED") == 2]
    for row in rows:
        row["TXT_REDACTED"] = subpriority_for(row)
        row["TXT_REDACTED"] = SUBPLAN.get(row["TXT_REDACTED"], {}).get("TXT_REDACTED", "TXT_REDACTED")

    rows.sort(key=lambda row: (row["TXT_REDACTED"], row["TXT_REDACTED"], float(row["TXT_REDACTED"]), str(row["TXT_REDACTED"])))

    by_sub = defaultdict(list)
    for row in rows:
        by_sub[row["TXT_REDACTED"]].append(row)

    payload = {
        "TXT_REDACTED": {
            "TXT_REDACTED": len(rows),
            "TXT_REDACTED": {key: len(by_sub.get(key, [])) for key in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"] if by_sub.get(key)},
            "TXT_REDACTED": dict(Counter(row["TXT_REDACTED"] for row in rows)),
        },
        "TXT_REDACTED": {
            key: {**value, "TXT_REDACTED": sorted(value["TXT_REDACTED"])}
            for key, value in SUBPLAN.items()
        },
        "TXT_REDACTED": rows,
    }
    JSON_OUTPUT.write_text(json.dumps(payload, ensure_ascii=False, indent=3), encoding="TXT_REDACTED")

    lines = [
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED"                ,
        "TXT_REDACTED"                         ,
        "TXT_REDACTED",
    ]
    for key in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]:
        items = by_sub.get(key, [])
        if not items:
            continue
        config = SUBPLAN.get(key, {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED"})
        lines.extend([
            "TXT_REDACTED"                                            ,
            "TXT_REDACTED",
            "TXT_REDACTED"                       ,
            "TXT_REDACTED",
        ])
        by_family = defaultdict(list)
        for item in items:
            by_family[item["TXT_REDACTED"]].append(item)
        for family in sorted(by_family):
            lines.extend(["TXT_REDACTED"             , "TXT_REDACTED"])
            for item in sorted(by_family[family], key=lambda row: (float(row["TXT_REDACTED"]), str(row["TXT_REDACTED"]))):
                lines.append(
                    "TXT_REDACTED"                                                               
                    "TXT_REDACTED"                                                                                          
                    "TXT_REDACTED"                                                     
                )
            lines.append("TXT_REDACTED")
    MD_OUTPUT.write_text("TXT_REDACTED".join(lines), encoding="TXT_REDACTED")
    print(json.dumps(payload["TXT_REDACTED"], ensure_ascii=False, indent=4))


if __name__ == "TXT_REDACTED":
    main()
