# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def _normalize_contract_text(text: Any) -> str:
    return re.sub("TXT_REDACTED", "TXT_REDACTED", str(text or "TXT_REDACTED")).replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")


def _is_blank(value: Any) -> bool:
    return value in (None, "TXT_REDACTED", [], {})


@dataclass(frozen=True)
class FieldContract:
    field_id: str
    label: str
    aliases: List[str]
    output_label: str
    value_type: str

    def all_aliases(self) -> List[str]:
        values = [self.field_id, self.label, self.output_label, *self.aliases]
        unique: List[str] = []
        seen = set()
        for value in values:
            text = str(value or "TXT_REDACTED").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            unique.append(text)
        return unique


class FieldContractRegistry:
    def __init__(self, payload: Dict[str, Any]):
        self.payload = payload
        self.version = str(payload.get("TXT_REDACTED") or "TXT_REDACTED")
        self.contracts: Dict[str, FieldContract] = {}
        self._alias_to_id: Dict[str, str] = {}

        for item in payload.get("TXT_REDACTED", []) or []:
            contract = FieldContract(
                field_id=str(item.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
                label=str(item.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
                aliases=[str(alias) for alias in (item.get("TXT_REDACTED") or []) if str(alias).strip()],
                output_label=str(item.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
                value_type=str(item.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
            )
            if not contract.field_id:
                continue
            self.contracts[contract.field_id] = contract
            for alias in contract.all_aliases():
                normalized = _normalize_contract_text(alias)
                if normalized:
                    self._alias_to_id.setdefault(normalized, contract.field_id)

    @classmethod
    def from_file(cls, path: str) -> "TXT_REDACTED":
        return cls(json.loads(Path(path).read_text(encoding="TXT_REDACTED")))

    def contract_for(self, name: Any) -> Optional[FieldContract]:
        field_id = self.canonical_field_id(name)
        return self.contracts.get(field_id)

    def canonical_field_id(self, name: Any) -> str:
        text = str(name or "TXT_REDACTED").strip()
        if not text:
            return "TXT_REDACTED"
        normalized = _normalize_contract_text(text)
        return self._alias_to_id.get(normalized, text)

    def label_for(self, name: Any) -> str:
        contract = self.contract_for(name)
        if contract:
            return contract.label or str(name or "TXT_REDACTED")
        return str(name or "TXT_REDACTED")

    def output_label_for(self, header: Any) -> str:
        contract = self.contract_for(header)
        if contract and contract.output_label:
            return contract.output_label
        return str(header or "TXT_REDACTED")

    def aliases_for(self, name: Any) -> List[str]:
        contract = self.contract_for(name)
        if not contract:
            text = str(name or "TXT_REDACTED").strip()
            return [text] if text else []
        return contract.all_aliases()

    def value_type_for(self, name: Any) -> str:
        contract = self.contract_for(name)
        return contract.value_type if contract else "TXT_REDACTED"

    def get_value(self, row: Dict[str, Any], *names: Any, default: Any = "TXT_REDACTED") -> Any:
        for name in names:
            contract = self.contract_for(name)
            if contract:
                for alias in contract.all_aliases():
                    if alias in row and not _is_blank(row.get(alias)):
                        return row.get(alias)
            text = str(name or "TXT_REDACTED").strip()
            if text in row and not _is_blank(row.get(text)):
                return row.get(text)
        return default

    def normalize_mapping_keys(self, mapping: Dict[str, Any]) -> Dict[str, Any]:
        normalized: Dict[str, Any] = {}
        for key, value in (mapping or {}).items():
            canonical = self.canonical_field_id(key)
            if canonical not in normalized or _is_blank(normalized.get(canonical)):
                normalized[canonical] = value
        return normalized

    def normalize_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        normalized: Dict[str, Any] = {}
        for key, value in (row or {}).items():
            key_text = str(key or "TXT_REDACTED")
            if key_text == "TXT_REDACTED":
                normalized[key_text] = self.normalize_mapping_keys(value if isinstance(value, dict) else {})
                continue
            if key_text == "TXT_REDACTED":
                normalized[key_text] = self.normalize_mapping_keys(value if isinstance(value, dict) else {})
                continue
            if key_text.startswith("TXT_REDACTED"):
                normalized[key_text] = value
                continue
            canonical = self.canonical_field_id(key_text)
            if canonical not in normalized or _is_blank(normalized.get(canonical)):
                normalized[canonical] = value
        return normalized


DEFAULT_FIELD_CONTRACTS_PATH = Path(__file__).resolve().parent / "TXT_REDACTED" / "TXT_REDACTED"
FIELD_CONTRACTS = FieldContractRegistry.from_file(str(DEFAULT_FIELD_CONTRACTS_PATH))
