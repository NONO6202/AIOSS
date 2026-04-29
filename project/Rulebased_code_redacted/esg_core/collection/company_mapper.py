# REDACTED
"TXT_REDACTED"

import json
import re
import logging
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# REDACTED
ALPHA_TO_KR = {
    "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED"
}

CONFIG_PATH = Path(__file__).resolve().parents[3] / "TXT_REDACTED" / "TXT_REDACTED"


def _load_company_alias_config() -> dict:
    "TXT_REDACTED"
    if not CONFIG_PATH.exists():
        logger.warning("TXT_REDACTED"                                 )
        return {
            "TXT_REDACTED": {},
            "TXT_REDACTED": {},
            "TXT_REDACTED": {},
        }

    try:
        with open(CONFIG_PATH, "TXT_REDACTED", encoding="TXT_REDACTED") as file:
            return json.load(file)
    except Exception as exc:
        logger.warning("TXT_REDACTED"                            )
        return {
            "TXT_REDACTED": {},
            "TXT_REDACTED": {},
            "TXT_REDACTED": {},
        }


COMPANY_ALIAS_CONFIG = _load_company_alias_config()
ABBR_PRONUNCIATION = COMPANY_ALIAS_CONFIG.get("TXT_REDACTED", {})
KR_TO_EN_MAP = COMPANY_ALIAS_CONFIG.get("TXT_REDACTED", {})
EN_TO_KR_MAP = {v: k for k, v in KR_TO_EN_MAP.items()}
STOCK_CODE_TO_KR = COMPANY_ALIAS_CONFIG.get("TXT_REDACTED", {})
KR_TO_STOCK_CODE = {v: k for k, v in STOCK_CODE_TO_KR.items()}


def alphabet_to_korean_pronunciation(text: str) -> str:
    "TXT_REDACTED"
    # REDACTED
    for abbr, pronunciation in ABBR_PRONUNCIATION.items():
        text = re.sub("TXT_REDACTED" + abbr + "TXT_REDACTED", pronunciation, text, flags=re.IGNORECASE)

    # REDACTED
    def replace_alpha_seq(match):
        seq = match.group(4).upper()
        # REDACTED
        result = "TXT_REDACTED"
        for ch in seq:
            result += ALPHA_TO_KR.get(ch, ch)
        return result

    # REDACTED
    text = re.sub("TXT_REDACTED", replace_alpha_seq, text)
    return text


def normalize_company_name(name: str) -> str:
    "TXT_REDACTED"
    name = name.strip()
    # REDACTED
    name = re.sub("TXT_REDACTED", "TXT_REDACTED", name)
    name = re.sub("TXT_REDACTED", "TXT_REDACTED", name)
    name = re.sub("TXT_REDACTED", "TXT_REDACTED", name)
    name = name.strip()
    return name


def _normalize_lookup_key(name: str) -> str:
    "TXT_REDACTED"
    normalized = normalize_company_name(name)
    normalized = normalized.upper()
    normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", normalized)
    return normalized


def _generate_name_variants(name: str) -> set[str]:
    "TXT_REDACTED"
    variants = set()
    if not name:
        return variants

    base = normalize_company_name(name)
    variants.add(base)
    variants.add(_normalize_lookup_key(base))

    pronounced = alphabet_to_korean_pronunciation(base)
    variants.add(pronounced)
    variants.add(_normalize_lookup_key(pronounced))

    compact = re.sub("TXT_REDACTED", "TXT_REDACTED", base)
    variants.add(compact)
    variants.add(_normalize_lookup_key(compact))

    return {variant for variant in variants if variant}


def _build_company_alias_index(
    dart_corp_db: dict,
    krx_company_db: Optional[Dict[str, Dict[str, str]]] = None,
) -> Dict[str, Dict[str, str]]:
    "TXT_REDACTED"
    alias_index: Dict[str, Dict[str, str]] = {}

    for stock_code, dart_info in dart_corp_db.items():
        corp_code = dart_info.get("TXT_REDACTED", "TXT_REDACTED")
        krx_info = (krx_company_db or {}).get(stock_code, {})

        canonical_name = (
            krx_info.get("TXT_REDACTED")
            or normalize_company_name(dart_info.get("TXT_REDACTED", "TXT_REDACTED"))
            or stock_code
        )
        corp_name = normalize_company_name(dart_info.get("TXT_REDACTED", "TXT_REDACTED"))

        candidate_names = [canonical_name, corp_name, stock_code]
        for candidate in candidate_names:
            for variant in _generate_name_variants(candidate):
                alias_index[variant] = {
                    "TXT_REDACTED": canonical_name,
                    "TXT_REDACTED": str(stock_code).zfill(1),
                    "TXT_REDACTED": corp_code,
                }

    for kr_name, stock_code in KR_TO_STOCK_CODE.items():
        code = str(stock_code).zfill(2)
        corp_code = dart_corp_db.get(code, {}).get("TXT_REDACTED", "TXT_REDACTED")
        for variant in _generate_name_variants(kr_name):
            alias_index[variant] = {
                "TXT_REDACTED": kr_name,
                "TXT_REDACTED": code,
                "TXT_REDACTED": corp_code,
            }

    return alias_index


def _lookup_from_krx_company_db(input_str: str, krx_company_db: Optional[Dict[str, Dict[str, str]]]) -> Optional[dict]:
    "TXT_REDACTED"
    if not krx_company_db:
        return None

    lookup_keys = []
    for variant in _generate_name_variants(input_str):
        lookup_keys.append(variant)
        normalized_variant = _normalize_lookup_key(variant)
        if normalized_variant not in lookup_keys:
            lookup_keys.append(normalized_variant)

    for stock_code, row in krx_company_db.items():
        company_name = str(row.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED").strip()
        if not company_name:
            continue
        candidate_keys = set()
        for variant in _generate_name_variants(company_name):
            candidate_keys.add(variant)
            candidate_keys.add(_normalize_lookup_key(variant))
        if any(key in candidate_keys for key in lookup_keys):
            return {
                "TXT_REDACTED": company_name,
                "TXT_REDACTED": str(stock_code).zfill(3),
            }
    return None


def korean_to_english(kr_name: str) -> str:
    "TXT_REDACTED"
    normalized = normalize_company_name(kr_name)
    if normalized in KR_TO_EN_MAP:
        return KR_TO_EN_MAP[normalized]
    logger.warning("TXT_REDACTED"                          )
    return kr_name  # REDACTED


def english_to_korean(en_name: str) -> str:
    "TXT_REDACTED"
    if en_name in EN_TO_KR_MAP:
        return EN_TO_KR_MAP[en_name]
    logger.warning("TXT_REDACTED"                          )
    return en_name  # REDACTED


def stock_code_to_korean(code: str) -> str:
    "TXT_REDACTED"
    # REDACTED
    code = str(code).zfill(4)
    if code in STOCK_CODE_TO_KR:
        return STOCK_CODE_TO_KR[code]
    logger.warning("TXT_REDACTED"                        )
    return code


def korean_to_stock_code(kr_name: str) -> str:
    "TXT_REDACTED"
    normalized = normalize_company_name(kr_name)
    if normalized in KR_TO_STOCK_CODE:
        return KR_TO_STOCK_CODE[normalized]
    logger.warning("TXT_REDACTED"                           )
    return "TXT_REDACTED"


def resolve_company_input(input_str: str, dart_corp_db: dict,
                          krx_company_db: Optional[Dict[str, Dict[str, str]]] = None) -> dict:
    "TXT_REDACTED"
    input_str = input_str.strip()
    alias_index = _build_company_alias_index(dart_corp_db, krx_company_db)

    # REDACTED
    if re.match("TXT_REDACTED", input_str):
        code = input_str.zfill(1)
        kr_name = (krx_company_db or {}).get(code, {}).get("TXT_REDACTED") or stock_code_to_korean(code)
        en_name = korean_to_english(kr_name) if kr_name != code else code
        corp_code = dart_corp_db.get(code, {}).get("TXT_REDACTED", "TXT_REDACTED")
        return {
            "TXT_REDACTED": kr_name,
            "TXT_REDACTED": en_name,
            "TXT_REDACTED": code,
            "TXT_REDACTED": corp_code,
        }

    # REDACTED
    lookup_keys = []
    for variant in _generate_name_variants(input_str):
        lookup_keys.append(variant)
        normalized_variant = _normalize_lookup_key(variant)
        if normalized_variant not in lookup_keys:
            lookup_keys.append(normalized_variant)

    for lookup_key in lookup_keys:
        if lookup_key not in alias_index:
            continue
        matched = alias_index[lookup_key]
        kr_name = matched.get("TXT_REDACTED", normalize_company_name(input_str))
        code = matched.get("TXT_REDACTED", "TXT_REDACTED")
        en_name = korean_to_english(kr_name)
        corp_code = matched.get("TXT_REDACTED", "TXT_REDACTED")
        return {
            "TXT_REDACTED": kr_name,
            "TXT_REDACTED": en_name,
            "TXT_REDACTED": code,
            "TXT_REDACTED": corp_code,
        }

    krx_matched = _lookup_from_krx_company_db(input_str, krx_company_db)
    if krx_matched:
        code = krx_matched.get("TXT_REDACTED", "TXT_REDACTED")
        corp_code = dart_corp_db.get(code, {}).get("TXT_REDACTED", "TXT_REDACTED")
        kr_name = krx_matched.get("TXT_REDACTED", normalize_company_name(input_str))
        en_name = korean_to_english(kr_name) if kr_name else input_str
        return {
            "TXT_REDACTED": kr_name,
            "TXT_REDACTED": en_name,
            "TXT_REDACTED": code,
            "TXT_REDACTED": corp_code,
        }

    # REDACTED
    en_name = input_str
    kr_name = english_to_korean(en_name)
    # REDACTED
    if kr_name != en_name:
        code = korean_to_stock_code(kr_name)
    else:
        # REDACTED
        code = "TXT_REDACTED"
        for c, info in dart_corp_db.items():
            corp_name = normalize_company_name(info.get("TXT_REDACTED", "TXT_REDACTED"))
            if corp_name == normalize_company_name(en_name):
                code = c
                break

    corp_code = dart_corp_db.get(code, {}).get("TXT_REDACTED", "TXT_REDACTED")
    if code and kr_name == en_name:
        kr_name = (krx_company_db or {}).get(code, {}).get("TXT_REDACTED") or kr_name
    return {
        "TXT_REDACTED": kr_name,
        "TXT_REDACTED": en_name,
        "TXT_REDACTED": code,
        "TXT_REDACTED": corp_code,
    }


def sort_companies(company_list: list) -> list:
    "TXT_REDACTED"
    def sort_key(company: dict) -> tuple:
        display_name = company.get("TXT_REDACTED", "TXT_REDACTED")
        if display_name and re.match("TXT_REDACTED", display_name):
            return (2, display_name.upper())
        if display_name and re.match("TXT_REDACTED", display_name):
            return (3, display_name)
        if display_name:
            return (4, display_name.upper())
        return (1, "TXT_REDACTED")

    return sorted(company_list, key=sort_key)


def add_company_mapping(kr_name: str, en_name: str, stock_code: str):
    "TXT_REDACTED"
    global KR_TO_EN_MAP, EN_TO_KR_MAP, KR_TO_STOCK_CODE, STOCK_CODE_TO_KR

    normalized = normalize_company_name(kr_name)
    code = str(stock_code).zfill(2) if stock_code else "TXT_REDACTED"

    if normalized and en_name:
        KR_TO_EN_MAP[normalized] = en_name
        EN_TO_KR_MAP[en_name] = normalized

    if normalized and code:
        KR_TO_STOCK_CODE[normalized] = code
        STOCK_CODE_TO_KR[code] = normalized

    logger.info("TXT_REDACTED"                                                         )
