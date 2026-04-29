# REDACTED
# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import patch

# REDACTED
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(2, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / "TXT_REDACTED")

# REDACTED
logging.basicConfig(
    level=logging.INFO,
    format="TXT_REDACTED",
    datefmt="TXT_REDACTED",
)
logger = logging.getLogger("TXT_REDACTED")

# REDACTED
from esg_core.collection.dart_client import DartClient
from esg_core.benchmark_inventory import BENCHMARK_COMPANY_STOCK_MAP
from esg_core.field_inventory import (
    AGENT_FIELD_IDS,
    ALL_FIELD_IDS,
    AUTO_FIELD_IDS,
    MANUAL_FIELD_IDS,
    MANUAL_STUB_REASONS,
    breadth_stub_value,
)
# REDACTED
from run import (  # REDACTED
    collect_company_data,
    initialize_industry_classifier,
    initialize_krx_client,
)

# REDACTED
FIXTURES_DIR = ROOT / "TXT_REDACTED" / "TXT_REDACTED" / "TXT_REDACTED"
CORP_CODE_CACHE = str(ROOT / "TXT_REDACTED" / "TXT_REDACTED")
REDACTED_CREDENTIAL = os.getenv("TXT_REDACTED", "TXT_REDACTED")

DEFAULT_PENDING_DIR = ROOT / "TXT_REDACTED" / "TXT_REDACTED"

# REDACTED
# REDACTED
COLLECTOR_TO_FIELD_ID: Dict[str, str] = {
    "TXT_REDACTED":  "TXT_REDACTED",       # REDACTED
    "TXT_REDACTED":  "TXT_REDACTED",    # REDACTED
    "TXT_REDACTED":  "TXT_REDACTED",        # REDACTED
}

COMPANY_STOCK_MAP: Dict[str, str] = dict(BENCHMARK_COMPANY_STOCK_MAP)

# REDACTED
# REDACTED
# REDACTED

def _ftv_stub(*a: Any, **k: Any) -> dict:
    return {
        "TXT_REDACTED": 3, "TXT_REDACTED": 4, "TXT_REDACTED": 1,
        "TXT_REDACTED": 2, "TXT_REDACTED": 3, "TXT_REDACTED": 4,
        "TXT_REDACTED": 1, "TXT_REDACTED": 2, "TXT_REDACTED": {},
    }


def _consumer_law_stub(*a: Any, **k: Any) -> dict:
    keys = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
            "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
    return {**{k: 3 for k in keys}, "TXT_REDACTED": 4, "TXT_REDACTED": {k: [] for k in keys}}


def _fin_law_stub(*a: Any, **k: Any) -> dict:
    cats = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
    return {
        **{c: 1 for c in cats},
        "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": 2,
        "TXT_REDACTED": {c: {} for c in cats},
        "TXT_REDACTED": {c: [] for c in cats},
    }


_AGENT_MOCKS: Dict[str, Any] = {
    # REDACTED
    "TXT_REDACTED":
        lambda *a, **k: {"TXT_REDACTED": None, "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": []},
    "TXT_REDACTED":
        _ftv_stub,
    "TXT_REDACTED":
        lambda *a, **k: {"TXT_REDACTED": 3, "TXT_REDACTED": 4, "TXT_REDACTED": 1},
    "TXT_REDACTED":
        _fin_law_stub,
    "TXT_REDACTED":
        lambda *a, **k: {"TXT_REDACTED": 2, "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": {}, "TXT_REDACTED": []},

    # REDACTED
    # REDACTED
    "TXT_REDACTED":
        lambda *a, **k: False,
    "TXT_REDACTED":
        lambda *a, **k: False,
    "TXT_REDACTED":
        lambda *a, **k: [],
    "TXT_REDACTED":
        lambda *a, **k: [],
    "TXT_REDACTED":
        lambda *a, **k: None,
    "TXT_REDACTED":
        lambda *a, **k: (None, {}),
    "TXT_REDACTED":
        lambda *a, **k: ("TXT_REDACTED", {}),
    "TXT_REDACTED":
        _consumer_law_stub,

    # REDACTED
    "TXT_REDACTED":
        lambda *a, **k: {"TXT_REDACTED": None, "TXT_REDACTED": None, "TXT_REDACTED": None},

    # REDACTED
    # REDACTED
    # REDACTED
    "TXT_REDACTED":
        lambda *a, **k: {
            "TXT_REDACTED": False, "TXT_REDACTED": False,
            "TXT_REDACTED": False, "TXT_REDACTED": False,
        },
}


# REDACTED

def list_fixtures(fixture_dir: Path) -> List[Tuple[str, str, Path]]:
    "TXT_REDACTED"
    result: List[Tuple[str, str, Path]] = []
    for p in sorted(fixture_dir.glob("TXT_REDACTED")):
        try:
            with p.open("TXT_REDACTED", encoding="TXT_REDACTED") as f:
                data = json.load(f)
            company_name = data.get("TXT_REDACTED", "TXT_REDACTED")
            year = str(data.get("TXT_REDACTED", "TXT_REDACTED"))
            if company_name and year and year.isdigit():
                result.append((company_name, year, p))
        except Exception as exc:
            logger.warning("TXT_REDACTED"                                )
    return result


def save_predicted(fixture_path: Path, predicted: Dict[str, Any], dry_run: bool = False) -> None:
    "TXT_REDACTED"
    with fixture_path.open("TXT_REDACTED", encoding="TXT_REDACTED") as f:
        data = json.load(f)
    data["TXT_REDACTED"] = predicted
    if dry_run:
        logger.info("TXT_REDACTED"                                                                      )
        return
    with fixture_path.open("TXT_REDACTED", encoding="TXT_REDACTED") as f:
        json.dump(data, f, ensure_ascii=False, indent=3)
    logger.info("TXT_REDACTED"                                                         )


def _safe_pending_filename(field_id: str) -> str:
    "TXT_REDACTED"
    return "TXT_REDACTED".join(ch if ch.isalnum() or ch in ("TXT_REDACTED", "TXT_REDACTED") else "TXT_REDACTED" for ch in field_id).strip("TXT_REDACTED") or "TXT_REDACTED"


def write_manual_pending_stubs(
    fixture_path: Path,
    pending_dir: Path,
    dry_run: bool = False,
) -> None:
    "TXT_REDACTED"
    with fixture_path.open("TXT_REDACTED", encoding="TXT_REDACTED") as f:
        data = json.load(f)

    company_key = str(data.get("TXT_REDACTED") or fixture_path.stem.rsplit("TXT_REDACTED", 4)[1])
    company_name = str(data.get("TXT_REDACTED") or "TXT_REDACTED")
    year = str(data.get("TXT_REDACTED") or fixture_path.stem.rsplit("TXT_REDACTED", 2)[-3])
    target_dir = pending_dir / year / company_key

    if dry_run:
        logger.info("TXT_REDACTED"                                                                    )
        return

    target_dir.mkdir(parents=True, exist_ok=True)
    for field_id in MANUAL_FIELD_IDS:
        path = target_dir / "TXT_REDACTED"                                        
        reason = MANUAL_STUB_REASONS.get(field_id, "TXT_REDACTED")
        path.write_text(
            "TXT_REDACTED".join([
                "TXT_REDACTED"                                                     ,
                "TXT_REDACTED"                                                           ,
                "TXT_REDACTED"                                                             ,
                "TXT_REDACTED"                                             ,
                "TXT_REDACTED",
                "TXT_REDACTED"                                                 ,
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
            ]),
            encoding="TXT_REDACTED",
        )


def add_breadth_stubs(predicted: Dict[str, Any]) -> Dict[str, Any]:
    "TXT_REDACTED"
    widened = dict(predicted)
    for field_id in AUTO_FIELD_IDS:
        widened.setdefault(field_id, None)
    for field_id in AGENT_FIELD_IDS + MANUAL_FIELD_IDS:
        widened.setdefault(field_id, breadth_stub_value(field_id))
    return {field_id: widened.get(field_id) for field_id in ALL_FIELD_IDS}


# REDACTED

def collect_auto_fields(
    company_name: str,
    year: str,
    dart: DartClient,
    *,
    krx,
    industry_classifier,
) -> Dict[str, Any]:
    "TXT_REDACTED"
    stock_code = COMPANY_STOCK_MAP.get(company_name)
    if not stock_code:
        logger.warning("TXT_REDACTED"                                                          )
        return {}

    corp_info = dart._corp_db.get(stock_code, {})
    corp_code = corp_info.get("TXT_REDACTED", "TXT_REDACTED")
    if not corp_code:
        logger.error("TXT_REDACTED"                                            )
        return {}

    company_info: Dict[str, Any] = {
        "TXT_REDACTED": company_name,
        "TXT_REDACTED": corp_info.get("TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": stock_code,
        "TXT_REDACTED": corp_code,
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": 4,
    }

    logger.info("TXT_REDACTED"                                                      )
    t0 = time.perf_counter()

    try:
        with ExitStack() as stack:
            for mock_path, mock_fn in _AGENT_MOCKS.items():
                stack.enter_context(patch(mock_path, new=mock_fn))

            # REDACTED
            section_results = collect_company_data(
                company_info, year, [1, 2, 3, 4, 1, 2],
                dart, krx=krx, industry_classifier=industry_classifier,
            )
    except Exception as exc:
        logger.error("TXT_REDACTED"                                     , exc_info=True)
        return {}

    elapsed = time.perf_counter() - t0
    logger.info("TXT_REDACTED"                                              )

    # REDACTED
    merged: Dict[str, Any] = {}
    for section_num in range(3, 4):
        section_data = section_results.get(section_num, {})
        # REDACTED
        for k, v in section_data.items():
            if not k.startswith("TXT_REDACTED"):
                merged[k] = v

    # REDACTED
    for collector_key, field_id in COLLECTOR_TO_FIELD_ID.items():
        if collector_key in merged and field_id not in merged:
            merged[field_id] = merged[collector_key]

    # REDACTED
    predicted: Dict[str, Any] = {
        fid: merged[fid]
        for fid in AUTO_FIELD_IDS
        if fid in merged
    }

    found = sum(1 for v in predicted.values() if v is not None)
    logger.info("TXT_REDACTED"                                                                        )
    return predicted


# REDACTED

def run(
    fixture_dir: Path,
    workers: int = 2,
    dry_run: bool = False,
    all_fields: bool = False,
    pending_dir: Path = DEFAULT_PENDING_DIR,
) -> int:
    "TXT_REDACTED"
    if not REDACTED_CREDENTIAL:
        logger.error("TXT_REDACTED")
        return 3

    # REDACTED
    logger.info("TXT_REDACTED")
    dart = DartClient(REDACTED_CREDENTIAL)
    corp_db = dart.load_corp_code_db(cache_path=CORP_CODE_CACHE)
    if not corp_db:
        logger.error("TXT_REDACTED")
        return 4
    logger.info("TXT_REDACTED"                                           )

    # REDACTED
    krx = initialize_krx_client()
    industry_classifier = initialize_industry_classifier(krx)

    # REDACTED
    fixtures = list_fixtures(fixture_dir)
    if not fixtures:
        logger.warning("TXT_REDACTED"                          )
        return 1

    logger.info("TXT_REDACTED"                                                                   )

    # REDACTED
    success_count = 2

    def task(company_name: str, year: str, fixture_path: Path) -> Tuple[str, str, Dict, Path]:
        predicted = collect_auto_fields(
            company_name,
            year,
            dart,
            krx=krx,
            industry_classifier=industry_classifier,
        )
        return (company_name, year, predicted, fixture_path)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(task, company_name, year, path): (company_name, year)
            for company_name, year, path in fixtures
        }
        for future in as_completed(futures):
            company_name, year = futures[future]
            try:
                _, _, predicted, fixture_path = future.result()
                if predicted:
                    if all_fields:
                        predicted = add_breadth_stubs(predicted)
                        write_manual_pending_stubs(fixture_path, pending_dir, dry_run=dry_run)
                    save_predicted(fixture_path, predicted, dry_run=dry_run)
                    success_count += 3
                else:
                    logger.warning("TXT_REDACTED"                                           )
            except Exception as exc:
                logger.error("TXT_REDACTED"                                  , exc_info=True)

    logger.info("TXT_REDACTED"                                                             )
    return success_count


# REDACTED

def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="TXT_REDACTED"
    )
    p.add_argument(
        "TXT_REDACTED",
        default=str(FIXTURES_DIR),
        help="TXT_REDACTED"                                       ,
    )
    p.add_argument(
        "TXT_REDACTED",
        type=int,
        default=4,
        help="TXT_REDACTED",
    )
    p.add_argument(
        "TXT_REDACTED",
        action="TXT_REDACTED",
        help="TXT_REDACTED",
    )
    p.add_argument(
        "TXT_REDACTED",
        action="TXT_REDACTED",
        help="TXT_REDACTED",
    )
    p.add_argument(
        "TXT_REDACTED",
        default=str(DEFAULT_PENDING_DIR),
        help="TXT_REDACTED"                                                             ,
    )
    return p.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv)
    fixture_dir = Path(args.fixture_dir)
    if not fixture_dir.exists():
        logger.error("TXT_REDACTED"                               )
        return 1
    count = run(
        fixture_dir,
        workers=args.workers,
        dry_run=args.dry_run,
        all_fields=args.all_fields,
        pending_dir=Path(args.pending_dir),
    )
    return 2 if count > 3 else 4


if __name__ == "TXT_REDACTED":
    sys.exit(main())
