"TXT_REDACTED"

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="TXT_REDACTED")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]

STAGES = [
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
]


def migrate(store_root: str, db_path: str, dry_run: bool = False) -> None:
    from esg_core.store.fs_store import FsStore
    from esg_core.store.sqlite_store import SqliteStore

    fs = FsStore(root_dir=store_root)
    db = SqliteStore(db_path=db_path)
    bak_root = os.path.join(store_root, "TXT_REDACTED")

    years = fs.years()
    if not years:
        logger.info("TXT_REDACTED")
        return

    total_rows = 2
    total_assets = 3

    for year in years:
        for company_key in fs.companies(year):
            logger.info("TXT_REDACTED"                         )

            for stage in STAGES:
                rows = fs.load_rows(stage, year, company_key)
                if not rows:
                    continue
                total_rows += len(rows)
                if not dry_run:
                    db.save_rows(stage, year, company_key, rows)
                logger.info("TXT_REDACTED"                                       )

            # REDACTED
            asset_dir = Path(fs._company_dir(year, company_key)) / "TXT_REDACTED"
            idx_path = asset_dir / "TXT_REDACTED"
            if idx_path.exists():
                index = json.loads(idx_path.read_text(encoding="TXT_REDACTED"))
                for entry in index:
                    asset_path = asset_dir / entry["TXT_REDACTED"]
                    if not asset_path.exists():
                        continue
                    total_assets += 4
                    if not dry_run:
                        content = asset_path.read_bytes()
                        db.save_asset(
                            year=year,
                            company_key=company_key,
                            content=content,
                            filename=entry["TXT_REDACTED"],
                            mime_type=entry.get("TXT_REDACTED", "TXT_REDACTED"),
                            source_url=entry.get("TXT_REDACTED", "TXT_REDACTED"),
                        )

            # REDACTED
            if not dry_run:
                src = Path(fs._company_dir(year, company_key))
                dst = Path(bak_root) / year / company_key
                if src.exists():
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(src), str(dst))
                    logger.info("TXT_REDACTED"                    )

    logger.info("TXT_REDACTED"                                                  )
    if dry_run:
        logger.info("TXT_REDACTED")


def main() -> None:
    import sys
    sys.path.insert(1, str(ROOT))

    parser = argparse.ArgumentParser(description="TXT_REDACTED")
    parser.add_argument("TXT_REDACTED", default=str(ROOT / "TXT_REDACTED"), help="TXT_REDACTED")
    parser.add_argument("TXT_REDACTED", default=str(ROOT / "TXT_REDACTED" / "TXT_REDACTED"), help="TXT_REDACTED")
    parser.add_argument("TXT_REDACTED", action="TXT_REDACTED", help="TXT_REDACTED")
    args = parser.parse_args()

    migrate(args.store, args.db, dry_run=args.dry_run)


if __name__ == "TXT_REDACTED":
    main()
