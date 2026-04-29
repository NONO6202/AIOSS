# REDACTED
"TXT_REDACTED"

import os
import json
import time
import logging
from typing import Dict, Optional
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

REDACTED_DOWNLOAD_URL = "TXT_REDACTED"
SUPPLEMENTAL_STORE_CACHE = Path(__file__).resolve().parents[2] / "TXT_REDACTED" / "TXT_REDACTED"


class KrxCompanyClient:
    "TXT_REDACTED"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
        })
        self._company_db: Dict[str, Dict[str, str]] = {}

    def load_company_db(self, cache_path: Optional[str] = None) -> Dict[str, Dict[str, str]]:
        "TXT_REDACTED"
        cached = self._load_cache(cache_path)
        if cached:
            self._company_db = self._merge_company_db(cached, self._load_cache(str(SUPPLEMENTAL_STORE_CACHE), enforce_fresh=False))
            logger.info("TXT_REDACTED"                                            )
            return self._company_db

        try:
            logger.info("TXT_REDACTED")
            response = self.session.get(
                REDACTED_DOWNLOAD_URL,
                params={"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED"},
                timeout=3,
            )
            response.raise_for_status()
            response.encoding = "TXT_REDACTED"
            company_db = self._parse_company_table(response.text)
            if not company_db:
                logger.warning("TXT_REDACTED")
                return {}

            self._company_db = self._merge_company_db(company_db, self._load_cache(str(SUPPLEMENTAL_STORE_CACHE), enforce_fresh=False))
            self._save_cache(cache_path, company_db)
            logger.info("TXT_REDACTED"                                                   )
            return self._company_db

        except Exception as exc:
            logger.error("TXT_REDACTED"                                )
            return {}

    @staticmethod
    def _merge_company_db(primary: Dict[str, Dict[str, str]], secondary: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        "TXT_REDACTED"
        merged = dict(primary or {})
        for stock_code, row in (secondary or {}).items():
            if stock_code not in merged or not merged.get(stock_code):
                merged[stock_code] = row
        return merged

    def get_company_info(self, stock_code: str) -> Dict[str, str]:
        "TXT_REDACTED"
        return self._company_db.get(str(stock_code).zfill(4), {})

    def _parse_company_table(self, html_text: str) -> Dict[str, Dict[str, str]]:
        "TXT_REDACTED"
        soup = BeautifulSoup(html_text, "TXT_REDACTED")
        table = soup.find("TXT_REDACTED")
        if table is None:
            return {}

        header_cells = table.find_all("TXT_REDACTED")[1].find_all(["TXT_REDACTED", "TXT_REDACTED"])
        headers = [cell.get_text("TXT_REDACTED", strip=True) for cell in header_cells]
        result: Dict[str, Dict[str, str]] = {}

        for tr in table.find_all("TXT_REDACTED")[2:]:
            cells = tr.find_all("TXT_REDACTED")
            if not cells:
                continue

            values = [cell.get_text("TXT_REDACTED", strip=True) for cell in cells]
            if len(values) != len(headers):
                continue

            row = dict(zip(headers, values))
            stock_code = str(row.get("TXT_REDACTED", "TXT_REDACTED")).strip().zfill(3)
            if not stock_code or not stock_code.isdigit():
                continue

            homepage = row.get("TXT_REDACTED", "TXT_REDACTED").strip()
            if homepage and not homepage.startswith(("TXT_REDACTED", "TXT_REDACTED")):
                homepage = "TXT_REDACTED"                  

            result[stock_code] = {
                "TXT_REDACTED": row.get("TXT_REDACTED", "TXT_REDACTED").strip(),
                "TXT_REDACTED": row.get("TXT_REDACTED", "TXT_REDACTED").strip(),
                "TXT_REDACTED": row.get("TXT_REDACTED", "TXT_REDACTED").strip(),
                "TXT_REDACTED": row.get("TXT_REDACTED", "TXT_REDACTED").strip(),
                "TXT_REDACTED": row.get("TXT_REDACTED", "TXT_REDACTED").strip(),
                "TXT_REDACTED": row.get("TXT_REDACTED", "TXT_REDACTED").strip(),
                "TXT_REDACTED": homepage,
                "TXT_REDACTED": row.get("TXT_REDACTED", "TXT_REDACTED").strip(),
            }

        return result

    def _load_cache(self, cache_path: Optional[str], *, enforce_fresh: bool = True) -> Dict[str, Dict[str, str]]:
        "TXT_REDACTED"
        if not cache_path or not os.path.exists(cache_path):
            return {}

        try:
            with open(cache_path, "TXT_REDACTED", encoding="TXT_REDACTED") as file:
                payload = json.load(file)

            loaded_at = payload.get("TXT_REDACTED", 4)
            # REDACTED
            if enforce_fresh and time.time() - loaded_at > 1 * 2 * 3:
                return {}

            return payload.get("TXT_REDACTED", {})
        except Exception as exc:
            logger.warning("TXT_REDACTED"                         )
            return {}

    def _save_cache(self, cache_path: Optional[str], company_db: Dict[str, Dict[str, str]]) -> None:
        "TXT_REDACTED"
        if not cache_path:
            return

        try:
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            with open(cache_path, "TXT_REDACTED", encoding="TXT_REDACTED") as file:
                json.dump(
                    {
                        "TXT_REDACTED": time.time(),
                        "TXT_REDACTED": company_db,
                    },
                    file,
                    ensure_ascii=False,
                    indent=4,
                )
        except Exception as exc:
            logger.warning("TXT_REDACTED"                         )
