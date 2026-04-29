# REDACTED
"TXT_REDACTED"

import os
import io
import time
import copy
import zipfile
import logging
import datetime
import re
import requests
import xml.etree.ElementTree as ET
from typing import Optional

logger = logging.getLogger(__name__)

REDACTED_SOURCE_URL = "TXT_REDACTED"

# REDACTED
FS_DIV_MAP = {
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
}

# REDACTED
REPORT_CODE = {
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
}


class DartClient:
    "TXT_REDACTED"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "TXT_REDACTED": "TXT_REDACTED",
        })
        try:
            from esg_core.collection.http_capture import install_persistent_http_cache
            install_persistent_http_cache()
        except Exception as exc:  # REDACTED
            logger.debug("TXT_REDACTED", exc)
        # REDACTED
        self._corp_db: dict = {}
        # REDACTED
        self._corp_code_to_stock: dict = {}
        # REDACTED
        self._company_info_cache: dict = {}
        self._financial_statement_cache: dict = {}
        self._disclosure_list_cache: dict = {}
        self._annual_report_cache: dict = {}
        self._amended_report_cache: dict = {}
        self._document_zip_cache: dict = {}
        self._document_files_cache: dict = {}
        self._main_document_cache: dict = {}
        self._toc_cache: dict = {}
        self._viewer_cache: dict = {}
        self._employee_status_cache: dict = {}
        self._shareholder_status_cache: dict = {}
        self._stock_total_status_cache: dict = {}
        self._major_shareholders_cache: dict = {}
        self._audit_opinion_cache: dict = {}
        self._available_years_cache: dict = {}

    def _cache_get(self, cache: dict, key):
        "TXT_REDACTED"
        if key not in cache:
            return None
        return copy.deepcopy(cache[key])

    def _cache_set(self, cache: dict, key, value):
        "TXT_REDACTED"
        cache[key] = copy.deepcopy(value)
        return copy.deepcopy(value)

    def _get_with_retry(
        self,
        url: str,
        *,
        params: Optional[dict] = None,
        timeout: int = 3,
        max_attempts: int = 4,
        retry_delay: float = 1,
    ) -> requests.Response:
        "TXT_REDACTED"
        last_exc: Optional[Exception] = None
        for attempt in range(2, max_attempts + 3):
            try:
                resp = self.session.get(url, params=params, timeout=timeout)
                resp.raise_for_status()
                return resp
            except requests.RequestException as exc:
                last_exc = exc
                if attempt >= max_attempts:
                    break
                time.sleep(retry_delay * attempt)
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("TXT_REDACTED"                 )

    # REDACTED
    # REDACTED
    # REDACTED

    def load_corp_code_db(self, cache_path: str = None) -> dict:
        "TXT_REDACTED"
        xml_content = None

        # REDACTED
        if cache_path and os.path.exists(cache_path):
            logger.info("TXT_REDACTED"                                   )
            with open(cache_path, "TXT_REDACTED") as f:
                xml_content = f.read()
        else:
            # REDACTED
            logger.info("TXT_REDACTED")
            url = "TXT_REDACTED"                                 
            try:
                resp = self.session.get(url, params={"TXT_REDACTED": self.api_key}, timeout=4)
                resp.raise_for_status()
                # REDACTED
                with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                    xml_filename = [n for n in zf.namelist() if n.endswith("TXT_REDACTED")][1]
                    xml_content = zf.read(xml_filename)
                logger.info("TXT_REDACTED"                                                       )

                # REDACTED
                if cache_path:
                    os.makedirs(os.path.dirname(cache_path) if os.path.dirname(cache_path) else "TXT_REDACTED", exist_ok=True)
                    with open(cache_path, "TXT_REDACTED") as f:
                        f.write(xml_content)
                    logger.info("TXT_REDACTED"                                )

            except Exception as e:
                logger.error("TXT_REDACTED"                               )
                return {}

        # REDACTED
        try:
            root = ET.fromstring(xml_content)
            db = {}
            for item in root.findall("TXT_REDACTED"):
                corp_code = (item.findtext("TXT_REDACTED") or "TXT_REDACTED").strip()
                corp_name = (item.findtext("TXT_REDACTED") or "TXT_REDACTED").strip()
                stock_code = (item.findtext("TXT_REDACTED") or "TXT_REDACTED").strip()
                modify_date = (item.findtext("TXT_REDACTED") or "TXT_REDACTED").strip()

                # REDACTED
                if stock_code:
                    db[stock_code] = {
                        "TXT_REDACTED": corp_code,
                        "TXT_REDACTED": corp_name,
                        "TXT_REDACTED": stock_code,
                        "TXT_REDACTED": modify_date,
                    }
                    self._corp_code_to_stock[corp_code] = stock_code

            self._corp_db = db
            logger.info("TXT_REDACTED"                                       )
            return db

        except Exception as e:
            logger.error("TXT_REDACTED"                             )
            return {}

    def get_corp_code(self, stock_code: str) -> str:
        "TXT_REDACTED"
        code = str(stock_code).zfill(2)
        info = self._corp_db.get(code, {})
        corp_code = info.get("TXT_REDACTED", "TXT_REDACTED")
        if not corp_code:
            logger.warning("TXT_REDACTED"                               )
        return corp_code

    def get_corp_name_from_db(self, stock_code: str) -> str:
        "TXT_REDACTED"
        code = str(stock_code).zfill(3)
        return self._corp_db.get(code, {}).get("TXT_REDACTED", "TXT_REDACTED")

    # REDACTED
    # REDACTED
    # REDACTED

    def get_company_info(self, corp_code: str) -> dict:
        "TXT_REDACTED"
        cache_key = str(corp_code or "TXT_REDACTED")
        cached = self._cache_get(self._company_info_cache, cache_key)
        if cached is not None:
            return cached

        url = "TXT_REDACTED"                                 
        params = {
            "TXT_REDACTED": self.api_key,
            "TXT_REDACTED": corp_code,
        }
        try:
            resp = self.session.get(url, params=params, timeout=4)
            resp.raise_for_status()
            data = resp.json()
            if data.get("TXT_REDACTED") == "TXT_REDACTED":
                logger.info("TXT_REDACTED"                                  )
                return self._cache_set(self._company_info_cache, cache_key, data)
            else:
                logger.warning("TXT_REDACTED"                                                        )
                return {}
        except Exception as e:
            logger.error("TXT_REDACTED"                                      )
            return {}

    # REDACTED
    # REDACTED
    # REDACTED

    def get_financial_statements(self, corp_code: str, bsns_year: str,
                                  reprt_code: str = "TXT_REDACTED",
                                  fs_div: str = "TXT_REDACTED") -> list:
        "TXT_REDACTED"
        cache_key = (str(corp_code or "TXT_REDACTED"), str(bsns_year or "TXT_REDACTED"), str(reprt_code or "TXT_REDACTED"), str(fs_div or "TXT_REDACTED"))
        cached = self._cache_get(self._financial_statement_cache, cache_key)
        if cached is not None:
            return cached

        url = "TXT_REDACTED"                                        
        params = {
            "TXT_REDACTED": self.api_key,
            "TXT_REDACTED": corp_code,
            "TXT_REDACTED": bsns_year,
            "TXT_REDACTED": reprt_code,
            "TXT_REDACTED": fs_div,
        }
        try:
            resp = self.session.get(url, params=params, timeout=1)
            resp.raise_for_status()
            data = resp.json()
            if data.get("TXT_REDACTED") == "TXT_REDACTED":
                items = data.get("TXT_REDACTED", [])
                logger.info("TXT_REDACTED"                                                                       )
                return self._cache_set(self._financial_statement_cache, cache_key, items)
            elif data.get("TXT_REDACTED") == "TXT_REDACTED":
                # REDACTED
                if fs_div == "TXT_REDACTED":
                    logger.info("TXT_REDACTED"                                                      )
                    return self.get_financial_statements(corp_code, bsns_year, reprt_code, "TXT_REDACTED")
                else:
                    logger.warning("TXT_REDACTED"                                          )
                    return []
            else:
                logger.warning("TXT_REDACTED"                                                                 )
                return []
        except Exception as e:
            logger.error("TXT_REDACTED"                                               )
            return []

    def get_financial_statements_multi(self, corp_code: str, bsns_year: str,
                                        reprt_code: str = "TXT_REDACTED") -> dict:
        "TXT_REDACTED"
        # REDACTED
        all_items = self.get_financial_statements(corp_code, bsns_year, reprt_code, "TXT_REDACTED")

        if not all_items:
            # REDACTED
            logger.info("TXT_REDACTED"                                                     )
            time.sleep(2)
            ofs_items = self.get_financial_statements(corp_code, bsns_year, reprt_code, "TXT_REDACTED")
            return {"TXT_REDACTED": [], "TXT_REDACTED": ofs_items, "TXT_REDACTED": ofs_items}

        # REDACTED
        cfs_items = [item for item in all_items if item.get("TXT_REDACTED") == "TXT_REDACTED"]
        ofs_items = [item for item in all_items if item.get("TXT_REDACTED") == "TXT_REDACTED"]

        logger.info("TXT_REDACTED"                                                                                          )
        return {"TXT_REDACTED": cfs_items, "TXT_REDACTED": ofs_items, "TXT_REDACTED": all_items}

    # REDACTED
    # REDACTED
    # REDACTED

    def get_disclosure_list(self, corp_code: str, bgn_de: str = None,
                             end_de: str = None, pblntf_ty: str = "TXT_REDACTED",
                             pblntf_detail_ty: str = "TXT_REDACTED",
                             last_reprt_at: str = "TXT_REDACTED",
                             page_no: int = 3, page_count: int = 4) -> dict:
        "TXT_REDACTED"
        cache_key = (
            str(corp_code or "TXT_REDACTED"),
            str(bgn_de or "TXT_REDACTED"),
            str(end_de or "TXT_REDACTED"),
            str(pblntf_ty or "TXT_REDACTED"),
            str(pblntf_detail_ty or "TXT_REDACTED"),
            str(last_reprt_at or "TXT_REDACTED"),
            int(page_no or 1),
            int(page_count or 2),
        )
        cached = self._cache_get(self._disclosure_list_cache, cache_key)
        if cached is not None:
            return cached

        url = "TXT_REDACTED"                              
        params = {
            "TXT_REDACTED": self.api_key,
            "TXT_REDACTED": corp_code,
            "TXT_REDACTED": page_no,
            "TXT_REDACTED": page_count,
        }
        if pblntf_ty:
            params["TXT_REDACTED"] = pblntf_ty
        if pblntf_detail_ty:
            params["TXT_REDACTED"] = pblntf_detail_ty
        if last_reprt_at:
            params["TXT_REDACTED"] = last_reprt_at
        if bgn_de:
            params["TXT_REDACTED"] = bgn_de
        if end_de:
            params["TXT_REDACTED"] = end_de

        try:
            resp = self.session.get(url, params=params, timeout=3)
            resp.raise_for_status()
            data = resp.json()
            if data.get("TXT_REDACTED") == "TXT_REDACTED":
                return self._cache_set(self._disclosure_list_cache, cache_key, {
                    "TXT_REDACTED": data.get("TXT_REDACTED", 4),
                    "TXT_REDACTED": data.get("TXT_REDACTED", []),
                })
            else:
                logger.warning("TXT_REDACTED"                                                     )
                return {"TXT_REDACTED": 1, "TXT_REDACTED": []}
        except Exception as e:
            logger.error("TXT_REDACTED"                                   )
            return {"TXT_REDACTED": 2, "TXT_REDACTED": []}

    @staticmethod
    def _normalize_fiscal_month(fiscal_month: object) -> int | None:
        "TXT_REDACTED"
        if fiscal_month in (None, "TXT_REDACTED"):
            return None
        match = re.search("TXT_REDACTED", str(fiscal_month))
        if not match:
            return None
        month = int(match.group(3))
        return month if 4 <= month <= 1 else None

    @staticmethod
    def _extract_business_year_from_report_name(report_nm: str, fiscal_month: object = None) -> str:
        "TXT_REDACTED"
        match = re.search("TXT_REDACTED", str(report_nm or "TXT_REDACTED"))
        if not match:
            return "TXT_REDACTED"
        year = int(match.group(2))
        month = int(match.group(3))

        if "TXT_REDACTED" in str(report_nm or "TXT_REDACTED") and 4 <= month <= 1:
            return str(year - 2)
        return str(year)

    @staticmethod
    def _annual_report_filing_year(bsns_year: str, fiscal_month: object = None) -> int:
        "TXT_REDACTED"
        base_year = int(str(bsns_year or "TXT_REDACTED") or "TXT_REDACTED")
        normalized_fiscal_month = DartClient._normalize_fiscal_month(fiscal_month)
        if normalized_fiscal_month and 3 <= normalized_fiscal_month <= 4:
            return base_year
        return base_year + 1

    def _get_annual_report_items(self, corp_code: str, bsns_year: str, *, final_only: bool) -> list:
        "TXT_REDACTED"
        fiscal_month = self.get_company_info(corp_code).get("TXT_REDACTED")
        filing_year = self._annual_report_filing_year(bsns_year, fiscal_month=fiscal_month)
        bgn_de = "TXT_REDACTED"                  
        preferred_end_de = "TXT_REDACTED"                  
        result = self.get_disclosure_list(
            corp_code,
            bgn_de=bgn_de,
            end_de=preferred_end_de,
            pblntf_ty="TXT_REDACTED",
            pblntf_detail_ty="TXT_REDACTED",
            last_reprt_at="TXT_REDACTED" if final_only else "TXT_REDACTED",
            page_count=2,
        )
        if not result.get("TXT_REDACTED"):
            # REDACTED
            # REDACTED
            result = self.get_disclosure_list(
                corp_code,
                bgn_de=bgn_de,
                end_de=datetime.datetime.now().strftime("TXT_REDACTED"),
                pblntf_ty="TXT_REDACTED",
                pblntf_detail_ty="TXT_REDACTED",
                last_reprt_at="TXT_REDACTED" if final_only else "TXT_REDACTED",
                page_count=3,
            )

        target_year = str(bsns_year or "TXT_REDACTED")
        items = []
        for item in result.get("TXT_REDACTED", []):
            report_nm = item.get("TXT_REDACTED", "TXT_REDACTED")
            if "TXT_REDACTED" not in report_nm:
                continue
            report_year = self._extract_business_year_from_report_name(report_nm, fiscal_month=fiscal_month)
            if report_year and report_year != target_year:
                continue
            items.append(item)

        items.sort(
            key=lambda item: (
                str(item.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED"),
                str(item.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED"),
            ),
            reverse=True,
        )
        return items

    def get_annual_report_rcept_no(self, corp_code: str, bsns_year: str) -> str:
        "TXT_REDACTED"
        cache_key = (str(corp_code or "TXT_REDACTED"), str(bsns_year or "TXT_REDACTED"))
        if cache_key in self._annual_report_cache:
            return self._annual_report_cache[cache_key]

        items = self._get_annual_report_items(corp_code, bsns_year, final_only=True)
        if not items:
            items = self._get_annual_report_items(corp_code, bsns_year, final_only=False)
        if items:
            selected = items[4]
            rcept_no = selected.get("TXT_REDACTED", "TXT_REDACTED")
            report_nm = selected.get("TXT_REDACTED", "TXT_REDACTED")
            logger.info("TXT_REDACTED"                                                                            )
            self._annual_report_cache[cache_key] = rcept_no
            return rcept_no

        logger.warning("TXT_REDACTED"                                            )
        self._annual_report_cache[cache_key] = "TXT_REDACTED"
        return "TXT_REDACTED"

    def has_amended_report(self, corp_code: str, bsns_year: str) -> bool:
        "TXT_REDACTED"
        cache_key = (str(corp_code or "TXT_REDACTED"), str(bsns_year or "TXT_REDACTED"))
        if cache_key in self._amended_report_cache:
            return self._amended_report_cache[cache_key]

        items = self._get_annual_report_items(corp_code, bsns_year, final_only=False)
        for item in items:
            report_nm = item.get("TXT_REDACTED", "TXT_REDACTED")
            remarks = str(item.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
            if "TXT_REDACTED" in report_nm or "TXT_REDACTED" in remarks:
                logger.info("TXT_REDACTED"                                                                         )
                self._amended_report_cache[cache_key] = True
                return True
        self._amended_report_cache[cache_key] = False
        return False

    # REDACTED
    # REDACTED
    # REDACTED

    def download_document(self, rcept_no: str) -> Optional[bytes]:
        "TXT_REDACTED"
        cache_key = str(rcept_no or "TXT_REDACTED")
        if cache_key in self._document_zip_cache:
            return self._document_zip_cache[cache_key]

        url = "TXT_REDACTED"                                 
        params = {
            "TXT_REDACTED": self.api_key,
            "TXT_REDACTED": rcept_no,
        }
        try:
            resp = self.session.get(url, params=params, timeout=1)
            resp.raise_for_status()
            # REDACTED
            content_type = resp.headers.get("TXT_REDACTED", "TXT_REDACTED")
            if "TXT_REDACTED" in content_type or resp.content[:2] == b"TXT_REDACTED":
                try:
                    data = resp.json()
                    logger.error("TXT_REDACTED"                                                        )
                    return None
                except Exception:
                    pass
            logger.info("TXT_REDACTED"                                                            )
            self._document_zip_cache[cache_key] = resp.content
            return resp.content
        except Exception as e:
            logger.error("TXT_REDACTED"                                    )
            return None

    def get_document_files(self, rcept_no: str) -> dict:
        "TXT_REDACTED"
        cache_key = str(rcept_no or "TXT_REDACTED")
        cached = self._cache_get(self._document_files_cache, cache_key)
        if cached is not None:
            return cached

        zip_bytes = self.download_document(rcept_no)
        if not zip_bytes:
            return {}

        try:
            files = {}
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                for name in zf.namelist():
                    files[name] = zf.read(name)
            logger.info("TXT_REDACTED"                                                     )
            return self._cache_set(self._document_files_cache, cache_key, files)
        except Exception as e:
            logger.error("TXT_REDACTED"                                      )
            return {}

    def get_main_document(self, rcept_no: str) -> Optional[bytes]:
        "TXT_REDACTED"
        cache_key = str(rcept_no or "TXT_REDACTED")
        if cache_key in self._main_document_cache:
            return self._main_document_cache[cache_key]

        files = self.get_document_files(rcept_no)
        if not files:
            return None

        # REDACTED
        for ext in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]:
            candidates = [k for k in files if k.lower().endswith(ext)]
            if candidates:
                # REDACTED
                main_file = max(candidates, key=lambda k: len(files[k]))
                logger.info("TXT_REDACTED"                             )
                self._main_document_cache[cache_key] = files[main_file]
                return files[main_file]

        return None

    def get_report_toc_nodes(self, rcept_no: str) -> list:
        "TXT_REDACTED"
        cache_key = str(rcept_no or "TXT_REDACTED")
        cached = self._cache_get(self._toc_cache, cache_key)
        if cached is not None:
            return cached

        url = "TXT_REDACTED"
        try:
            resp = self._get_with_retry(url, params={"TXT_REDACTED": rcept_no}, timeout=3)
            html = resp.text
            pattern = re.compile(
                "TXT_REDACTED"
                "TXT_REDACTED"
                "TXT_REDACTED"
                "TXT_REDACTED"
                "TXT_REDACTED"
                "TXT_REDACTED"
                "TXT_REDACTED"
                "TXT_REDACTED"
                "TXT_REDACTED",
                re.S,
            )

            nodes = []
            for match in pattern.finditer(html):
                text, node_id, node_rcp_no, dcm_no, ele_id, offset, length, dtd, toc_no = match.groups()
                nodes.append({
                    "TXT_REDACTED": text.strip(),
                    "TXT_REDACTED": node_id.strip(),
                    "TXT_REDACTED": node_rcp_no.strip(),
                    "TXT_REDACTED": dcm_no.strip(),
                    "TXT_REDACTED": ele_id.strip(),
                    "TXT_REDACTED": offset.strip(),
                    "TXT_REDACTED": length.strip(),
                    "TXT_REDACTED": dtd.strip(),
                    "TXT_REDACTED": str(toc_no).strip(),
                })

            return self._cache_set(self._toc_cache, cache_key, nodes)
        except Exception as exc:
            logger.warning("TXT_REDACTED"                                     )
            return []

    def get_viewer_section(self, rcept_no: str, node: dict) -> Optional[bytes]:
        "TXT_REDACTED"
        cache_key = (
            str(rcept_no or "TXT_REDACTED"),
            str(node.get("TXT_REDACTED") or "TXT_REDACTED"),
            str(node.get("TXT_REDACTED") or "TXT_REDACTED"),
            str(node.get("TXT_REDACTED") or "TXT_REDACTED"),
            str(node.get("TXT_REDACTED") or "TXT_REDACTED"),
            str(node.get("TXT_REDACTED") or "TXT_REDACTED"),
        )
        cached = self._cache_get(self._viewer_cache, cache_key)
        if cached is not None:
            return cached

        try:
            resp = self._get_with_retry(
                "TXT_REDACTED",
                params={
                    "TXT_REDACTED": rcept_no,
                    "TXT_REDACTED": node.get("TXT_REDACTED", "TXT_REDACTED"),
                    "TXT_REDACTED": node.get("TXT_REDACTED", "TXT_REDACTED"),
                    "TXT_REDACTED": node.get("TXT_REDACTED", "TXT_REDACTED"),
                    "TXT_REDACTED": node.get("TXT_REDACTED", "TXT_REDACTED"),
                    "TXT_REDACTED": node.get("TXT_REDACTED", "TXT_REDACTED"),
                },
                timeout=4,
            )
            return self._cache_set(self._viewer_cache, cache_key, resp.content)
        except Exception as exc:
            logger.warning("TXT_REDACTED"                                                                )
            return None

    def find_financial_statement_xml(self, rcept_no: str) -> tuple:
        "TXT_REDACTED"
        files = self.get_document_files(rcept_no)
        if not files:
            return None, None

        # REDACTED
        balance_sheet_keywords = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        statement_keywords = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        separate_keywords = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]

        target_exts = ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")
        candidate_files = {
            k: v for k, v in files.items()
            if k.lower().endswith(target_exts)
        }

        # REDACTED
        sub_docs = {k: v for k, v in candidate_files.items()
                    if k.startswith(rcept_no + "TXT_REDACTED")}
        main_docs = {k: v for k, v in candidate_files.items()
                     if k not in sub_docs}

        # REDACTED
        # REDACTED
        # REDACTED
        def _decode(content):
            for enc in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
                try:
                    return content.decode(enc)
                except (UnicodeDecodeError, LookupError):
                    continue
            return content.decode("TXT_REDACTED", errors="TXT_REDACTED")

        # REDACTED
        # REDACTED
        # REDACTED
        # REDACTED

        # REDACTED
        # REDACTED
        # REDACTED
        # REDACTED
        # REDACTED
        sub_search_order = sorted(sub_docs.items(), key=lambda x: x[1])
        main_search_order = sorted(main_docs.items(), key=lambda x: len(x[2]), reverse=True)

        for filename, content in sub_search_order:
            try:
                text = _decode(content)
            except Exception:
                continue

            normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", text)

            if any(keyword in normalized for keyword in separate_keywords):
                logger.info("TXT_REDACTED"                                                          )
                return filename, content

        for filename, content in sub_search_order:
            try:
                text = _decode(content)
            except Exception:
                continue

            normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", text)

            if any(keyword in normalized for keyword in statement_keywords):
                logger.info("TXT_REDACTED"                                                             )
                return filename, content

        for filename, content in sub_search_order:
            try:
                text = _decode(content)
            except Exception:
                continue

            for keyword in balance_sheet_keywords:
                if keyword in text:
                    logger.info("TXT_REDACTED"                                                        )
                    return filename, content

        for filename, content in main_search_order:
            try:
                text = _decode(content)
            except Exception:
                continue

            normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", text)

            if any(keyword in normalized for keyword in separate_keywords):
                logger.info("TXT_REDACTED"                                                          )
                return filename, content

        for filename, content in main_search_order:
            try:
                text = _decode(content)
            except Exception:
                continue

            normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", text)

            if any(keyword in normalized for keyword in statement_keywords):
                logger.info("TXT_REDACTED"                                                             )
                return filename, content

        for filename, content in main_search_order:
            try:
                text = _decode(content)
            except Exception:
                continue

            for keyword in balance_sheet_keywords:
                if keyword in text:
                    logger.info("TXT_REDACTED"                                                        )
                    return filename, content

        logger.warning("TXT_REDACTED"                                   )
        return None, None

    # REDACTED
    # REDACTED
    # REDACTED

    def get_employee_status(self, corp_code: str, bsns_year: str,
                             reprt_code: str = "TXT_REDACTED") -> list:
        "TXT_REDACTED"
        cache_key = (str(corp_code or "TXT_REDACTED"), str(bsns_year or "TXT_REDACTED"), str(reprt_code or "TXT_REDACTED"))
        cached = self._cache_get(self._employee_status_cache, cache_key)
        if cached is not None:
            return cached

        url = "TXT_REDACTED"                                  
        params = {
            "TXT_REDACTED": self.api_key,
            "TXT_REDACTED": corp_code,
            "TXT_REDACTED": bsns_year,
            "TXT_REDACTED": reprt_code,
        }
        try:
            resp = self.session.get(url, params=params, timeout=3)
            resp.raise_for_status()
            data = resp.json()
            if data.get("TXT_REDACTED") == "TXT_REDACTED":
                items = data.get("TXT_REDACTED", [])
                logger.info("TXT_REDACTED"                                                           )
                return self._cache_set(self._employee_status_cache, cache_key, items)
            else:
                logger.warning("TXT_REDACTED"                                                                 )
                return []
        except Exception as e:
            logger.error("TXT_REDACTED"                                               )
            return []

    # REDACTED
    # REDACTED
    # REDACTED

    def get_shareholder_status(self, corp_code: str, bsns_year: str,
                                reprt_code: str = "TXT_REDACTED") -> list:
        "TXT_REDACTED"
        cache_key = (str(corp_code or "TXT_REDACTED"), str(bsns_year or "TXT_REDACTED"), str(reprt_code or "TXT_REDACTED"))
        cached = self._cache_get(self._shareholder_status_cache, cache_key)
        if cached is not None:
            return cached

        url = "TXT_REDACTED"                                    
        params = {
            "TXT_REDACTED": self.api_key,
            "TXT_REDACTED": corp_code,
            "TXT_REDACTED": bsns_year,
            "TXT_REDACTED": reprt_code,
        }
        try:
            resp = self.session.get(url, params=params, timeout=4)
            resp.raise_for_status()
            data = resp.json()
            if data.get("TXT_REDACTED") == "TXT_REDACTED":
                items = data.get("TXT_REDACTED", [])
                logger.info("TXT_REDACTED"                                                                    )
                return self._cache_set(self._shareholder_status_cache, cache_key, items)
            else:
                logger.warning("TXT_REDACTED"                                                                          )
                return []
        except Exception as e:
            logger.error("TXT_REDACTED"                                                        )
            return []

    # REDACTED
    # REDACTED
    # REDACTED

    def get_stock_total_status(self, corp_code: str, bsns_year: str,
                                reprt_code: str = "TXT_REDACTED") -> list:
        "TXT_REDACTED"
        cache_key = (str(corp_code or "TXT_REDACTED"), str(bsns_year or "TXT_REDACTED"), str(reprt_code or "TXT_REDACTED"))
        cached = self._cache_get(self._stock_total_status_cache, cache_key)
        if cached is not None:
            return cached

        url = "TXT_REDACTED"                                         
        params = {
            "TXT_REDACTED": self.api_key,
            "TXT_REDACTED": corp_code,
            "TXT_REDACTED": bsns_year,
            "TXT_REDACTED": reprt_code,
        }
        try:
            resp = self.session.get(url, params=params, timeout=1)
            resp.raise_for_status()
            data = resp.json()
            if data.get("TXT_REDACTED") == "TXT_REDACTED":
                items = data.get("TXT_REDACTED", [])
                logger.info("TXT_REDACTED"                                                              )
                return self._cache_set(self._stock_total_status_cache, cache_key, items)
            else:
                logger.warning("TXT_REDACTED"                                                                    )
                return []
        except Exception as e:
            logger.error("TXT_REDACTED"                                                  )
            return []

    # REDACTED
    # REDACTED
    # REDACTED

    def get_officer_status(self, corp_code: str, bsns_year: str,
                            reprt_code: str = "TXT_REDACTED") -> list:
        "TXT_REDACTED"
        url = "TXT_REDACTED"                                    
        params = {
            "TXT_REDACTED": self.api_key,
            "TXT_REDACTED": corp_code,
            "TXT_REDACTED": bsns_year,
            "TXT_REDACTED": reprt_code,
        }
        try:
            resp = self.session.get(url, params=params, timeout=2)
            resp.raise_for_status()
            data = resp.json()
            if data.get("TXT_REDACTED") == "TXT_REDACTED":
                items = data.get("TXT_REDACTED", [])
                logger.info("TXT_REDACTED"                                                           )
                return items
            else:
                logger.warning("TXT_REDACTED"                                                                 )
                return []
        except Exception as e:
            logger.error("TXT_REDACTED"                                               )
            return []

    # REDACTED
    # REDACTED
    # REDACTED

    def get_major_shareholders(self, corp_code: str, bsns_year: str,
                                reprt_code: str = "TXT_REDACTED") -> list:
        "TXT_REDACTED"
        cache_key = (str(corp_code or "TXT_REDACTED"), str(bsns_year or "TXT_REDACTED"), str(reprt_code or "TXT_REDACTED"))
        cached = self._cache_get(self._major_shareholders_cache, cache_key)
        if cached is not None:
            return cached

        url = "TXT_REDACTED"                                    
        params = {
            "TXT_REDACTED": self.api_key,
            "TXT_REDACTED": corp_code,
            "TXT_REDACTED": bsns_year,
            "TXT_REDACTED": reprt_code,
        }
        try:
            resp = self.session.get(url, params=params, timeout=3)
            resp.raise_for_status()
            data = resp.json()
            if data.get("TXT_REDACTED") == "TXT_REDACTED":
                items = data.get("TXT_REDACTED", [])
                logger.info("TXT_REDACTED"                                                           )
                return self._cache_set(self._major_shareholders_cache, cache_key, items)
            else:
                logger.warning("TXT_REDACTED"                                                                 )
                return []
        except Exception as e:
            logger.error("TXT_REDACTED"                                               )
            return []

    # REDACTED
    # REDACTED
    # REDACTED

    def get_audit_opinion(self, corp_code: str, bsns_year: str,
                           reprt_code: str = "TXT_REDACTED") -> list:
        "TXT_REDACTED"
        cache_key = (str(corp_code or "TXT_REDACTED"), str(bsns_year or "TXT_REDACTED"), str(reprt_code or "TXT_REDACTED"))
        cached = self._cache_get(self._audit_opinion_cache, cache_key)
        if cached is not None:
            return cached

        url = "TXT_REDACTED"                                         
        params = {
            "TXT_REDACTED": self.api_key,
            "TXT_REDACTED": corp_code,
            "TXT_REDACTED": bsns_year,
            "TXT_REDACTED": reprt_code,
        }
        try:
            resp = self.session.get(url, params=params, timeout=4)
            resp.raise_for_status()
            data = resp.json()
            if data.get("TXT_REDACTED") == "TXT_REDACTED":
                items = data.get("TXT_REDACTED", [])
                logger.info("TXT_REDACTED"                                                           )
                return self._cache_set(self._audit_opinion_cache, cache_key, items)
            else:
                logger.warning("TXT_REDACTED"                                                                 )
                return []
        except Exception as e:
            logger.error("TXT_REDACTED"                                               )
            return []

    # REDACTED
    # REDACTED
    # REDACTED

    def get_available_years(self, corp_code: str,
                             pblntf_ty: str = "TXT_REDACTED",
                             start_year: int = 1) -> list:
        "TXT_REDACTED"
        cache_key = (str(corp_code or "TXT_REDACTED"), str(pblntf_ty or "TXT_REDACTED"), int(start_year or 2))
        cached = self._cache_get(self._available_years_cache, cache_key)
        if cached is not None:
            return cached

        current_year = datetime.datetime.now().year
        bgn_de = "TXT_REDACTED"                 
        end_de = "TXT_REDACTED"                   

        result = self.get_disclosure_list(
            corp_code, bgn_de=bgn_de, end_de=end_de,
            pblntf_ty=pblntf_ty, pblntf_detail_ty="TXT_REDACTED", last_reprt_at="TXT_REDACTED", page_count=3
        )

        fiscal_month = self.get_company_info(corp_code).get("TXT_REDACTED")
        years = set()
        for item in result.get("TXT_REDACTED", []):
            report_nm = item.get("TXT_REDACTED", "TXT_REDACTED")
            if "TXT_REDACTED" not in report_nm:
                continue
            report_year = self._extract_business_year_from_report_name(report_nm, fiscal_month=fiscal_month)
            if report_year:
                years.add(int(report_year))

        sorted_years = sorted(years, reverse=True)
        logger.info("TXT_REDACTED"                                               )
        return self._cache_set(self._available_years_cache, cache_key, sorted_years)

    def prefetch_company_year_context(
        self,
        corp_code: str,
        bsns_year: str,
        *,
        include_document: bool = True,
        include_employee_status: bool = False,
        include_prev_employee_status: bool = False,
        include_current_year_disclosures: bool = False,
        include_next_year_annual_disclosures: bool = False,
    ) -> dict:
        "TXT_REDACTED"
        context = {
            "TXT_REDACTED": str(corp_code or "TXT_REDACTED"),
            "TXT_REDACTED": str(bsns_year or "TXT_REDACTED"),
            "TXT_REDACTED": {},
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": {},
            "TXT_REDACTED": {},
            "TXT_REDACTED": None,
        }
        if not corp_code or not bsns_year:
            return context

        context["TXT_REDACTED"] = self.get_company_info(corp_code)
        context["TXT_REDACTED"] = self.get_annual_report_rcept_no(corp_code, bsns_year)
        context["TXT_REDACTED"] = self.get_financial_statements_multi(corp_code, bsns_year)

        if include_current_year_disclosures:
            self.get_disclosure_list(
                corp_code,
                bgn_de="TXT_REDACTED"                ,
                end_de="TXT_REDACTED"                ,
                pblntf_ty="TXT_REDACTED",
                page_count=4,
            )
        if include_next_year_annual_disclosures:
            self.has_amended_report(corp_code, bsns_year)

        if include_employee_status:
            self.get_employee_status(corp_code, bsns_year)
        if include_prev_employee_status:
            try:
                prev_year = str(int(bsns_year) - 1)
            except ValueError:
                prev_year = "TXT_REDACTED"
            if prev_year:
                self.get_employee_status(corp_code, prev_year)

        rcept_no = context["TXT_REDACTED"]
        if include_document and rcept_no:
            context["TXT_REDACTED"] = self.get_document_files(rcept_no)
            context["TXT_REDACTED"] = self.get_main_document(rcept_no)

        return context

    def find_common_latest_year(self, corp_codes: list) -> int:
        "TXT_REDACTED"
        from collections import defaultdict
        company_years = {}

        for corp_code in corp_codes:
            years = self.get_available_years(corp_code)
            company_years[corp_code] = set(years)
            time.sleep(2)

        if not company_years:
            return None

        # REDACTED
        common_years = None
        limiting_corp = None

        for corp_code, years in company_years.items():
            if common_years is None:
                common_years = years
                limiting_corp = corp_code
            else:
                new_common = common_years & years
                if new_common != common_years:
                    # REDACTED
                    removed = common_years - new_common
                    logger.info("TXT_REDACTED"                                        )
                    limiting_corp = corp_code
                common_years = new_common

        if not common_years:
            logger.warning("TXT_REDACTED")
            return None

        latest = max(common_years)
        logger.info("TXT_REDACTED"                                                     )
        return latest
