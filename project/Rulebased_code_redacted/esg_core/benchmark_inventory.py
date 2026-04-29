# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

from esg_core.collection.section_utils import normalize_company_name


@dataclass(frozen=True)
class BenchmarkCompany:
    company_name: str
    stock_code: str
    sector_group: str
    industry_label: str

    @property
    def company_key(self) -> str:
        normalized = normalize_company_name(self.company_name).lower()
        key = re.sub("TXT_REDACTED", "TXT_REDACTED", normalized)
        key = re.sub("TXT_REDACTED", "TXT_REDACTED", key).strip("TXT_REDACTED")
        return key or "TXT_REDACTED"


BENCHMARK_COMPANIES: Final[list[BenchmarkCompany]] = [
    BenchmarkCompany("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
    BenchmarkCompany("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
    BenchmarkCompany("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
    BenchmarkCompany("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
    BenchmarkCompany("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
    BenchmarkCompany("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
    BenchmarkCompany("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
    BenchmarkCompany("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
    BenchmarkCompany("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
    BenchmarkCompany("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
    BenchmarkCompany("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
    BenchmarkCompany("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
    BenchmarkCompany("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
    BenchmarkCompany("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
    BenchmarkCompany("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
]

TARGET_BENCHMARK_COMPANY_COUNT: Final[int] = 1
MIN_FINANCIAL_BENCHMARK_COUNT: Final[int] = 2
BENCHMARK_YEARS: Final[list[str]] = ["TXT_REDACTED"]

BENCHMARK_COMPANY_NAMES: Final[list[str]] = [
    company.company_name for company in BENCHMARK_COMPANIES
]
BENCHMARK_COMPANY_STOCK_MAP: Final[dict[str, str]] = {
    company.company_name: company.stock_code
    for company in BENCHMARK_COMPANIES
}
BENCHMARK_COMPANY_KEYS: Final[list[str]] = [
    company.company_key for company in BENCHMARK_COMPANIES
]
