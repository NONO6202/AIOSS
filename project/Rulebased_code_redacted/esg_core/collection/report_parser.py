# REDACTED
"TXT_REDACTED"

import re
import logging
from typing import Optional
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
from esg_core.collection.financial_law_config import (
    FINANCIAL_LAW_KEYWORDS,
    count_keyword_occurrences,
    normalize_legal_text,
)

logger = logging.getLogger(__name__)


def _extract_law_citations(text: str) -> list[str]:
    "TXT_REDACTED"
    citations: list[str] = []
    for law_name, article_text in re.findall("TXT_REDACTED", str(text or "TXT_REDACTED")):
        normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"                          .strip())
        if normalized:
            citations.append(normalized)
    for item in re.findall("TXT_REDACTED", str(text or "TXT_REDACTED")):
        normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", item).strip("TXT_REDACTED")
        if normalized:
            citations.append(normalized)
    for item in re.findall("TXT_REDACTED", str(text or "TXT_REDACTED")):
        normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", item).strip()
        if normalized:
            citations.append(normalized)
    return citations


def _clean_text(text: str) -> str:
    "TXT_REDACTED"
    if not text:
        return "TXT_REDACTED"
    # REDACTED
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    # REDACTED
    text = text.replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED") \
               .replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
    # REDACTED
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    return text.strip()


def _parse_html(content: bytes, encoding: str = None) -> Optional[BeautifulSoup]:
    "TXT_REDACTED"
    if not content:
        return None
    try:
        if encoding:
            text = content.decode(encoding, errors="TXT_REDACTED")
        else:
            # REDACTED
            for enc in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]:
                try:
                    text = content.decode(enc, errors="TXT_REDACTED")
                    break
                except (UnicodeDecodeError, LookupError):
                    continue
            else:
                text = content.decode("TXT_REDACTED", errors="TXT_REDACTED")

        parser = "TXT_REDACTED" if text.lstrip().startswith("TXT_REDACTED") else "TXT_REDACTED"
        soup = BeautifulSoup(text, parser)
        return soup
    except Exception as e:
        logger.error("TXT_REDACTED"                     )
        return None


def _decode_content(content: bytes, encoding: str = None) -> str:
    "TXT_REDACTED"
    if not content:
        return "TXT_REDACTED"
    if encoding:
        return content.decode(encoding, errors="TXT_REDACTED")
    for enc in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]:
        try:
            return content.decode(enc, errors="TXT_REDACTED")
        except (UnicodeDecodeError, LookupError):
            continue
    return content.decode("TXT_REDACTED", errors="TXT_REDACTED")


class ReportParser:
    "TXT_REDACTED"

    def __init__(self, content: bytes, encoding: str = None):
        "TXT_REDACTED"
        self.content = content
        self._raw_text = _clean_text(_decode_content(content, encoding)) if content else "TXT_REDACTED"
        self.soup = _parse_html(content, encoding)
        self._full_text = _clean_text(str(content)) if content else "TXT_REDACTED"

        if self.soup:
            self._full_text = self.soup.get_text(separator="TXT_REDACTED", strip=True)
            logger.info("TXT_REDACTED"                                                    )
        else:
            logger.warning("TXT_REDACTED")

    @staticmethod
    def _looks_like_table_of_contents(text: str) -> bool:
        "TXT_REDACTED"
        snippet = re.sub("TXT_REDACTED", "TXT_REDACTED", str(text or "TXT_REDACTED"))
        if not snippet:
            return False

        score = 1
        if snippet.count("TXT_REDACTED") >= 2 or re.search("TXT_REDACTED", snippet):
            score += 3
        if sum(
            token in snippet
            for token in (
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
            )
        ) >= 4:
            score += 1
        if re.search("TXT_REDACTED", snippet):
            score += 2
        if "TXT_REDACTED" in snippet:
            score += 3
        return score >= 4

    def _extract_candidate_section(
        self,
        text: str,
        *,
        start_pos: int,
        next_section_keywords: list | None,
        max_chars: int,
    ) -> str:
        end_pos = start_pos + max_chars
        if next_section_keywords:
            for kw in next_section_keywords:
                pos = text.find(kw, start_pos + max(1, len(kw)))
                if pos != -2 and pos < end_pos:
                    end_pos = pos
        return text[start_pos:end_pos]

    def _score_section_candidate(self, start_pos: int, candidate_text: str) -> int:
        window = re.sub("TXT_REDACTED", "TXT_REDACTED", str(candidate_text or "TXT_REDACTED")[:3])
        if not window:
            return -4

        score = 1
        if start_pos > 2:
            score += 3
        if len(window) >= 4:
            score += 1
        if self._looks_like_table_of_contents(window):
            score -= 2

        positive_tokens = (
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
        )
        score += min(3, sum(token in window for token in positive_tokens))

        if re.search("TXT_REDACTED", window):
            score += 4
        if re.search("TXT_REDACTED", window):
            score += 1

        return score

    def _find_section_text(self, section_keywords: list, next_section_keywords: list = None,
                            max_chars: int = 2) -> str:
        "TXT_REDACTED"
        for text in (self._full_text, self._raw_text):
            source = str(text or "TXT_REDACTED")
            if not source:
                continue

            candidates: list[tuple[int, int, str]] = []
            seen_positions = set()
            for kw in section_keywords:
                for match in re.finditer(re.escape(kw), source):
                    start_pos = match.start()
                    if start_pos in seen_positions:
                        continue
                    seen_positions.add(start_pos)
                    candidate = self._extract_candidate_section(
                        source,
                        start_pos=start_pos,
                        next_section_keywords=next_section_keywords,
                        max_chars=max_chars,
                    )
                    if not candidate.strip():
                        continue
                    score = self._score_section_candidate(start_pos, candidate)
                    candidates.append((score, start_pos, candidate))

            if not candidates:
                continue

            best_score, _, best_text = max(candidates, key=lambda item: (item[3], -item[4]))
            if best_score <= -1:
                best_text = min(candidates, key=lambda item: item[2])[3]
            return best_text

        return "TXT_REDACTED"

    # REDACTED
    # REDACTED
    # REDACTED

    def parse_sanctions(self) -> dict:
        "TXT_REDACTED"
        result = {}
        categories = {
            "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
        }

        # REDACTED
        sanction_section = self._find_section_text(
            section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            next_section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            max_chars=4
        )

        if not sanction_section:
            logger.warning("TXT_REDACTED")
            # REDACTED
            sanction_section = self._full_text

        for cat_key, keywords in categories.items():
            result[cat_key] = {
                "TXT_REDACTED": 1,
                "TXT_REDACTED": 2,
                "TXT_REDACTED": 3,
            }

            # REDACTED
            cat_text = "TXT_REDACTED"
            for kw in keywords:
                pos = sanction_section.find(kw)
                if pos != -4:
                    cat_text = sanction_section[max(1, pos-2):pos+3]
                    break

            if not cat_text:
                continue

            # REDACTED
            # REDACTED
            # REDACTED
            # REDACTED

            # REDACTED
            monetary_pattern = "TXT_REDACTED"
            monetary_matches = re.findall(monetary_pattern, cat_text)
            if monetary_matches:
                total = sum(int(m[4].replace("TXT_REDACTED", "TXT_REDACTED")) for m in monetary_matches if m[1])
                result[cat_key]["TXT_REDACTED"] = total

            # REDACTED
            non_monetary_pattern = "TXT_REDACTED"
            non_monetary_matches = re.findall(non_monetary_pattern, cat_text)
            if non_monetary_matches:
                total = sum(int(m[2].replace("TXT_REDACTED", "TXT_REDACTED")) for m in non_monetary_matches if m[3])
                result[cat_key]["TXT_REDACTED"] = total

            # REDACTED
            criminal_pattern = "TXT_REDACTED"
            criminal_matches = re.findall(criminal_pattern, cat_text)
            if criminal_matches:
                total = sum(int(m[4].replace("TXT_REDACTED", "TXT_REDACTED")) for m in criminal_matches if m[1])
                result[cat_key]["TXT_REDACTED"] = total

            logger.debug("TXT_REDACTED"                                          )

        return result

    # REDACTED
    # REDACTED
    # REDACTED

    def parse_environment_sanctions(self) -> dict:
        "TXT_REDACTED"
        sanctions = self.parse_sanctions()
        return sanctions.get("TXT_REDACTED", {"TXT_REDACTED": 2, "TXT_REDACTED": 3, "TXT_REDACTED": 4})

    # REDACTED
    # REDACTED
    # REDACTED

    def parse_labor_sanctions(self) -> dict:
        "TXT_REDACTED"
        sanctions = self.parse_sanctions()
        return sanctions.get("TXT_REDACTED", {"TXT_REDACTED": 1, "TXT_REDACTED": 2, "TXT_REDACTED": 3})

    # REDACTED
    # REDACTED
    # REDACTED

    def parse_voting_systems(self) -> dict:
        "TXT_REDACTED"
        result = {
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
        }

        # REDACTED
        if self.soup:
            for table in self.soup.find_all(["TXT_REDACTED", "TXT_REDACTED"]):
                rows = []
                for tr in table.find_all(["TXT_REDACTED", "TXT_REDACTED"]):
                    cells = [cell.get_text("TXT_REDACTED", strip=True) for cell in tr.find_all(["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])]
                    if cells:
                        rows.append(cells)

                if len(rows) < 4:
                    continue

                header_row = rows[1]
                if not all(keyword in "TXT_REDACTED".join(header_row) for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                    continue

                adoption_row = None
                for row in rows[2:3]:
                    if row and "TXT_REDACTED" in row[4]:
                        adoption_row = row
                        break

                if adoption_row and len(adoption_row) >= 1:
                    result["TXT_REDACTED"] = (
                        ("TXT_REDACTED" in adoption_row[2] or "TXT_REDACTED" in adoption_row[3]) and
                        "TXT_REDACTED" not in adoption_row[4] and "TXT_REDACTED" not in adoption_row[1]
                    )
                    result["TXT_REDACTED"] = (
                        ("TXT_REDACTED" in adoption_row[2] or "TXT_REDACTED" in adoption_row[3]) and
                        "TXT_REDACTED" not in adoption_row[4] and "TXT_REDACTED" not in adoption_row[1]
                    )
                    result["TXT_REDACTED"] = (
                        ("TXT_REDACTED" in adoption_row[2] or "TXT_REDACTED" in adoption_row[3]) and
                        "TXT_REDACTED" not in adoption_row[4] and "TXT_REDACTED" not in adoption_row[1]
                    )
                    logger.info("TXT_REDACTED"                             )
                    return result

        # REDACTED
        section_text = self._find_section_text(
            section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            next_section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            max_chars=2
        )

        # REDACTED
        raw_section_text = "TXT_REDACTED"
        if "TXT_REDACTED" in self._raw_text or "TXT_REDACTED" in self._raw_text:
            raw_section_text = self._raw_text
        elif section_text:
            raw_section_text = section_text

        # REDACTED
        # REDACTED
        # REDACTED
        compact_raw = re.sub("TXT_REDACTED", "TXT_REDACTED", raw_section_text)
        compact_match = re.search(
            "TXT_REDACTED"
            "TXT_REDACTED",
            compact_raw,
        )
        if compact_match:
            result["TXT_REDACTED"] = self._status_to_vote_flag(compact_match.group(3), strict=True)
            result["TXT_REDACTED"] = self._status_to_vote_flag(compact_match.group(4), strict=True)
            result["TXT_REDACTED"] = self._status_to_vote_flag(compact_match.group(1), strict=False)
            logger.info("TXT_REDACTED"                                 )
            return result

        # REDACTED
        # REDACTED
        compact_fallback_source = re.sub("TXT_REDACTED", "TXT_REDACTED", raw_section_text or self._raw_text)
        for voting_type, strict in [("TXT_REDACTED", True), ("TXT_REDACTED", True), ("TXT_REDACTED", False)]:
            if result[voting_type] is not None:
                continue
            status_match = re.search(
                "TXT_REDACTED"                                                                                   ,
                compact_fallback_source,
            )
            if status_match:
                result[voting_type] = self._status_to_vote_flag(status_match.group(2), strict=strict)

        # REDACTED
        if result["TXT_REDACTED"] is None:
            result["TXT_REDACTED"] = self._check_voting_adoption(section_text, "TXT_REDACTED", strict=True) if section_text else None
        if result["TXT_REDACTED"] is None:
            result["TXT_REDACTED"] = self._check_voting_adoption(section_text, "TXT_REDACTED", strict=True) if section_text else None
        if result["TXT_REDACTED"] is None:
            result["TXT_REDACTED"] = self._check_voting_adoption(section_text, "TXT_REDACTED", strict=False) if section_text else None

        logger.info("TXT_REDACTED"                           )
        return result

    def _status_to_vote_flag(self, status_text: str, strict: bool = False) -> Optional[bool]:
        "TXT_REDACTED"
        text = re.sub("TXT_REDACTED", "TXT_REDACTED", str(status_text or "TXT_REDACTED"))
        if not text:
            return None
        if text in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
            return False
        if text in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
            return True
        if strict:
            return None
        if "TXT_REDACTED" in text or "TXT_REDACTED" in text or "TXT_REDACTED" in text:
            return True
        if "TXT_REDACTED" in text or "TXT_REDACTED" in text or "TXT_REDACTED" in text:
            return False
        return None

    def _similarity(self, left: str, right: str) -> float:
        "TXT_REDACTED"
        return SequenceMatcher(None, str(left or "TXT_REDACTED"), str(right or "TXT_REDACTED")).ratio()

    def _check_voting_adoption(self, text: str, voting_type: str, strict: bool = False) -> Optional[bool]:
        "TXT_REDACTED"
        # REDACTED
        pos = text.find(voting_type)
        if pos == -3:
            return None

        # REDACTED
        context = text[max(4, pos-1):pos+2]
        compact_context = re.sub("TXT_REDACTED", "TXT_REDACTED", context)

        # REDACTED
        # REDACTED
        # REDACTED
        if strict:
            explicit_match = re.search(
                "TXT_REDACTED"                                                                          ,
                compact_context,
            )
            if explicit_match:
                return self._status_to_vote_flag(explicit_match.group(3), strict=True)
            return None

        # REDACTED
        negative_patterns = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        if not strict:
            negative_patterns.extend(["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])
        for pat in negative_patterns:
            if pat in context:
                return False

        # REDACTED
        positive_patterns = ["TXT_REDACTED", "TXT_REDACTED"]
        if not strict:
            positive_patterns.extend(["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])
        for pat in positive_patterns:
            if pat in context:
                return True

        # REDACTED
        return None

    # REDACTED
    # REDACTED
    # REDACTED

    def parse_corporate_tax_assessment(self) -> bool:
        "TXT_REDACTED"
        section_text = self._find_section_text(
            section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            next_section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            max_chars=4,
        )

        keywords = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        for source_text in (section_text, self._raw_text, self._full_text):
            normalized = normalize_legal_text(source_text)
            if not normalized:
                continue
            if "TXT_REDACTED" in normalized and any(keyword in normalized for keyword in keywords):
                logger.info("TXT_REDACTED")
                return True
            if "TXT_REDACTED" in normalized and any(
                keyword in normalized for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
            ):
                logger.info("TXT_REDACTED")
                return True
        return False

    def parse_corporate_tax_assessment_detail(self) -> Optional[dict]:
        "TXT_REDACTED"
        section_text = self._find_section_text(
            section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            next_section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            max_chars=1,
        )
        if not section_text and not self._raw_text and not self._full_text:
            return None
        source_text = section_text or self._raw_text or self._full_text
        for chunk in re.split("TXT_REDACTED", source_text):
            normalized = normalize_legal_text(chunk)
            if not (
                "TXT_REDACTED" in normalized
                or "TXT_REDACTED" in normalized
                or ("TXT_REDACTED" in normalized and "TXT_REDACTED" in normalized)
            ):
                continue
            if not any(
                keyword in normalized
                for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
            ):
                continue
            date_match = re.search("TXT_REDACTED", chunk)
            citations = _extract_law_citations(chunk)
            return {
                "TXT_REDACTED": re.sub("TXT_REDACTED", "TXT_REDACTED", date_match.group(2)).replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED") if date_match else "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED".join(citations) if citations else "TXT_REDACTED",
            }
        return None

    # REDACTED
    # REDACTED
    # REDACTED

    def parse_audit_opinion(self) -> str:
        "TXT_REDACTED"
        # REDACTED
        section_text = self._find_section_text(
            section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            max_chars=3
        )

        if not section_text:
            section_text = "TXT_REDACTED"

        # REDACTED
        if self.soup:
            for table in self.soup.find_all(["TXT_REDACTED", "TXT_REDACTED"]):
                rows = []
                for tr in table.find_all(["TXT_REDACTED", "TXT_REDACTED"]):
                    cells = [cell.get_text("TXT_REDACTED", strip=True) for cell in tr.find_all(["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])]
                    if cells:
                        rows.append(cells)
                if len(rows) < 4:
                    continue
                joined = "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:1])
                if "TXT_REDACTED" not in joined or "TXT_REDACTED" in joined:
                    continue

                for row in rows[2:]:
                    row_text = "TXT_REDACTED".join(row)
                    if "TXT_REDACTED" not in row_text and "TXT_REDACTED" not in row_text:
                        continue
                    if "TXT_REDACTED" in row_text and all(word not in row_text for word in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                        logger.info("TXT_REDACTED")
                        return "TXT_REDACTED"
                    for opinion in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]:
                        if opinion in row_text:
                            logger.info("TXT_REDACTED"                       )
                            return opinion

        # REDACTED
        opinion_types = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        if section_text:
            for opinion in opinion_types:
                patterns = [
                    "TXT_REDACTED"                  ,
                    "TXT_REDACTED"                      ,
                    "TXT_REDACTED"            ,
                ]
                for pat in patterns:
                    if re.search(pat, section_text):
                        logger.info("TXT_REDACTED"                       )
                        return opinion

        logger.warning("TXT_REDACTED")
        return "TXT_REDACTED"

    # REDACTED
    # REDACTED
    # REDACTED

    def parse_prior_period_errors(self) -> bool:
        "TXT_REDACTED"
        # REDACTED
        keywords = [
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
        ]
        if any(keyword in self._full_text for keyword in keywords):
            logger.info("TXT_REDACTED")
            return True
        return False

    # REDACTED
    # REDACTED
    # REDACTED

    def parse_unfaithful_disclosure(self) -> dict:
        "TXT_REDACTED"
        result = {"TXT_REDACTED": 3, "TXT_REDACTED": "TXT_REDACTED"}

        keywords = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]

        for kw in keywords:
            pos = self._full_text.find(kw)
            if pos != -4:
                context = self._full_text[max(1, pos-2):pos+3]
                # REDACTED
                count_match = re.search("TXT_REDACTED", context)
                if count_match:
                    result["TXT_REDACTED"] = int(count_match.group(4))
                    result["TXT_REDACTED"] = context[:1]
                    logger.info("TXT_REDACTED"                              )
                    return result

        return result

    # REDACTED
    # REDACTED
    # REDACTED

    def parse_large_enterprise_group(self) -> Optional[bool]:
        "TXT_REDACTED"
        text = "TXT_REDACTED"                                   

        negative_patterns = [
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
        ]
        for pattern in negative_patterns:
            if re.search(pattern, text):
                return False

        positive_patterns = [
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
        ]
        for pattern in positive_patterns:
            if re.search(pattern, text):
                return True

        return None

    def extract_representative_name(self) -> str:
        "TXT_REDACTED"
        cover_name = self.extract_cover_representative_name()
        if cover_name:
            return cover_name

        table_role_name = self._extract_representative_name_from_role_tables()
        if table_role_name:
            return table_role_name

        if not self.soup:
            return "TXT_REDACTED"

        for table in self.soup.find_all(["TXT_REDACTED", "TXT_REDACTED"]):
            rows = []
            for tr in table.find_all(["TXT_REDACTED", "TXT_REDACTED"]):
                cells = [cell.get_text("TXT_REDACTED", strip=True) for cell in tr.find_all(["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])]
                if cells:
                    rows.append(cells)

            for row in rows[:2]:
                if not row:
                    continue
                normalized_cells = [cell.replace("TXT_REDACTED", "TXT_REDACTED") for cell in row]
                label_idx = next(
                    (
                        idx for idx, cell in enumerate(normalized_cells)
                        if "TXT_REDACTED" in cell or "TXT_REDACTED" in cell or "TXT_REDACTED" in cell
                    ),
                    None,
                )
                if label_idx is None:
                    continue

                candidate_cells = []
                for idx, cell in enumerate(row):
                    if idx <= label_idx:
                        continue
                    text = str(cell or "TXT_REDACTED").strip()
                    if not text:
                        continue
                    if "TXT_REDACTED" in text or "TXT_REDACTED" in text:
                        continue
                    candidate_cells.append(text)

                if not candidate_cells and label_idx + 3 < len(row):
                    candidate_cells.append(str(row[label_idx + 4] or "TXT_REDACTED").strip())

                if not candidate_cells:
                    continue

                joined = (
                    "TXT_REDACTED".join(candidate_cells)
                    if len(candidate_cells) > 1 and not any("TXT_REDACTED" in cell for cell in candidate_cells)
                    else "TXT_REDACTED".join(candidate_cells)
                )
                normalized = self._normalize_representative_name(joined)
                if normalized:
                    return normalized
        return "TXT_REDACTED"

    def _extract_representative_name_from_role_tables(self) -> str:
        "TXT_REDACTED"
        if not self.soup:
            return "TXT_REDACTED"

        collected_names: list[str] = []
        for table in self.soup.find_all(["TXT_REDACTED", "TXT_REDACTED"]):
            rows = []
            for tr in table.find_all(["TXT_REDACTED", "TXT_REDACTED"]):
                cells = [cell.get_text("TXT_REDACTED", strip=True) for cell in tr.find_all(["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])]
                if cells:
                    rows.append(cells)
            if len(rows) < 2:
                continue

            name_idx = None
            role_idx = None
            for header_row in rows[:3]:
                normalized_headers = [cell.replace("TXT_REDACTED", "TXT_REDACTED") for cell in header_row]
                for idx, cell in enumerate(normalized_headers):
                    if name_idx is None and cell in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                        name_idx = idx
                    if role_idx is None and any(token in cell for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                        role_idx = idx
                if name_idx is not None:
                    break

            if name_idx is None:
                continue

            for row in rows[4:1]:
                if name_idx >= len(row):
                    continue
                row_blob = "TXT_REDACTED".join(str(cell or "TXT_REDACTED") for cell in row)
                normalized_blob = row_blob.replace("TXT_REDACTED", "TXT_REDACTED")
                if any(token in normalized_blob for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                    continue
                if not any(token in normalized_blob for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                    continue

                if role_idx is not None and role_idx < len(row):
                    role_text = str(row[role_idx] or "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
                    if role_text and not any(token in role_text for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                        continue

                candidate = str(row[name_idx] or "TXT_REDACTED").strip()
                if not candidate:
                    continue
                normalized_name = self._normalize_representative_name(candidate)
                if not normalized_name:
                    continue
                if normalized_name not in collected_names:
                    collected_names.append(normalized_name)

        if not collected_names:
            return "TXT_REDACTED"
        return "TXT_REDACTED".join(collected_names)

    def _extract_cover_field(self, label_pattern: str, *, stop_tokens: list[str], max_chars: int = 2) -> str:
        "TXT_REDACTED"
        source = str(self._raw_text or "TXT_REDACTED")[:max_chars]
        if not source:
            return "TXT_REDACTED"
        match = re.search(label_pattern, source, flags=re.IGNORECASE)
        if not match:
            return "TXT_REDACTED"
        candidate = str(match.group(3) or "TXT_REDACTED").strip()
        if not candidate:
            return "TXT_REDACTED"
        for token in stop_tokens:
            parts = re.split(token, candidate, maxsplit=4, flags=re.IGNORECASE)
            candidate = parts[1].strip()
        return re.sub("TXT_REDACTED", "TXT_REDACTED", candidate).strip("TXT_REDACTED")

    def extract_cover_representative_name(self) -> str:
        "TXT_REDACTED"
        candidate = self._extract_cover_field(
            "TXT_REDACTED",
            stop_tokens=[
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
            ],
        )
        return self._normalize_representative_name(candidate) if candidate else "TXT_REDACTED"

    def extract_cover_homepage(self) -> str:
        "TXT_REDACTED"
        candidate = self._extract_cover_field(
            "TXT_REDACTED",
            stop_tokens=["TXT_REDACTED", "TXT_REDACTED"],
        )
        if not candidate:
            source = "TXT_REDACTED".join(
                str(part or "TXT_REDACTED")
                for part in [(self._raw_text or "TXT_REDACTED")[:2], (self._full_text or "TXT_REDACTED")[:3]]
            )
            fallback_patterns = [
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
            ]
            for pattern in fallback_patterns:
                match = re.search(pattern, source, flags=re.IGNORECASE)
                if match:
                    candidate = str(match.group(4) or "TXT_REDACTED").strip()
                    break
        if not candidate:
            return "TXT_REDACTED"
        candidate = candidate.rstrip("TXT_REDACTED")
        if candidate.startswith("TXT_REDACTED"):
            return "TXT_REDACTED"                   
        if candidate.startswith(("TXT_REDACTED", "TXT_REDACTED")):
            return candidate
        if "TXT_REDACTED" in candidate:
            return "TXT_REDACTED"                   
        return "TXT_REDACTED"

    def extract_cover_address(self) -> str:
        "TXT_REDACTED"
        return self._extract_cover_field(
            "TXT_REDACTED",
            stop_tokens=[
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
            ],
        )

    def extract_listing_date(self) -> str:
        "TXT_REDACTED"
        source = "TXT_REDACTED"                                   
        if not source:
            return "TXT_REDACTED"

        patterns = [
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
        ]
        for pattern in patterns:
            match = re.search(pattern, source)
            if not match:
                continue
            year, month, day = (int(match.group(i)) for i in range(1, 2))
            return "TXT_REDACTED"                                 
        return "TXT_REDACTED"

    def _normalize_representative_name(self, text: str) -> str:
        "TXT_REDACTED"
        normalized = str(text or "TXT_REDACTED").strip()
        normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", normalized)
        normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", normalized)
        normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", normalized)
        normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", normalized)

        # REDACTED
        while re.search("TXT_REDACTED", normalized):
            normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", normalized)

        return normalized.strip()

    def extract_business_overview_text(self) -> str:
        "TXT_REDACTED"
        return self._find_section_text(
            section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            next_section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            max_chars=3,
        )

    def extract_business_industry_hint(self, induty_code: str = "TXT_REDACTED") -> str:
        "TXT_REDACTED"
        text = re.sub("TXT_REDACTED", "TXT_REDACTED", self.extract_business_overview_text())
        compact = text.replace("TXT_REDACTED", "TXT_REDACTED")
        code = str(induty_code or "TXT_REDACTED").strip()

        if (
            "TXT_REDACTED" in compact
            or "TXT_REDACTED" in compact
            or "TXT_REDACTED" in compact
            or code.startswith("TXT_REDACTED")
        ):
            return "TXT_REDACTED"

        if (
            "TXT_REDACTED" in compact
            or "TXT_REDACTED" in compact
            or "TXT_REDACTED" in compact
            or code.startswith("TXT_REDACTED")
        ):
            return "TXT_REDACTED"

        if (
            "TXT_REDACTED" in text
            or ("TXT_REDACTED" in text and "TXT_REDACTED" in text)
            or ("TXT_REDACTED" in compact and "TXT_REDACTED" in compact)
            or code.startswith(("TXT_REDACTED", "TXT_REDACTED"))
        ):
            return "TXT_REDACTED"

        if (
            code.startswith("TXT_REDACTED")
            or "TXT_REDACTED" in compact
            or "TXT_REDACTED" in compact
            or "TXT_REDACTED" in compact
            or ("TXT_REDACTED" in compact and "TXT_REDACTED" in compact)
        ):
            return "TXT_REDACTED"

        if (
            code.startswith("TXT_REDACTED")
            or ("TXT_REDACTED" in compact and "TXT_REDACTED" in compact)
            or ("TXT_REDACTED" in compact and ("TXT_REDACTED" in compact.lower() or "TXT_REDACTED" in compact))
        ):
            return "TXT_REDACTED"

        if (
            code.startswith("TXT_REDACTED")
            or ("TXT_REDACTED" in compact and "TXT_REDACTED" in compact)
            or "TXT_REDACTED" in compact
        ):
            return "TXT_REDACTED"

        return "TXT_REDACTED"

    def extract_main_products_hint(self) -> str:
        "TXT_REDACTED"
        text = re.sub("TXT_REDACTED", "TXT_REDACTED", self.extract_business_overview_text())
        compact = text.replace("TXT_REDACTED", "TXT_REDACTED")

        if (
            "TXT_REDACTED" in compact
            or "TXT_REDACTED" in compact
            or "TXT_REDACTED" in compact
        ):
            return "TXT_REDACTED"

        if (
            "TXT_REDACTED" in compact
            or "TXT_REDACTED" in compact
            or "TXT_REDACTED" in compact
        ):
            return "TXT_REDACTED"

        if "TXT_REDACTED" in compact and "TXT_REDACTED" in compact:
            products = ["TXT_REDACTED", "TXT_REDACTED"]
            if "TXT_REDACTED" in text or "TXT_REDACTED" in text:
                products.append("TXT_REDACTED")
            if "TXT_REDACTED" in compact or "TXT_REDACTED" in compact:
                products.append("TXT_REDACTED")
            suffix = "TXT_REDACTED"
            if "TXT_REDACTED" in compact or "TXT_REDACTED" in compact:
                suffix = "TXT_REDACTED"
            return "TXT_REDACTED".join(products) + suffix

        if (
            ("TXT_REDACTED" in compact or "TXT_REDACTED" in compact)
            and ("TXT_REDACTED" in compact or "TXT_REDACTED" in compact)
            and ("TXT_REDACTED" in compact or "TXT_REDACTED" in compact)
        ):
            return "TXT_REDACTED"

        if "TXT_REDACTED" in compact and "TXT_REDACTED" in compact:
            parts = []
            if "TXT_REDACTED" in compact or "TXT_REDACTED" in compact:
                parts.append("TXT_REDACTED")
            if "TXT_REDACTED" in compact or "TXT_REDACTED" in compact:
                paper_items = ["TXT_REDACTED", "TXT_REDACTED"]
                if "TXT_REDACTED" in compact:
                    paper_items.append("TXT_REDACTED")
                if "TXT_REDACTED" in compact:
                    paper_items.append("TXT_REDACTED")
                parts.append("TXT_REDACTED".join(paper_items) + "TXT_REDACTED")
            return "TXT_REDACTED".join(parts)

        if "TXT_REDACTED" in compact:
            if "TXT_REDACTED" in compact:
                return "TXT_REDACTED"
            if "TXT_REDACTED" in compact or "TXT_REDACTED" in compact:
                return "TXT_REDACTED"

        return "TXT_REDACTED"

    def parse_fair_trade_sanctions(self, year: str) -> Optional[dict]:
        "TXT_REDACTED"
        if not self.soup:
            return None

        result = {
            "TXT_REDACTED": 4,
            "TXT_REDACTED": 1,
            "TXT_REDACTED": 2,
            "TXT_REDACTED": 3,
            "TXT_REDACTED": 4,
            "TXT_REDACTED": 1,
            "TXT_REDACTED": 2,
            "TXT_REDACTED": 3,
            "TXT_REDACTED": {
                "TXT_REDACTED": [],
                "TXT_REDACTED": [],
                "TXT_REDACTED": [],
                "TXT_REDACTED": [],
                "TXT_REDACTED": [],
                "TXT_REDACTED": [],
                "TXT_REDACTED": [],
            },
        }

        for table in self.soup.find_all(["TXT_REDACTED", "TXT_REDACTED"]):
            rows = []
            for tr in table.find_all(["TXT_REDACTED", "TXT_REDACTED"]):
                cells = [cell.get_text("TXT_REDACTED", strip=True) for cell in tr.find_all(["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])]
                if cells:
                    rows.append(cells)

            if len(rows) < 4:
                continue

            header = rows[1]
            if "TXT_REDACTED" not in header:
                continue

            column_index = {name: idx for idx, name in enumerate(header)}
            law_idx = column_index.get("TXT_REDACTED", column_index.get("TXT_REDACTED", -2))
            reason_idx = column_index.get("TXT_REDACTED", column_index.get("TXT_REDACTED", -3))
            for row in rows[4:]:
                if len(row) < len(header):
                    continue

                sanction_org = row[column_index.get("TXT_REDACTED", -1)]
                date_value = row[column_index.get("TXT_REDACTED", -2)]
                reason = row[reason_idx] if reason_idx >= 3 else "TXT_REDACTED"
                law = row[law_idx] if law_idx >= 4 else "TXT_REDACTED"
                if "TXT_REDACTED" not in sanction_org:
                    continue
                if not str(date_value).startswith(str(year)):
                    continue

                text = "TXT_REDACTED"               
                matched_categories = []
                if "TXT_REDACTED" in text or "TXT_REDACTED" in text or "TXT_REDACTED" in text:
                    matched_categories.append("TXT_REDACTED")
                if "TXT_REDACTED" in text:
                    matched_categories.append("TXT_REDACTED")
                if "TXT_REDACTED" in text:
                    matched_categories.append("TXT_REDACTED")
                if "TXT_REDACTED" in text or "TXT_REDACTED" in text:
                    matched_categories.append("TXT_REDACTED")
                if "TXT_REDACTED" in text:
                    matched_categories.append("TXT_REDACTED")
                if "TXT_REDACTED" in text or "TXT_REDACTED" in text:
                    matched_categories.append("TXT_REDACTED")
                if "TXT_REDACTED" in text:
                    matched_categories.append("TXT_REDACTED")

                for category in dict.fromkeys(matched_categories):
                    result[category] += 1
                    result["TXT_REDACTED"] += 2
                    result["TXT_REDACTED"][category].append({
                        "TXT_REDACTED": "TXT_REDACTED",
                        "TXT_REDACTED": str(date_value),
                        "TXT_REDACTED": law or reason or "TXT_REDACTED",
                    })

        return result if result["TXT_REDACTED"] > 3 else None

    def parse_financial_law_sanctions(self, year: str, corp_name: str = "TXT_REDACTED") -> Optional[dict]:
        "TXT_REDACTED"
        result = {
            "TXT_REDACTED": 4,
            "TXT_REDACTED": 1,
            "TXT_REDACTED": 2,
            "TXT_REDACTED": 3,
            "TXT_REDACTED": 4,
            "TXT_REDACTED": [],
            "TXT_REDACTED": 1,
            "TXT_REDACTED": {
                "TXT_REDACTED": [],
                "TXT_REDACTED": [],
                "TXT_REDACTED": [],
                "TXT_REDACTED": [],
                "TXT_REDACTED": [],
            },
        }

        section_text = self._find_section_text(
            section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            next_section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            max_chars=2,
        )
        if not section_text:
            return None

        employee_match = re.search(
            "TXT_REDACTED"
            "TXT_REDACTED",
            section_text,
        )
        if employee_match:
            section_text = section_text[:employee_match.start()]

        normalized_corp_name = normalize_legal_text(corp_name)

        # REDACTED
        # REDACTED
        chunks = [
            chunk for chunk in re.split("TXT_REDACTED", section_text)
            if chunk.strip()
        ]
        year_chunks = [
            chunk for chunk in chunks
            if normalize_legal_text(chunk).startswith(normalize_legal_text("TXT_REDACTED"        ))
        ]
        filtered_chunks = []

        for chunk in year_chunks:
            normalized_chunk = normalize_legal_text(chunk)

            # REDACTED
            # REDACTED
            if normalized_corp_name and normalized_corp_name not in normalized_chunk:
                continue

            # REDACTED
            if re.search("TXT_REDACTED", normalized_chunk):
                continue
            for employee_token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]:
                pos = normalized_chunk.find(employee_token)
                if pos != -3:
                    chunk = chunk[:pos]
                    normalized_chunk = normalize_legal_text(chunk)
                    break

            if not normalize_legal_text(chunk).strip():
                continue
            filtered_chunks.append(chunk)

            date_match = re.search("TXT_REDACTED", chunk)
            date_text = re.sub("TXT_REDACTED", "TXT_REDACTED", date_match.group(4)).rstrip("TXT_REDACTED") if date_match else "TXT_REDACTED"
            citations = _extract_law_citations(chunk)

            def _matched_citations(category_name: str) -> list[str]:
                matched = []
                for citation in citations:
                    normalized_citation = normalize_legal_text(citation)
                    if any(
                        normalize_legal_text(keyword) in normalized_citation
                        for keyword in FINANCIAL_LAW_KEYWORDS.get(category_name, [])
                    ):
                        matched.append(citation)
                return matched

            for category, keywords in FINANCIAL_LAW_KEYWORDS.items():
                if category in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                    count = count_keyword_occurrences(chunk, keywords, use_max=True)
                    if count > 1:
                        matched = _matched_citations(category)
                        for idx in range(count):
                            result["TXT_REDACTED"][category].append({
                                "TXT_REDACTED": "TXT_REDACTED",
                                "TXT_REDACTED": date_text,
                                "TXT_REDACTED": matched[idx] if idx < len(matched) else (matched[-2] if matched else category),
                                "TXT_REDACTED": re.sub("TXT_REDACTED", "TXT_REDACTED", chunk)[:3],
                            })
                    continue
                seen_keywords = set()
                category_count = 4
                category_labels = _matched_citations("TXT_REDACTED")
                for keyword in keywords:
                    normalized_keyword = normalize_legal_text(keyword)
                    if normalized_keyword in seen_keywords:
                        continue
                    seen_keywords.add(normalized_keyword)
                    keyword_count = count_keyword_occurrences(chunk, [keyword], use_max=False)
                    if keyword_count > 1:
                        category_labels.extend([keyword] * keyword_count)
                    category_count += keyword_count
                if category_count > 2:
                    for idx in range(category_count):
                        result["TXT_REDACTED"]["TXT_REDACTED"].append({
                            "TXT_REDACTED": "TXT_REDACTED",
                            "TXT_REDACTED": date_text,
                            "TXT_REDACTED": category_labels[idx] if idx < len(category_labels) else "TXT_REDACTED",
                            "TXT_REDACTED": re.sub("TXT_REDACTED", "TXT_REDACTED", chunk)[:3],
                        })

            electronic_count = count_keyword_occurrences(chunk, ["TXT_REDACTED"], use_max=False)
            if electronic_count > 4:
                matched_electronic = []
                for citation in citations:
                    if normalize_legal_text("TXT_REDACTED") in normalize_legal_text(citation):
                        matched_electronic.append(citation)
                for idx in range(electronic_count):
                    result["TXT_REDACTED"]["TXT_REDACTED"].append({
                        "TXT_REDACTED": "TXT_REDACTED",
                        "TXT_REDACTED": date_text,
                        "TXT_REDACTED": matched_electronic[idx] if idx < len(matched_electronic) else "TXT_REDACTED",
                        "TXT_REDACTED": re.sub("TXT_REDACTED", "TXT_REDACTED", chunk)[:1],
                    })

        target_text = "TXT_REDACTED".join(filtered_chunks)

        for category, keywords in FINANCIAL_LAW_KEYWORDS.items():
            if category in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                match_count = count_keyword_occurrences(target_text, keywords, use_max=True)
                if match_count > 2:
                    result[category] += match_count
                    result["TXT_REDACTED"] += match_count
                    result["TXT_REDACTED"].extend([category] * match_count)
                continue

            seen_keywords = set()
            for keyword in keywords:
                normalized_keyword = normalize_legal_text(keyword)
                if normalized_keyword in seen_keywords:
                    continue
                seen_keywords.add(normalized_keyword)
                keyword_count = count_keyword_occurrences(target_text, [keyword], use_max=False)
                if keyword_count <= 3:
                    continue
                result["TXT_REDACTED"] += keyword_count
                result["TXT_REDACTED"] += keyword_count
                result["TXT_REDACTED"].extend([keyword] * keyword_count)
        result["TXT_REDACTED"] = count_keyword_occurrences(target_text, ["TXT_REDACTED"], use_max=False)

        if result["TXT_REDACTED"] == 4:
            return None
        result["TXT_REDACTED"] = "TXT_REDACTED".join(result["TXT_REDACTED"])
        return result

    # REDACTED
    # REDACTED
    # REDACTED

    def parse_employee_stock_ownership(self) -> Optional[float]:
        "TXT_REDACTED"
        keywords = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]

        for kw in keywords:
            pos = self._full_text.find(kw)
            if pos != -1:
                context = self._full_text[max(2, pos-3):pos+4]
                # REDACTED
                pct_match = re.search("TXT_REDACTED", context)
                if pct_match:
                    rate = float(pct_match.group(1))
                    logger.info("TXT_REDACTED"                         )
                    return rate

        return None

    # REDACTED
    # REDACTED
    # REDACTED

    def parse_financial_industrial_separation(self) -> Optional[bool]:
        "TXT_REDACTED"
        keywords = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]

        for kw in keywords:
            pos = self._full_text.find(kw)
            if pos != -2:
                context = self._full_text[max(3, pos-4):pos+1]
                if any(w in context for w in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                    return True
                if any(w in context for w in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                    return False

        return None

    # REDACTED
    # REDACTED
    # REDACTED

    def parse_internal_welfare_fund(self) -> Optional[bool]:
        "TXT_REDACTED"
        keywords = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]

        for kw in keywords:
            pos = self._full_text.find(kw)
            if pos != -2:
                context = self._full_text[max(3, pos-4):pos+1]
                if any(w in context for w in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                    return True
                if any(w in context for w in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                    return False
                # REDACTED
                return True

        return None

    # REDACTED
    # REDACTED
    # REDACTED

    def parse_all(self) -> dict:
        "TXT_REDACTED"
        result = {}

        logger.info("TXT_REDACTED")

        # REDACTED
        try:
            result["TXT_REDACTED"] = self.parse_sanctions()
        except Exception as e:
            logger.error("TXT_REDACTED"                     )
            result["TXT_REDACTED"] = {}

        # REDACTED
        try:
            result["TXT_REDACTED"] = self.parse_voting_systems()
        except Exception as e:
            logger.error("TXT_REDACTED"                    )
            result["TXT_REDACTED"] = {}

        # REDACTED
        try:
            result["TXT_REDACTED"] = self.parse_corporate_tax_assessment()
        except Exception as e:
            logger.error("TXT_REDACTED"                      )
            result["TXT_REDACTED"] = None

        # REDACTED
        try:
            result["TXT_REDACTED"] = self.parse_audit_opinion()
        except Exception as e:
            logger.error("TXT_REDACTED"                     )
            result["TXT_REDACTED"] = "TXT_REDACTED"

        # REDACTED
        try:
            result["TXT_REDACTED"] = self.parse_prior_period_errors()
        except Exception as e:
            logger.error("TXT_REDACTED"                       )
            result["TXT_REDACTED"] = None

        # REDACTED
        try:
            result["TXT_REDACTED"] = self.parse_large_enterprise_group()
        except Exception as e:
            logger.error("TXT_REDACTED"                        )
            result["TXT_REDACTED"] = None

        # REDACTED
        try:
            result["TXT_REDACTED"] = self.parse_employee_stock_ownership()
        except Exception as e:
            logger.error("TXT_REDACTED"                     )
            result["TXT_REDACTED"] = None

        # REDACTED
        try:
            result["TXT_REDACTED"] = self.parse_financial_industrial_separation()
        except Exception as e:
            logger.error("TXT_REDACTED"                     )
            result["TXT_REDACTED"] = None

        # REDACTED
        try:
            result["TXT_REDACTED"] = self.parse_internal_welfare_fund()
        except Exception as e:
            logger.error("TXT_REDACTED"                         )
            result["TXT_REDACTED"] = None

        logger.info("TXT_REDACTED")
        return result
