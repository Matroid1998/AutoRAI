"""
Shared MIMIC-IV infrastructure for all section adapters.

Provides:
- Compressed CSV loading with caching
- Discharge note loading and section segmentation
- Admission window computation
- Common temporal filtering
"""

from __future__ import annotations

import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from ie.config import IEConfig, MIMICPaths, get_config
from ie.core.interfaces import FusionPolicy, IEAdapter, TemporalResolver
from ie.core.models import (
    Document,
    DocumentSection,
    EvidenceRecord,
    Mention,
    StructuredEvidence,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Section segmentation patterns for discharge summaries
# ---------------------------------------------------------------------------

# Common section headers in MIMIC discharge summaries
DISCHARGE_SECTION_PATTERNS = [
    r"(?i)^(chief complaint|cc)\s*:",
    r"(?i)^(history of present illness|hpi)\s*:",
    r"(?i)^(past medical history|pmh|pmhx)\s*:",
    r"(?i)^(past surgical history|psh)\s*:",
    r"(?i)^(social history|sh)\s*:",
    r"(?i)^(family history|fh|fhx)\s*:",
    r"(?i)^(medications on admission|home medications|admission medications)\s*:",
    r"(?i)^(allergies)\s*:",
    r"(?i)^(physical exam|physical examination|pe)\s*:",
    r"(?i)^(pertinent results|labs|laboratory)\s*:",
    r"(?i)^(imaging|radiology)\s*:",
    r"(?i)^(brief hospital course|hospital course)\s*:",
    r"(?i)^(discharge medications|medications on discharge)\s*:",
    r"(?i)^(discharge diagnosis|discharge diagnoses)\s*:",
    r"(?i)^(discharge disposition)\s*:",
    r"(?i)^(discharge condition)\s*:",
    r"(?i)^(discharge instructions)\s*:",
    r"(?i)^(followup instructions|follow-up|follow up)\s*:",
    r"(?i)^(active issues|active problems)\s*:",
]

# Compiled pattern for matching any section header
_SECTION_RE = re.compile(
    "|".join(f"({p})" for p in DISCHARGE_SECTION_PATTERNS),
    re.MULTILINE,
)


# ---------------------------------------------------------------------------
# Table loading utilities
# ---------------------------------------------------------------------------

def load_mimic_table(
    paths: MIMICPaths,
    module: str,
    table_name: str,
    usecols: list[str] | None = None,
    dtype: dict | None = None,
) -> pd.DataFrame:
    """
    Load a MIMIC table from compressed CSV.

    Args:
        paths: MIMIC path configuration.
        module: "hosp", "icu", or "note".
        table_name: Table name (e.g., "admissions", "diagnoses_icd").
        usecols: Columns to load (None = all).
        dtype: Column type overrides.

    Returns:
        DataFrame with the table data.
    """
    table_path = paths.table_path(module, table_name)
    if not table_path.exists():
        raise FileNotFoundError(
            f"MIMIC table not found: {table_path}. "
            f"Check that MIMIC-IV is extracted at {paths.mimic_iv_root}"
        )

    logger.info(f"Loading MIMIC table: {table_path}")
    df = pd.read_csv(
        table_path,
        compression="gzip",
        usecols=usecols,
        dtype=dtype,
        low_memory=False,
    )
    logger.info(f"  Loaded {len(df)} rows from {table_name}")
    return df


def load_table_for_episode(
    paths: MIMICPaths,
    module: str,
    table_name: str,
    episode_id: str,
    episode_column: str = "hadm_id",
    usecols: list[str] | None = None,
    dtype: dict | None = None,
) -> pd.DataFrame:
    """
    Load a MIMIC table filtered to a single episode.

    For very large tables, this reads the full table and filters.
    For production use, this should be replaced with a database query.
    """
    # Make sure the episode column is included in usecols
    if usecols is not None and episode_column not in usecols:
        usecols = [episode_column] + usecols

    df = load_mimic_table(paths, module, table_name, usecols=usecols, dtype=dtype)

    # Handle type mismatch (hadm_id can be int or float)
    try:
        episode_val = int(float(episode_id))
        df_filtered = df[df[episode_column] == episode_val]
    except (ValueError, TypeError):
        df_filtered = df[df[episode_column].astype(str) == str(episode_id)]

    logger.info(
        f"  Filtered to {len(df_filtered)} rows for "
        f"{episode_column}={episode_id}"
    )
    return df_filtered


# ---------------------------------------------------------------------------
# Discharge note segmentation
# ---------------------------------------------------------------------------

def segment_discharge_note(text: str) -> list[DocumentSection]:
    """
    Segment a discharge summary into named sections.

    Uses regex matching on common MIMIC discharge section headers.
    Returns sections with their text content and character offsets.
    """
    sections: list[DocumentSection] = []

    # Find all section header matches
    matches = list(_SECTION_RE.finditer(text))

    if not matches:
        # No headers found — return the whole text as one section
        return [DocumentSection(
            name="full_text",
            text=text,
            start_char=0,
            end_char=len(text),
        )]

    # Add content before the first header (if any)
    if matches[0].start() > 0:
        preamble = text[:matches[0].start()].strip()
        if preamble:
            sections.append(DocumentSection(
                name="preamble",
                text=preamble,
                start_char=0,
                end_char=matches[0].start(),
            ))

    # Extract each section
    for i, match in enumerate(matches):
        # Section name: clean up the matched header
        header_text = match.group(0).strip().rstrip(":")
        section_name = _normalize_section_name(header_text)

        # Section content: from after this header to next header (or end)
        content_start = match.end()
        content_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        content = text[content_start:content_end].strip()

        if content:
            sections.append(DocumentSection(
                name=section_name,
                text=content,
                start_char=content_start,
                end_char=content_end,
            ))

    return sections


def _normalize_section_name(header: str) -> str:
    """Normalize a section header to a canonical name."""
    header_lower = header.lower().strip()

    name_map = {
        "chief complaint": "chief_complaint",
        "cc": "chief_complaint",
        "history of present illness": "hpi",
        "hpi": "hpi",
        "past medical history": "past_medical_history",
        "pmh": "past_medical_history",
        "pmhx": "past_medical_history",
        "past surgical history": "past_surgical_history",
        "psh": "past_surgical_history",
        "social history": "social_history",
        "sh": "social_history",
        "family history": "family_history",
        "fh": "family_history",
        "fhx": "family_history",
        "medications on admission": "medications_on_admission",
        "home medications": "medications_on_admission",
        "admission medications": "medications_on_admission",
        "allergies": "allergies",
        "physical exam": "physical_exam",
        "physical examination": "physical_exam",
        "pe": "physical_exam",
        "pertinent results": "pertinent_results",
        "labs": "pertinent_results",
        "laboratory": "pertinent_results",
        "imaging": "imaging",
        "radiology": "imaging",
        "brief hospital course": "hospital_course",
        "hospital course": "hospital_course",
        "discharge medications": "discharge_medications",
        "medications on discharge": "discharge_medications",
        "discharge diagnosis": "discharge_diagnoses",
        "discharge diagnoses": "discharge_diagnoses",
        "discharge disposition": "discharge_disposition",
        "discharge condition": "discharge_condition",
        "discharge instructions": "discharge_instructions",
        "followup instructions": "followup",
        "follow-up": "followup",
        "follow up": "followup",
        "active issues": "active_issues",
        "active problems": "active_issues",
    }

    return name_map.get(header_lower, header_lower.replace(" ", "_"))


# ---------------------------------------------------------------------------
# Shared temporal resolver for MIMIC
# ---------------------------------------------------------------------------

class MIMICTemporalResolver(TemporalResolver):
    """
    Temporal resolver for MIMIC-IV data.

    Uses admission/discharge timestamps for lookback filtering.
    """

    def is_within_lookback(
        self,
        timestamp: str,
        reference_time: str,
        lookback_type: str,
        lookback_days: int | None = None,
    ) -> bool:
        """
        Check if a timestamp falls within the lookback window.

        For MIMIC hospital data in v1, we use "admission_window" as
        the default: anything during the hospitalization is in scope.
        """
        if lookback_type == "open":
            return True

        if not timestamp or not reference_time:
            return True  # can't filter without timestamps, include by default

        try:
            ts = pd.Timestamp(timestamp)
            ref = pd.Timestamp(reference_time)

            if lookback_type == "admission_window":
                # Within the hospitalization — this is the default for MIMIC
                return True  # all codes in the episode are in scope

            elif lookback_type == "fixed_days" and lookback_days is not None:
                cutoff = ref - pd.Timedelta(days=lookback_days)
                return ts >= cutoff

            else:
                return True

        except Exception as e:
            logger.debug(f"Temporal filtering error: {e}")
            return True  # include on error


# ---------------------------------------------------------------------------
# Shared clinical fusion policy
# ---------------------------------------------------------------------------

class ClinicalFusionPolicy(FusionPolicy):
    """
    Default clinical fusion policy for MIMIC-based MDS extraction.

    Implements the conflict resolution rules from the architecture doc:
    - Note negation overrides structured codes
    - Note+code agreement boosts confidence
    - Code without note → moderate confidence
    - Note without code → moderate confidence
    """

    def __init__(
        self,
        text_negation_overrides_code: bool = True,
        code_without_text_confidence: float = 0.5,
        text_without_code_confidence: float = 0.6,
        agreement_confidence: float = 0.9,
        uncertain_mention_confidence: float = 0.3,
    ):
        self.text_negation_overrides_code = text_negation_overrides_code
        self.code_without_text_confidence = code_without_text_confidence
        self.text_without_code_confidence = text_without_code_confidence
        self.agreement_confidence = agreement_confidence
        self.uncertain_mention_confidence = uncertain_mention_confidence

    def fuse_item_evidence(
        self,
        target_item_id: str,
        target_item_name: str,
        text_mentions: list[Mention],
        structured_evidence: list[StructuredEvidence],
    ) -> EvidenceRecord:
        """
        Fuse all evidence for a single target item.

        Decision logic:
        1. Separate affirmed, negated, and uncertain mentions
        2. Check agreement/conflict with structured evidence
        3. Apply priority rules
        """
        # Classify text mentions by assertion
        affirmed = [m for m in text_mentions if m.attributes.get("assertion") == "affirmed"]
        negated = [m for m in text_mentions if m.attributes.get("assertion") == "negated"]
        uncertain = [m for m in text_mentions if m.attributes.get("assertion") == "uncertain"]

        # Also filter by temporality — only current mentions support the item
        current_affirmed = [
            m for m in affirmed
            if m.attributes.get("temporality", "current") in ("current", "")
        ]

        has_text_support = len(current_affirmed) > 0
        has_text_negation = len(negated) > 0
        has_structured = len(structured_evidence) > 0

        # Decision matrix
        if has_text_support and has_structured:
            # AGREEMENT: both paths support
            return EvidenceRecord(
                target_item_id=target_item_id,
                target_item_name=target_item_name,
                supporting_mentions=current_affirmed,
                supporting_structured=structured_evidence,
                negative_mentions=negated,
                final_status="supported",
                confidence=self.agreement_confidence,
                reason_summary=(
                    f"Both text and structured evidence support {target_item_name}."
                ),
            )

        elif has_text_negation and has_structured and self.text_negation_overrides_code:
            # CONFLICT: text says no, code says yes → trust text negation
            return EvidenceRecord(
                target_item_id=target_item_id,
                target_item_name=target_item_name,
                supporting_mentions=[],
                supporting_structured=structured_evidence,
                negative_mentions=negated,
                final_status="negated",
                confidence=0.6,
                conflict_flags=["text_negation_overrides_code"],
                reason_summary=(
                    f"Text explicitly negates {target_item_name} despite "
                    f"structured code. Trusting text negation."
                ),
            )

        elif has_text_support and not has_structured:
            # TEXT ONLY: note mentions it but no structured code
            return EvidenceRecord(
                target_item_id=target_item_id,
                target_item_name=target_item_name,
                supporting_mentions=current_affirmed,
                supporting_structured=[],
                negative_mentions=negated,
                final_status="supported",
                confidence=self.text_without_code_confidence,
                reason_summary=(
                    f"Text evidence supports {target_item_name} but no "
                    f"structured code found."
                ),
            )

        elif not has_text_support and has_structured:
            # STRUCTURED ONLY: code exists but note is silent
            return EvidenceRecord(
                target_item_id=target_item_id,
                target_item_name=target_item_name,
                supporting_mentions=[],
                supporting_structured=structured_evidence,
                negative_mentions=negated,
                final_status="supported",
                confidence=self.code_without_text_confidence,
                reason_summary=(
                    f"Structured code supports {target_item_name} but no "
                    f"text mention found."
                ),
            )

        elif uncertain and not has_structured:
            # UNCERTAIN TEXT ONLY
            return EvidenceRecord(
                target_item_id=target_item_id,
                target_item_name=target_item_name,
                supporting_mentions=uncertain,
                supporting_structured=[],
                negative_mentions=negated,
                final_status="conflicted",
                confidence=self.uncertain_mention_confidence,
                conflict_flags=["uncertain_mention_only"],
                reason_summary=(
                    f"Only uncertain text evidence for {target_item_name}."
                ),
            )

        else:
            # NO EVIDENCE
            return EvidenceRecord(
                target_item_id=target_item_id,
                target_item_name=target_item_name,
                final_status="unsupported",
                confidence=0.0,
                reason_summary=f"No evidence found for {target_item_name}.",
            )
