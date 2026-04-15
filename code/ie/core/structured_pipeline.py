"""
Schema-driven extraction from structured coded tables.

This module is domain-agnostic. It reads structured data sources
defined in the schema, maps codes to target items using the adapter's
code mapper, filters by temporal window, and produces StructuredEvidence
objects. No LLM calls — pure data processing.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

import pandas as pd

from ie.core.interfaces import StructuredCodeMapper, TemporalResolver
from ie.core.models import (
    ExtractionSchema,
    StructuredEvidence,
    StructuredSourceConfig,
)

logger = logging.getLogger(__name__)


class StructuredPipeline:
    """
    Extracts and maps structured coded records to target form items.

    The pipeline is generic: it reads whatever sources the schema defines,
    maps codes using whatever mapper the adapter provides, and filters
    by whatever temporal window the schema specifies.
    """

    def extract(
        self,
        data_frames: dict[str, pd.DataFrame],
        schema: ExtractionSchema,
        code_mapper: StructuredCodeMapper,
        temporal_resolver: TemporalResolver,
        reference_time: str = "",
    ) -> list[StructuredEvidence]:
        """
        Run structured extraction on all configured data sources.

        Args:
            data_frames: Dict mapping source_name → DataFrame (loaded by adapter).
            schema: The extraction schema defining which sources to process.
            code_mapper: Adapter-provided mapper from codes to target items.
            temporal_resolver: Adapter-provided temporal filter.
            reference_time: Reference timestamp for lookback filtering
                            (e.g., discharge time).

        Returns:
            List of StructuredEvidence objects mapped to target items.
        """
        all_evidence: list[StructuredEvidence] = []

        for source_config in schema.structured_sources:
            source_name = source_config.source_name

            if source_name not in data_frames:
                logger.warning(
                    f"Structured source '{source_name}' not found in "
                    f"provided data frames. Skipping."
                )
                continue

            df = data_frames[source_name]
            if df.empty:
                logger.info(f"Source '{source_name}' has no data. Skipping.")
                continue

            try:
                evidence = self._process_source(
                    df=df,
                    source_config=source_config,
                    code_mapper=code_mapper,
                    temporal_resolver=temporal_resolver,
                    reference_time=reference_time,
                    lookback=schema.lookback,
                )
                all_evidence.extend(evidence)
                logger.info(
                    f"Extracted {len(evidence)} evidence records "
                    f"from source '{source_name}'"
                )
            except Exception as e:
                logger.warning(
                    f"Processing failed for source '{source_name}': {e}"
                )

        logger.info(
            f"Total structured evidence: {len(all_evidence)} "
            f"from {len(schema.structured_sources)} sources"
        )
        return all_evidence

    def _process_source(
        self,
        df: pd.DataFrame,
        source_config: StructuredSourceConfig,
        code_mapper: StructuredCodeMapper,
        temporal_resolver: TemporalResolver,
        reference_time: str,
        lookback: Any,
    ) -> list[StructuredEvidence]:
        """
        Process one structured data source.

        Steps:
        1. Validate that required columns exist
        2. Iterate rows
        3. Map each code to target items
        4. Filter by temporal window
        5. Produce StructuredEvidence objects
        """
        code_col = source_config.code_column
        code_system = source_config.code_system
        date_col = source_config.date_column

        # Validate columns
        if code_col and code_col not in df.columns:
            logger.warning(
                f"Code column '{code_col}' not found in source "
                f"'{source_config.source_name}'. Available: {list(df.columns)}"
            )
            return []

        evidence_list: list[StructuredEvidence] = []

        for idx, row in df.iterrows():
            # Get the code value
            raw_code = str(row.get(code_col, "")).strip() if code_col else ""
            if not raw_code:
                continue

            # Map code to target items
            mapped_items = code_mapper.map_code_to_target_items(
                code=raw_code, code_system=code_system
            )
            if not mapped_items:
                continue  # no mapping found, skip

            # Temporal filtering
            timestamp = ""
            if date_col and date_col in df.columns:
                timestamp = str(row.get(date_col, ""))

            if reference_time and timestamp and lookback:
                in_window = temporal_resolver.is_within_lookback(
                    timestamp=timestamp,
                    reference_time=reference_time,
                    lookback_type=lookback.type,
                    lookback_days=lookback.days,
                )
                if not in_window:
                    continue  # outside lookback window, skip

            # Build metadata from additional columns
            metadata: dict[str, Any] = {}
            for col in source_config.additional_columns:
                if col in df.columns:
                    val = row.get(col)
                    # Convert numpy types to Python types for JSON serialization
                    if pd.notna(val):
                        metadata[col] = _to_python_type(val)

            # Create evidence record
            evidence = StructuredEvidence(
                evidence_id=str(uuid.uuid4())[:8],
                source_table=source_config.source_name,
                raw_code=raw_code,
                code_system=code_system,
                mapped_target_items=mapped_items,
                episode_id="",  # filled by orchestrator
                timestamp=timestamp,
                active_status="likely_active",  # default for v1
                confidence=0.7,  # default structured confidence
                metadata=metadata,
            )
            evidence_list.append(evidence)

        return evidence_list


def _to_python_type(val: Any) -> Any:
    """Convert numpy/pandas types to native Python types."""
    import numpy as np

    if isinstance(val, (np.integer,)):
        return int(val)
    elif isinstance(val, (np.floating,)):
        return float(val)
    elif isinstance(val, (np.bool_,)):
        return bool(val)
    elif isinstance(val, pd.Timestamp):
        return str(val)
    else:
        return val
