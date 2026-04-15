"""
Abstract base classes that Layer 2 adapters must implement.

Layer 1 (core) depends only on these interfaces, never on concrete
adapter implementations. This ensures the core engine remains
domain-agnostic and reusable.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from ie.core.models import (
    Document,
    ExtractionSchema,
    EvidenceRecord,
    Mention,
    StructuredEvidence,
)


class StructuredCodeMapper(ABC):
    """
    Maps coded records from structured tables to target form items.

    Example: ICD-10 code I50.9 → MDS Section I heart failure item.
    The mapping logic is domain-specific and lives in the adapter.
    """

    @abstractmethod
    def map_code_to_target_items(
        self, code: str, code_system: str
    ) -> list[str]:
        """
        Given a code and its coding system, return the list of target
        item IDs it maps to. Returns empty list if no mapping found.
        """
        ...


class TemporalResolver(ABC):
    """
    Interprets domain-specific temporal context.

    Example: determines whether a mention found in "Past Medical History"
    should be classified as historical, or whether a procedure timestamp
    falls within the look-back window.
    """

    @abstractmethod
    def is_within_lookback(
        self, timestamp: str, reference_time: str, lookback_type: str, lookback_days: int | None = None
    ) -> bool:
        """
        Check whether `timestamp` falls within the relevant look-back
        window relative to `reference_time`.
        """
        ...


class FusionPolicy(ABC):
    """
    Defines how conflicts between unstructured and structured evidence
    are resolved.

    Example: if the note negates pneumonia but an ICD code for pneumonia
    exists, the fusion policy decides which to trust.
    """

    @abstractmethod
    def fuse_item_evidence(
        self,
        target_item_id: str,
        target_item_name: str,
        text_mentions: list[Mention],
        structured_evidence: list[StructuredEvidence],
    ) -> EvidenceRecord:
        """
        Given all evidence (text + structured) for a single target item,
        produce a fused EvidenceRecord with final status, confidence,
        and conflict flags.
        """
        ...


class IEAdapter(ABC):
    """
    The main adapter interface.

    Each adapter provides the extraction schema, domain-specific
    components (code mapper, temporal resolver, fusion policy),
    and data loading methods for a specific task + dataset.
    """

    @abstractmethod
    def get_extraction_schema(self) -> ExtractionSchema:
        """Return the full extraction schema for the target task."""
        ...

    @abstractmethod
    def get_structured_code_mapper(self) -> StructuredCodeMapper:
        """Return the code mapper for structured data."""
        ...

    @abstractmethod
    def get_temporal_resolver(self) -> TemporalResolver:
        """Return the temporal resolver for the domain."""
        ...

    @abstractmethod
    def get_fusion_policy(self) -> FusionPolicy:
        """Return the fusion policy for the domain."""
        ...

    @abstractmethod
    def load_structured_data(
        self, episode_id: str
    ) -> dict[str, pd.DataFrame]:
        """
        Load structured data tables for the given episode.
        Returns a dict mapping source_name → DataFrame.
        """
        ...

    @abstractmethod
    def load_unstructured_data(
        self, episode_id: str
    ) -> list[Document]:
        """
        Load unstructured text documents for the given episode.
        Returns a list of Document objects.
        """
        ...

    def get_episode_metadata(self, episode_id: str) -> dict[str, Any]:
        """
        Optional: return admission metadata (admittime, dischtime, etc.)
        for temporal filtering. Default returns empty dict.
        """
        return {}
