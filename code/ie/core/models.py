"""
Canonical data structures for the IE pipeline.

These dataclasses form the shared vocabulary across all IE modules.
Layer 1 (core) and Layer 2 (adapters) both use these structures to
communicate. They are domain-agnostic — no medical/clinical knowledge
is encoded here.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema definition objects (control what the IE pipeline does)
# ---------------------------------------------------------------------------

@dataclass
class EntityType:
    """Defines one type of entity to extract from unstructured text."""
    name: str                    # e.g., "disease", "medication", "procedure"
    description: str             # natural-language description for LLM prompt
    examples: list[str] = field(default_factory=list)  # example mentions

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> EntityType:
        return cls(**data)


@dataclass
class AttributeDimension:
    """Defines one classification axis applied to each extracted mention."""
    name: str                    # e.g., "assertion", "temporality"
    labels: list[str]            # e.g., ["affirmed", "negated", "uncertain"]
    description: str = ""        # instructions for the LLM classifier
    default_label: str = ""      # fallback when classification is uncertain

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> AttributeDimension:
        return cls(**data)


@dataclass
class TargetItem:
    """One target item on the form to map evidence to."""
    item_id: str                 # e.g., "I0200", "N0415A1", "O0110H1"
    name: str                    # e.g., "Anemia", "Antipsychotic: Has received"
    description: str = ""        # what this item represents
    code_patterns: list[str] = field(default_factory=list)  # regex for structured code matching
    keywords: list[str] = field(default_factory=list)       # keywords for text-based matching

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> TargetItem:
        return cls(**data)


@dataclass
class StructuredSourceConfig:
    """Configuration for one structured data source."""
    source_name: str             # e.g., "diagnoses_icd", "prescriptions"
    table_path: str = ""         # relative path to the CSV/table
    code_column: str = ""        # column containing the code to map
    code_system: str = ""        # e.g., "ICD10", "ICD9", "NDC", "drug_name"
    date_column: str = ""        # column for temporal filtering
    date_end_column: str = ""    # optional end-date column
    additional_columns: list[str] = field(default_factory=list)  # extra columns to retain
    filters: dict[str, Any] = field(default_factory=dict)  # static filters to apply

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> StructuredSourceConfig:
        return cls(**data)


@dataclass
class LookbackConfig:
    """Defines the temporal window for evidence relevance."""
    type: str = "admission_window"  # "admission_window", "fixed_days", "open"
    days: int | None = None         # for "fixed_days" type

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> LookbackConfig:
        return cls(**data)


@dataclass
class FusionPolicyConfig:
    """Configuration for how to resolve conflicts between paths."""
    text_negation_overrides_code: bool = True
    code_without_text_confidence: float = 0.5
    text_without_code_confidence: float = 0.6
    agreement_confidence_boost: float = 0.9
    uncertain_mention_confidence: float = 0.3

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> FusionPolicyConfig:
        return cls(**data)


@dataclass
class ExtractionSchema:
    """
    The full schema that controls the IE pipeline.

    This is the single control document: everything the IE pipeline does
    is driven by what this schema says. Different adapters produce
    different schemas; the core engine is the same.
    """
    task_name: str                                # e.g., "MDS Section I"
    target_domain: str                            # e.g., "clinical"
    entity_types: list[EntityType]                # what to extract
    attributes: list[AttributeDimension]          # how to classify each mention
    target_items: list[TargetItem]                # form items to map evidence to
    structured_sources: list[StructuredSourceConfig] = field(default_factory=list)
    lookback: LookbackConfig = field(default_factory=LookbackConfig)
    fusion_policy: FusionPolicyConfig = field(default_factory=FusionPolicyConfig)
    extraction_prompt_template: str = ""          # path or inline prompt template

    def to_dict(self) -> dict:
        return {
            "task_name": self.task_name,
            "target_domain": self.target_domain,
            "entity_types": [et.to_dict() for et in self.entity_types],
            "attributes": [a.to_dict() for a in self.attributes],
            "target_items": [ti.to_dict() for ti in self.target_items],
            "structured_sources": [s.to_dict() for s in self.structured_sources],
            "lookback": self.lookback.to_dict(),
            "fusion_policy": self.fusion_policy.to_dict(),
            "extraction_prompt_template": self.extraction_prompt_template,
        }

    @classmethod
    def from_dict(cls, data: dict) -> ExtractionSchema:
        return cls(
            task_name=data["task_name"],
            target_domain=data["target_domain"],
            entity_types=[EntityType.from_dict(et) for et in data["entity_types"]],
            attributes=[AttributeDimension.from_dict(a) for a in data["attributes"]],
            target_items=[TargetItem.from_dict(ti) for ti in data["target_items"]],
            structured_sources=[StructuredSourceConfig.from_dict(s) for s in data.get("structured_sources", [])],
            lookback=LookbackConfig.from_dict(data.get("lookback", {})),
            fusion_policy=FusionPolicyConfig.from_dict(data.get("fusion_policy", {})),
            extraction_prompt_template=data.get("extraction_prompt_template", ""),
        )

    def get_target_item(self, item_id: str) -> TargetItem | None:
        """Look up a target item by ID."""
        for item in self.target_items:
            if item.item_id == item_id:
                return item
        return None


# ---------------------------------------------------------------------------
# Document representation (input to unstructured pipeline)
# ---------------------------------------------------------------------------

@dataclass
class DocumentSection:
    """A section within a document (e.g., 'Past Medical History')."""
    name: str
    text: str
    start_char: int = 0
    end_char: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> DocumentSection:
        return cls(**data)


@dataclass
class Document:
    """One unstructured text document with metadata."""
    document_id: str             # e.g., note_id
    episode_id: str              # e.g., hadm_id
    patient_id: str              # e.g., subject_id
    text: str                    # full text content
    document_type: str = ""      # e.g., "discharge_summary"
    timestamp: str = ""          # charttime or storetime
    sections: list[DocumentSection] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "document_id": self.document_id,
            "episode_id": self.episode_id,
            "patient_id": self.patient_id,
            "text": self.text,
            "document_type": self.document_type,
            "timestamp": self.timestamp,
            "sections": [s.to_dict() for s in self.sections],
        }

    @classmethod
    def from_dict(cls, data: dict) -> Document:
        return cls(
            document_id=data["document_id"],
            episode_id=data["episode_id"],
            patient_id=data["patient_id"],
            text=data["text"],
            document_type=data.get("document_type", ""),
            timestamp=data.get("timestamp", ""),
            sections=[DocumentSection.from_dict(s) for s in data.get("sections", [])],
        )


# ---------------------------------------------------------------------------
# Extracted evidence objects (intermediate outputs)
# ---------------------------------------------------------------------------

@dataclass
class Mention:
    """
    One extracted span from unstructured text.

    Produced by the unstructured pipeline after LLM extraction
    and attribute classification.
    """
    mention_id: str
    text: str                               # the extracted mention text
    entity_type: str                        # matches an EntityType.name
    source_document_id: str
    source_section: str = ""                # section name within the document
    context_window: str = ""                # surrounding text for provenance
    attributes: dict[str, str] = field(default_factory=dict)  # e.g., {"assertion": "affirmed", "temporality": "current"}
    target_item_candidates: list[str] = field(default_factory=list)  # candidate item IDs
    confidence: float = 1.0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> Mention:
        return cls(**data)


@dataclass
class StructuredEvidence:
    """
    One coded record from a structured table after mapping and filtering.

    Produced by the structured pipeline.
    """
    evidence_id: str
    source_table: str                       # e.g., "diagnoses_icd"
    raw_code: str                           # e.g., "I50.9" or "furosemide"
    code_system: str                        # e.g., "ICD10", "drug_name"
    mapped_target_items: list[str]          # target item IDs this maps to
    episode_id: str = ""
    timestamp: str = ""
    active_status: str = "unknown"          # "active", "likely_active", "historical", "unknown"
    confidence: float = 0.7
    metadata: dict[str, Any] = field(default_factory=dict)  # extra fields (seq_num, route, etc.)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> StructuredEvidence:
        return cls(**data)


# ---------------------------------------------------------------------------
# Fused evidence objects (IE output)
# ---------------------------------------------------------------------------

@dataclass
class EvidenceRecord:
    """
    Fused evidence for one target item.

    This is the primary output unit of the IE component. It combines
    evidence from both unstructured and structured paths, with conflict
    resolution and confidence scoring already applied.
    """
    target_item_id: str
    target_item_name: str
    supporting_mentions: list[Mention] = field(default_factory=list)
    supporting_structured: list[StructuredEvidence] = field(default_factory=list)
    negative_mentions: list[Mention] = field(default_factory=list)
    final_status: str = "unsupported"       # "supported", "negated", "conflicted", "unsupported"
    confidence: float = 0.0
    conflict_flags: list[str] = field(default_factory=list)
    reason_summary: str = ""

    def to_dict(self) -> dict:
        return {
            "target_item_id": self.target_item_id,
            "target_item_name": self.target_item_name,
            "supporting_mentions": [m.to_dict() for m in self.supporting_mentions],
            "supporting_structured": [s.to_dict() for s in self.supporting_structured],
            "negative_mentions": [m.to_dict() for m in self.negative_mentions],
            "final_status": self.final_status,
            "confidence": self.confidence,
            "conflict_flags": self.conflict_flags,
            "reason_summary": self.reason_summary,
        }

    @classmethod
    def from_dict(cls, data: dict) -> EvidenceRecord:
        return cls(
            target_item_id=data["target_item_id"],
            target_item_name=data["target_item_name"],
            supporting_mentions=[Mention.from_dict(m) for m in data.get("supporting_mentions", [])],
            supporting_structured=[StructuredEvidence.from_dict(s) for s in data.get("supporting_structured", [])],
            negative_mentions=[Mention.from_dict(m) for m in data.get("negative_mentions", [])],
            final_status=data.get("final_status", "unsupported"),
            confidence=data.get("confidence", 0.0),
            conflict_flags=data.get("conflict_flags", []),
            reason_summary=data.get("reason_summary", ""),
        )


@dataclass
class EvidencePackage:
    """
    Collection of all EvidenceRecords for one episode + section.

    This is the top-level IE output consumed by downstream components
    (RAG, guideline-grounded prediction, verifier).
    """
    episode_id: str
    patient_id: str
    section_name: str                       # e.g., "Section I", "Section N"
    schema_task_name: str                   # e.g., "MDS Section I"
    records: list[EvidenceRecord] = field(default_factory=list)
    extraction_timestamp: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "episode_id": self.episode_id,
            "patient_id": self.patient_id,
            "section_name": self.section_name,
            "schema_task_name": self.schema_task_name,
            "records": [r.to_dict() for r in self.records],
            "extraction_timestamp": self.extraction_timestamp,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict) -> EvidencePackage:
        return cls(
            episode_id=data["episode_id"],
            patient_id=data["patient_id"],
            section_name=data["section_name"],
            schema_task_name=data["schema_task_name"],
            records=[EvidenceRecord.from_dict(r) for r in data.get("records", [])],
            extraction_timestamp=data.get("extraction_timestamp", ""),
            metadata=data.get("metadata", {}),
        )

    def to_json(self, path: str | Path) -> None:
        """Serialize the evidence package to a JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2, default=str)
        logger.info(f"Evidence package saved to {path}")

    @classmethod
    def from_json(cls, path: str | Path) -> EvidencePackage:
        """Deserialize an evidence package from a JSON file."""
        with open(path) as f:
            data = json.load(f)
        return cls.from_dict(data)

    def get_record(self, item_id: str) -> EvidenceRecord | None:
        """Look up an evidence record by target item ID."""
        for record in self.records:
            if record.target_item_id == item_id:
                return record
        return None

    def summary(self) -> dict:
        """Return a concise summary of the evidence package."""
        supported = [r for r in self.records if r.final_status == "supported"]
        negated = [r for r in self.records if r.final_status == "negated"]
        conflicted = [r for r in self.records if r.final_status == "conflicted"]
        unsupported = [r for r in self.records if r.final_status == "unsupported"]
        return {
            "episode_id": self.episode_id,
            "section": self.section_name,
            "total_items": len(self.records),
            "supported": len(supported),
            "negated": len(negated),
            "conflicted": len(conflicted),
            "unsupported": len(unsupported),
            "supported_items": [r.target_item_id for r in supported],
        }
