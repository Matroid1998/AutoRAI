"""
Evidence fusion: merges evidence from the unstructured and structured paths.

This module groups all extracted evidence by target item, detects
agreements and conflicts between the two paths, applies the adapter's
fusion policy, and produces the final EvidencePackage.
"""

from __future__ import annotations

import logging
from collections import defaultdict

from ie.core.interfaces import FusionPolicy
from ie.core.models import (
    EvidencePackage,
    EvidenceRecord,
    ExtractionSchema,
    Mention,
    StructuredEvidence,
)

logger = logging.getLogger(__name__)


class EvidenceFusion:
    """
    Fuses evidence from the unstructured and structured paths
    into a unified EvidencePackage.
    """

    def fuse(
        self,
        mentions: list[Mention],
        structured_evidence: list[StructuredEvidence],
        schema: ExtractionSchema,
        fusion_policy: FusionPolicy,
        episode_id: str = "",
        patient_id: str = "",
    ) -> EvidencePackage:
        """
        Merge all evidence by target item and produce the final package.

        Args:
            mentions: Extracted mentions from the unstructured pipeline.
            structured_evidence: Extracted evidence from the structured pipeline.
            schema: The extraction schema (provides target item inventory).
            fusion_policy: Adapter-provided policy for conflict resolution.
            episode_id: The episode identifier (e.g., hadm_id).
            patient_id: The patient identifier (e.g., subject_id).

        Returns:
            An EvidencePackage containing one EvidenceRecord per target item.
        """
        # Group mentions by target item
        mention_groups = self._group_mentions_by_item(mentions, schema)

        # Group structured evidence by target item
        structured_groups = self._group_structured_by_item(structured_evidence)

        # For every target item in the schema, produce an EvidenceRecord
        records: list[EvidenceRecord] = []
        for item in schema.target_items:
            item_id = item.item_id
            item_mentions = mention_groups.get(item_id, [])
            item_structured = structured_groups.get(item_id, [])

            record = fusion_policy.fuse_item_evidence(
                target_item_id=item_id,
                target_item_name=item.name,
                text_mentions=item_mentions,
                structured_evidence=item_structured,
            )
            records.append(record)

        # Log summary
        supported = sum(1 for r in records if r.final_status == "supported")
        logger.info(
            f"Fusion complete. {len(records)} items evaluated: "
            f"{supported} supported, "
            f"{len(records) - supported} not supported."
        )

        from datetime import datetime

        return EvidencePackage(
            episode_id=episode_id,
            patient_id=patient_id,
            section_name=schema.task_name,
            schema_task_name=schema.task_name,
            records=records,
            extraction_timestamp=datetime.now().isoformat(),
        )

    def _group_mentions_by_item(
        self,
        mentions: list[Mention],
        schema: ExtractionSchema,
    ) -> dict[str, list[Mention]]:
        """
        Group mentions by their target item candidates.

        A single mention can map to multiple target items.
        Mentions without target_item_candidates are assigned
        via keyword matching against the schema's target items.
        """
        groups: dict[str, list[Mention]] = defaultdict(list)

        for mention in mentions:
            assigned_items = mention.target_item_candidates

            # If no candidates assigned by LLM, try keyword matching
            if not assigned_items:
                assigned_items = self._keyword_match(mention, schema)

            for item_id in assigned_items:
                groups[item_id].append(mention)

        return dict(groups)

    def _group_structured_by_item(
        self,
        structured_evidence: list[StructuredEvidence],
    ) -> dict[str, list[StructuredEvidence]]:
        """Group structured evidence by their mapped target items."""
        groups: dict[str, list[StructuredEvidence]] = defaultdict(list)

        for evidence in structured_evidence:
            for item_id in evidence.mapped_target_items:
                groups[item_id].append(evidence)

        return dict(groups)

    def _keyword_match(
        self,
        mention: Mention,
        schema: ExtractionSchema,
    ) -> list[str]:
        """
        Fallback: match a mention to target items via keyword overlap.

        Used when the LLM didn't assign target_item_candidates.
        """
        mention_text_lower = mention.text.lower()
        matched = []

        for item in schema.target_items:
            # Check keywords
            for keyword in item.keywords:
                if keyword.lower() in mention_text_lower:
                    matched.append(item.item_id)
                    break

            # Check item name
            if item.item_id not in matched:
                if item.name.lower() in mention_text_lower or mention_text_lower in item.name.lower():
                    matched.append(item.item_id)

        return matched
