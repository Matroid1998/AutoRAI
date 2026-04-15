"""
End-to-end IE orchestrator.

Coordinates the full IE flow: load schema → load data → run unstructured
pipeline → run structured pipeline → fuse → return EvidencePackage.

This is the single entry point that downstream components call.
"""

from __future__ import annotations

import logging

from ie.core.fusion import EvidenceFusion
from ie.core.interfaces import IEAdapter
from ie.core.llm_client import LLMClient
from ie.core.models import EvidencePackage
from ie.core.structured_pipeline import StructuredPipeline
from ie.core.unstructured_pipeline import UnstructuredPipeline

logger = logging.getLogger(__name__)


class IEOrchestrator:
    """
    Orchestrates the entire Information Extraction pipeline.

    The orchestrator is domain-agnostic — all domain specifics come
    from the adapter. The orchestrator just runs the sequence:
    schema → data → extract → fuse → output.
    """

    def __init__(self, adapter: IEAdapter, llm_client: LLMClient):
        """
        Initialize the orchestrator.

        Args:
            adapter: A concrete IEAdapter providing schema, mappers,
                     and data loading for the target task.
            llm_client: LLM client for the unstructured pipeline.
        """
        self.adapter = adapter
        self.llm_client = llm_client
        self.unstructured_pipeline = UnstructuredPipeline(llm_client)
        self.structured_pipeline = StructuredPipeline()
        self.fusion = EvidenceFusion()

    def run(self, episode_id: str) -> EvidencePackage:
        """
        Run the full IE pipeline for a single episode.

        Steps:
        1. Load extraction schema from the adapter
        2. Load unstructured data (documents) from the adapter
        3. Load structured data (DataFrames) from the adapter
        4. Run unstructured pipeline → List[Mention]
        5. Run structured pipeline → List[StructuredEvidence]
        6. Fuse evidence → EvidencePackage

        Args:
            episode_id: The episode identifier (e.g., hadm_id).

        Returns:
            An EvidencePackage containing fused evidence for all
            target items in the schema.
        """
        logger.info(f"=== Starting IE pipeline for episode: {episode_id} ===")

        # Step 1: Load schema
        schema = self.adapter.get_extraction_schema()
        logger.info(
            f"Schema loaded: task='{schema.task_name}', "
            f"target_items={len(schema.target_items)}, "
            f"entity_types={[et.name for et in schema.entity_types]}"
        )

        # Step 2: Load data
        documents = self.adapter.load_unstructured_data(episode_id)
        logger.info(f"Loaded {len(documents)} unstructured documents")

        structured_data = self.adapter.load_structured_data(episode_id)
        logger.info(
            f"Loaded {len(structured_data)} structured sources: "
            f"{list(structured_data.keys())}"
        )

        # Get adapter components
        code_mapper = self.adapter.get_structured_code_mapper()
        temporal_resolver = self.adapter.get_temporal_resolver()
        fusion_policy = self.adapter.get_fusion_policy()

        # Get episode metadata for temporal filtering
        episode_meta = self.adapter.get_episode_metadata(episode_id)
        reference_time = episode_meta.get("dischtime", "")

        # Step 3: Run unstructured pipeline
        logger.info("Running unstructured extraction pipeline...")
        mentions = self.unstructured_pipeline.extract(documents, schema)
        logger.info(f"Unstructured pipeline produced {len(mentions)} mentions")

        # Step 4: Run structured pipeline
        logger.info("Running structured extraction pipeline...")
        structured_evidence = self.structured_pipeline.extract(
            data_frames=structured_data,
            schema=schema,
            code_mapper=code_mapper,
            temporal_resolver=temporal_resolver,
            reference_time=reference_time,
        )
        logger.info(
            f"Structured pipeline produced {len(structured_evidence)} evidence records"
        )

        # Step 5: Fuse evidence
        logger.info("Running evidence fusion...")
        patient_id = episode_meta.get("subject_id", "")
        evidence_package = self.fusion.fuse(
            mentions=mentions,
            structured_evidence=structured_evidence,
            schema=schema,
            fusion_policy=fusion_policy,
            episode_id=episode_id,
            patient_id=patient_id,
        )

        # Log summary
        summary = evidence_package.summary()
        logger.info(
            f"=== IE pipeline complete for episode {episode_id} ===\n"
            f"  Total items: {summary['total_items']}\n"
            f"  Supported: {summary['supported']} ({summary['supported_items']})\n"
            f"  Negated: {summary['negated']}\n"
            f"  Conflicted: {summary['conflicted']}\n"
            f"  Unsupported: {summary['unsupported']}"
        )

        return evidence_package
