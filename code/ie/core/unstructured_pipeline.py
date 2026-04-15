"""
Schema-driven extraction from unstructured text via LLM.

This module is domain-agnostic. It builds an LLM prompt dynamically
from the extraction schema's entity types and attribute dimensions,
sends the text to the LLM, and parses the structured JSON response
into Mention objects.

The prompt template can be provided by the adapter (for domain-specific
prompting) or generated automatically from the schema.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from ie.core.llm_client import LLMClient
from ie.core.models import (
    Document,
    ExtractionSchema,
    Mention,
)

logger = logging.getLogger(__name__)


class UnstructuredPipeline:
    """
    Extracts entities and classifies attributes from unstructured text
    using an LLM, driven by the extraction schema.
    """

    def __init__(self, llm_client: LLMClient):
        self.llm_client = llm_client

    def extract(
        self,
        documents: list[Document],
        schema: ExtractionSchema,
    ) -> list[Mention]:
        """
        Run schema-driven extraction on all documents.

        Args:
            documents: List of Document objects to process.
            schema: The extraction schema defining what to extract.

        Returns:
            List of Mention objects extracted from all documents.
        """
        all_mentions: list[Mention] = []

        for doc in documents:
            try:
                mentions = self._extract_from_document(doc, schema)
                all_mentions.extend(mentions)
                logger.info(
                    f"Extracted {len(mentions)} mentions from document {doc.document_id}"
                )
            except Exception as e:
                logger.warning(
                    f"Extraction failed for document {doc.document_id}: {e}"
                )
                # Don't crash — continue with other documents

        logger.info(
            f"Total mentions extracted: {len(all_mentions)} "
            f"from {len(documents)} documents"
        )
        return all_mentions

    def _extract_from_document(
        self,
        document: Document,
        schema: ExtractionSchema,
    ) -> list[Mention]:
        """
        Extract mentions from a single document.

        If the document has sections, processes each section separately
        for better context. Otherwise processes the full text.
        """
        if document.sections:
            mentions = []
            for section in document.sections:
                section_mentions = self._extract_from_text(
                    text=section.text,
                    schema=schema,
                    document_id=document.document_id,
                    section_name=section.name,
                )
                mentions.extend(section_mentions)
            return mentions
        else:
            return self._extract_from_text(
                text=document.text,
                schema=schema,
                document_id=document.document_id,
                section_name="",
            )

    def _extract_from_text(
        self,
        text: str,
        schema: ExtractionSchema,
        document_id: str,
        section_name: str,
    ) -> list[Mention]:
        """
        Run LLM extraction on a single text chunk.

        Builds the prompt from the schema's entity types and attribute
        dimensions, sends it to the LLM, and parses the response.
        """
        if not text.strip():
            return []

        prompt = self._build_extraction_prompt(schema, text, section_name)
        system_prompt = self._build_system_prompt(schema)

        try:
            response = self.llm_client.generate_json(
                prompt=prompt,
                system_prompt=system_prompt,
            )
            return self._parse_llm_response(
                response=response,
                schema=schema,
                document_id=document_id,
                section_name=section_name,
            )
        except Exception as e:
            logger.warning(f"LLM extraction failed for section '{section_name}': {e}")
            return []

    def _build_system_prompt(self, schema: ExtractionSchema) -> str:
        """Build the system prompt for the LLM."""
        return (
            "You are a precise information extraction system. "
            "You extract structured entities from text according to a given schema. "
            "You always respond with valid JSON. "
            "You never invent information that is not in the text. "
            "You capture assertion status (affirmed/negated/uncertain) and "
            "temporal status faithfully from the text."
        )

    def _build_extraction_prompt(
        self,
        schema: ExtractionSchema,
        text: str,
        section_name: str,
    ) -> str:
        """
        Build the extraction prompt dynamically from the schema.

        If the schema provides a custom prompt template, use it.
        Otherwise, generate a standard prompt from the schema components.
        """
        # If a custom prompt template is provided, use it
        if schema.extraction_prompt_template:
            return self._fill_prompt_template(
                schema.extraction_prompt_template, schema, text, section_name
            )

        # Otherwise, build a standard prompt
        return self._build_default_prompt(schema, text, section_name)

    def _fill_prompt_template(
        self,
        template: str,
        schema: ExtractionSchema,
        text: str,
        section_name: str,
    ) -> str:
        """Fill placeholders in a custom prompt template."""
        entity_types_desc = self._format_entity_types(schema)
        attributes_desc = self._format_attributes(schema)
        target_items_desc = self._format_target_items(schema)

        return template.format(
            entity_types=entity_types_desc,
            attribute_dimensions=attributes_desc,
            target_items=target_items_desc,
            text=text,
            section_name=section_name,
            task_name=schema.task_name,
        )

    def _build_default_prompt(
        self,
        schema: ExtractionSchema,
        text: str,
        section_name: str,
    ) -> str:
        """Build a standard extraction prompt from schema components."""
        entity_types_desc = self._format_entity_types(schema)
        attributes_desc = self._format_attributes(schema)
        target_items_desc = self._format_target_items(schema)

        section_context = ""
        if section_name:
            section_context = f"\nThis text is from the '{section_name}' section of the document.\n"

        prompt = f"""## Task: {schema.task_name}

Extract all relevant entities from the following clinical text and classify each one according to the schema below.

## Entity Types to Extract
{entity_types_desc}

## Attribute Dimensions
For each extracted entity, classify it on these dimensions:
{attributes_desc}

## Target Items
These are the specific form items we are trying to fill. For each extracted entity, suggest which target item(s) it might map to:
{target_items_desc}
{section_context}
## Text to Process
```
{text}
```

## Output Format
Respond with a JSON object containing a key "mentions" with a list of extracted entities.
Each entity should have:
- "text": the exact text span of the mention
- "entity_type": which entity type this is
- "attributes": a dict mapping each attribute dimension name to its label
- "target_item_candidates": list of target item IDs this might map to (can be empty if unsure)
- "context": a brief surrounding context (1-2 sentences) for provenance
- "confidence": a float 0-1 indicating extraction confidence

Example:
{{
  "mentions": [
    {{
      "text": "heart failure",
      "entity_type": "disease",
      "attributes": {{"assertion": "affirmed", "temporality": "current"}},
      "target_item_candidates": ["I0600"],
      "context": "Patient was admitted for acute decompensated heart failure",
      "confidence": 0.95
    }}
  ]
}}

If no relevant entities are found, return {{"mentions": []}}.
"""
        return prompt

    def _format_entity_types(self, schema: ExtractionSchema) -> str:
        """Format entity types for the prompt."""
        lines = []
        for et in schema.entity_types:
            line = f"- **{et.name}**: {et.description}"
            if et.examples:
                line += f"\n  Examples: {', '.join(et.examples)}"
            lines.append(line)
        return "\n".join(lines)

    def _format_attributes(self, schema: ExtractionSchema) -> str:
        """Format attribute dimensions for the prompt."""
        lines = []
        for attr in schema.attributes:
            labels_str = ", ".join(f'"{l}"' for l in attr.labels)
            line = f"- **{attr.name}**: [{labels_str}]"
            if attr.description:
                line += f"\n  {attr.description}"
            lines.append(line)
        return "\n".join(lines)

    def _format_target_items(self, schema: ExtractionSchema) -> str:
        """Format target items for the prompt."""
        lines = []
        for item in schema.target_items:
            line = f"- **{item.item_id}** — {item.name}"
            if item.description:
                line += f": {item.description}"
            if item.keywords:
                line += f"\n  Keywords: {', '.join(item.keywords[:10])}"
            lines.append(line)
        return "\n".join(lines)

    def _parse_llm_response(
        self,
        response: dict | list,
        schema: ExtractionSchema,
        document_id: str,
        section_name: str,
    ) -> list[Mention]:
        """
        Parse the LLM's JSON response into Mention objects.

        Handles various response formats gracefully.
        """
        # Handle response format
        if isinstance(response, dict):
            raw_mentions = response.get("mentions", [])
        elif isinstance(response, list):
            raw_mentions = response
        else:
            logger.warning(f"Unexpected response type: {type(response)}")
            return []

        mentions = []
        valid_entity_types = {et.name for et in schema.entity_types}
        valid_attributes = {a.name: set(a.labels) for a in schema.attributes}
        valid_item_ids = {item.item_id for item in schema.target_items}

        for raw in raw_mentions:
            try:
                mention = self._parse_single_mention(
                    raw=raw,
                    document_id=document_id,
                    section_name=section_name,
                    valid_entity_types=valid_entity_types,
                    valid_attributes=valid_attributes,
                    valid_item_ids=valid_item_ids,
                )
                if mention is not None:
                    mentions.append(mention)
            except Exception as e:
                logger.warning(f"Failed to parse mention: {raw}. Error: {e}")

        return mentions

    def _parse_single_mention(
        self,
        raw: dict,
        document_id: str,
        section_name: str,
        valid_entity_types: set[str],
        valid_attributes: dict[str, set[str]],
        valid_item_ids: set[str],
    ) -> Mention | None:
        """Parse and validate a single mention from the LLM response."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        entity_type = raw.get("entity_type", "")
        if entity_type not in valid_entity_types:
            logger.debug(f"Unknown entity type '{entity_type}', skipping mention: {text}")
            return None

        # Validate and clean attributes
        raw_attrs = raw.get("attributes", {})
        cleaned_attrs = {}
        for attr_name, attr_labels in valid_attributes.items():
            value = raw_attrs.get(attr_name, "")
            if value in attr_labels:
                cleaned_attrs[attr_name] = value
            else:
                logger.debug(
                    f"Invalid attribute value '{value}' for '{attr_name}', "
                    f"expected one of {attr_labels}"
                )
                # Use a reasonable default
                cleaned_attrs[attr_name] = ""

        # Validate target item candidates
        raw_candidates = raw.get("target_item_candidates", [])
        cleaned_candidates = [c for c in raw_candidates if c in valid_item_ids]

        return Mention(
            mention_id=str(uuid.uuid4())[:8],
            text=text,
            entity_type=entity_type,
            source_document_id=document_id,
            source_section=section_name,
            context_window=raw.get("context", ""),
            attributes=cleaned_attrs,
            target_item_candidates=cleaned_candidates,
            confidence=float(raw.get("confidence", 1.0)),
        )
