# Implementation Plan: IE Component

## Goal

Implement the Information Extraction (IE) component with a 2-layer architecture:
- **Layer 1 (Core)**: Domain-agnostic, schema-driven extraction engine
- **Layer 2 (Adapter)**: MIMIC-IV specific adapter for MDS Sections I, N, O

No normalization layer in v1. The system should extract evidence from both unstructured (discharge notes) and structured (MIMIC tables) paths, then fuse them into a unified evidence package per MDS item.

---

## Phase 1: Create Code Structure Document

Before writing any code, create a `code/code_structure.md` document that describes the full directory layout, module responsibilities, and data flow. This document will serve as a reference for all future LLM calls, ensuring consistency across code written in different sessions.

### [NEW] [code_structure.md](file:///home/mehdi/Projects/AutoRAI/code/code_structure.md)

Contents:
- Directory tree with every file
- Module responsibility summary (1-2 sentences per file)
- Data flow diagram (text-based)
- Key design patterns and conventions (naming, imports, error handling)
- Data model reference (canonical objects used across modules)

---

## Phase 2: Layer 1 — Core Engine

### [NEW] [models.py](file:///home/mehdi/Projects/AutoRAI/code/ie/core/models.py)

Canonical data structures used throughout the IE pipeline:
- `ExtractionSchema` — full schema definition (entity types, attributes, sources, fusion policy, target items)
- `EntityType` — defines what to extract (name, description, examples)
- `AttributeDimension` — defines classification axes (name, labels, description)
- `TargetItem` — one MDS item to map evidence to (item_id, name, description, code_patterns)
- `SourceConfig` — structured source configuration (table, columns, filters)
- `Mention` — single extracted span from unstructured text with attributes
- `StructuredEvidence` — single coded record from structured tables after mapping/filtering
- `EvidenceRecord` — fused evidence for one target item (the IE output unit)
- `EvidencePackage` — collection of all EvidenceRecords for one episode + section

---

### [NEW] [interfaces.py](file:///home/mehdi/Projects/AutoRAI/code/ie/core/interfaces.py)

Abstract base classes that adapters must implement:
- `IEAdapter` (ABC):
  - `get_extraction_schema() -> ExtractionSchema`
  - `get_structured_code_mapper() -> StructuredCodeMapper`
  - `get_temporal_resolver() -> TemporalResolver`
  - `get_fusion_policy() -> FusionPolicy`
  - `load_structured_data(episode_id) -> Dict[str, pd.DataFrame]`
  - `load_unstructured_data(episode_id) -> List[Document]`
- `StructuredCodeMapper` (ABC):
  - `map_code_to_target_items(code, code_system) -> List[str]`
- `TemporalResolver` (ABC):
  - `resolve_temporal_context(mention, section_context) -> str`
  - `is_within_lookback(timestamp, reference_time, lookback_config) -> bool`
- `FusionPolicy` (ABC):
  - `resolve_conflict(text_evidence, structured_evidence) -> EvidenceRecord`
  - `compute_confidence(agreements, conflicts) -> float`

---

### [NEW] [unstructured_pipeline.py](file:///home/mehdi/Projects/AutoRAI/code/ie/core/unstructured_pipeline.py)

Schema-driven extraction from unstructured text:
- `UnstructuredPipeline` class:
  - `extract(documents, schema) -> List[Mention]`
  - Uses LLM to perform section-aware entity extraction + attribute classification in a single prompt
  - The prompt is constructed dynamically from the schema (entity types, attribute dimensions, labels)
  - Returns `Mention` objects with attributes populated
- Internal methods:
  - `_build_extraction_prompt(schema, text)` — constructs the LLM prompt from schema
  - `_parse_llm_response(response, schema)` — parses structured JSON output into `Mention` objects
  - `_segment_document(document)` — splits into sections using headers (generic, adapter can override section list)

---

### [NEW] [structured_pipeline.py](file:///home/mehdi/Projects/AutoRAI/code/ie/core/structured_pipeline.py)

Schema-driven extraction from structured coded tables:
- `StructuredPipeline` class:
  - `extract(data_frames, schema, code_mapper, temporal_resolver, reference_time) -> List[StructuredEvidence]`
  - Reads structured sources defined in schema
  - Maps codes to target items using the adapter's code mapper
  - Filters by lookback period using temporal resolver
  - Returns `StructuredEvidence` objects

---

### [NEW] [fusion.py](file:///home/mehdi/Projects/AutoRAI/code/ie/core/fusion.py)

Merges evidence from both paths:
- `EvidenceFusion` class:
  - `fuse(mentions, structured_evidence, schema, fusion_policy) -> EvidencePackage`
  - Groups all evidence by target item
  - Identifies agreements and conflicts
  - Applies fusion policy to resolve conflicts
  - Computes final confidence scores
  - Produces `EvidenceRecord` per target item

---

### [NEW] [orchestrator.py](file:///home/mehdi/Projects/AutoRAI/code/ie/core/orchestrator.py)

End-to-end IE execution:
- `IEOrchestrator` class:
  - `__init__(adapter: IEAdapter, llm_client)`
  - `run(episode_id) -> EvidencePackage`
  - Loads schema from adapter
  - Loads data from adapter
  - Runs unstructured pipeline
  - Runs structured pipeline
  - Runs fusion
  - Returns evidence package

---

### [NEW] [llm_client.py](file:///home/mehdi/Projects/AutoRAI/code/ie/core/llm_client.py)

Thin wrapper around LLM API calls:
- `LLMClient` class:
  - `__init__(model_name, api_key, base_url)`
  - `generate(prompt, system_prompt, response_format) -> str`
  - Supports OpenAI-compatible APIs (OpenRouter, etc.)
  - Handles retries and rate limiting

---

## Phase 3: Layer 2 — MIMIC Adapter for MDS Sections I, N, O

### [NEW] [base_adapter.py](file:///home/mehdi/Projects/AutoRAI/code/ie/adapters/mds_mimic/base_adapter.py)

Shared MIMIC infrastructure:
- `MIMICBaseAdapter` (implements `IEAdapter` partially):
  - Data path configuration (MIMIC-IV + MIMIC-IV-Note directories)
  - Common table loading utilities (read compressed CSVs, join on hadm_id/subject_id)
  - Discharge note loading + generic section segmentation
  - Shared temporal filter logic (admission window)

---

### [NEW] [section_i_adapter.py](file:///home/mehdi/Projects/AutoRAI/code/ie/adapters/mds_mimic/section_i_adapter.py)

MDS Section I (Active Diagnoses) adapter:
- `SectionIAdapter(MIMICBaseAdapter)`:
  - `get_extraction_schema()` — returns schema for disease extraction with assertion/temporality attributes
  - `get_structured_code_mapper()` — returns ICD-to-Section-I mapper
  - Target items: I0200, I0300, I0400, I0600, I0700, I0900, I2900, I4500, I4900, I6200
  - Structured sources: `diagnoses_icd` + `d_icd_diagnoses`

### [NEW] [section_n_adapter.py](file:///home/mehdi/Projects/AutoRAI/code/ie/adapters/mds_mimic/section_n_adapter.py)

MDS Section N (Medications) adapter:
- `SectionNAdapter(MIMICBaseAdapter)`:
  - Target items: N0415A1 through N0415K1, N0350A, N0350B
  - Structured sources: `emar`, `emar_detail`, `prescriptions`, `pharmacy`
  - Code mapper: drug name → medication class (keyword-based for v1 since no normalization)

### [NEW] [section_o_adapter.py](file:///home/mehdi/Projects/AutoRAI/code/ie/adapters/mds_mimic/section_o_adapter.py)

MDS Section O (Special Treatments/Procedures) adapter:
- `SectionOAdapter(MIMICBaseAdapter)`:
  - Target items: O0110H1-H4, H10, I1, J1-J3, G2, G3
  - Structured sources: `procedures_icd`, `d_icd_procedures`, `hcpcsevents`, `inputevents`, `procedureevents`, `d_items`
  - Code mapper: ICD-PCS + ICU item IDs → Section O items

---

### [NEW] Mapping Files

#### [NEW] [icd_to_section_i.json](file:///home/mehdi/Projects/AutoRAI/code/ie/adapters/mds_mimic/mappings/icd_to_section_i.json)

Maps ICD-10/ICD-9 codes (regex patterns) to Section I target items. Example:
```json
{
  "I0200": {"name": "Anemia", "icd10_patterns": ["D50.*", "D51.*", "D52.*", "D53.*", "D55.*-D64.*"], "icd9_patterns": ["280.*", "281.*", "282.*", "283.*", "284.*", "285.*"]},
  "I0600": {"name": "Heart Failure", "icd10_patterns": ["I50.*", "I11.0", "I13.0", "I13.2"], "icd9_patterns": ["428.*"]}
}
```

#### [NEW] [drug_class_keywords.json](file:///home/mehdi/Projects/AutoRAI/code/ie/adapters/mds_mimic/mappings/drug_class_keywords.json)

Maps medication class items to keyword lists for v1 (no normalization). Example:
```json
{
  "N0415A1": {"name": "Antipsychotic", "keywords": ["haloperidol", "quetiapine", "olanzapine", "risperidone", "aripiprazole", "ziprasidone", "chlorpromazine"]},
  "N0415G1": {"name": "Diuretic", "keywords": ["furosemide", "lasix", "hydrochlorothiazide", "spironolactone", "bumetanide", "metolazone"]}
}
```

#### [NEW] [procedure_to_section_o.json](file:///home/mehdi/Projects/AutoRAI/code/ie/adapters/mds_mimic/mappings/procedure_to_section_o.json)

Maps ICD-PCS codes and ICU itemids to Section O items.

---

### [NEW] Prompt Templates

#### [NEW] [extraction_prompt.txt](file:///home/mehdi/Projects/AutoRAI/code/ie/adapters/mds_mimic/prompts/extraction_prompt.txt)

LLM prompt template for clinical entity extraction + attribute classification from discharge notes. Uses placeholders filled from the schema: `{entity_types}`, `{attribute_dimensions}`, `{text}`.

---

## Phase 4: Entry Point and Configuration

### [NEW] [config.py](file:///home/mehdi/Projects/AutoRAI/code/ie/config.py)

Configuration management:
- Data paths for MIMIC-IV and MIMIC-IV-Note
- LLM configuration (model, API key, endpoint)
- Default settings

### [NEW] [run_ie.py](file:///home/mehdi/Projects/AutoRAI/code/run_ie.py)

CLI entry point to run the IE pipeline:
- Accepts: `--section` (I/N/O), `--hadm_id`, `--output` path
- Loads the appropriate adapter
- Runs the orchestrator
- Saves the evidence package as JSON

---

## User Review Required

> [!IMPORTANT]
> **No normalization layer**: As you specified, the v1 implementation skips entity normalization (Mode C from the architecture doc). The unstructured path will extract entities and classify assertion/temporality via LLM, but won't normalize "Lasix" → "furosemide" → ATC code. For the structured path, code mapping is done via pre-built mapping tables. This means unstructured mentions map to target items via keyword/semantic matching rather than ontology codes.

> [!IMPORTANT]
> **Medication class mapping**: Since there's no normalization, Section N's structured path will use keyword-based drug-name-to-class matching (matching the `drug` column in `prescriptions`/`emar` against keyword lists per medication class). This is a pragmatic v1 approach; it will miss some drugs but should catch the common ones.

> [!WARNING]
> **LLM dependency**: The unstructured pipeline relies on an LLM for extraction and attribute classification. I'll implement this using an OpenAI-compatible API. Which LLM provider/model would you like to use? (e.g., OpenRouter with Claude, direct OpenAI API with GPT-4, local model via Ollama, etc.)

---

## Open Questions

1. **LLM provider**: Which LLM API should the code target? (OpenAI, OpenRouter, Ollama, etc.)
2. **Data paths**: The MIMIC data is at `data/Mimic/mimic-iv-2.2/mimic-iv-2.2/` (with a double directory level from zip extraction). Should I use that path as-is, or would you like to flatten it?
3. **Scope of first run**: Should all three section adapters (I, N, O) be implemented in this first coding session, or would you prefer to start with Section I only and iterate?
4. **Python environment**: Should I set up a `pyproject.toml` with `uv` for dependency management, or is there an existing environment preference?

---

## Verification Plan

### Automated Tests
- Unit tests for data models (serialization/deserialization)
- Unit tests for ICD-to-MDS code mapping
- Unit tests for drug keyword matching
- Integration test: run the structured pipeline on a single `hadm_id` from MIMIC and verify the evidence package structure
- Integration test: run the full orchestrator on a sample episode

### Manual Verification
- Inspect the evidence package output for a sample patient to verify it makes clinical sense
- Compare structured path output against raw MIMIC tables for correctness
