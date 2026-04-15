# AutoRAI IE Component: Code Structure Reference

This document defines the directory layout, module responsibilities, data flow, and coding conventions for the Information Extraction (IE) component. **All future LLM calls should read this document before writing or modifying code in `code/ie/`.**

---

## Directory Tree

```
code/
├── code_structure.md          # THIS FILE — architecture reference for LLM calls
├── pyproject.toml             # Python project config and dependencies
├── run_ie.py                  # CLI entry point for running the IE pipeline
│
└── ie/
    ├── __init__.py
    ├── config.py              # Configuration (data paths, LLM settings, defaults)
    │
    ├── core/                  # LAYER 1: Domain-agnostic extraction engine
    │   ├── __init__.py
    │   ├── models.py          # Canonical data structures (dataclasses)
    │   ├── interfaces.py      # Abstract base classes that adapters implement
    │   ├── llm_client.py      # Thin wrapper around OpenAI-compatible LLM APIs
    │   ├── unstructured_pipeline.py  # Schema-driven text extraction via LLM
    │   ├── structured_pipeline.py    # Schema-driven structured data extraction
    │   ├── fusion.py          # Merges evidence from both paths
    │   └── orchestrator.py    # End-to-end IE flow coordinator
    │
    └── adapters/              # LAYER 2: Domain/dataset-specific adapters
        ├── __init__.py
        └── mds_mimic/         # MDS form completion on MIMIC-IV data
            ├── __init__.py
            ├── base_adapter.py       # Shared MIMIC loading utilities
            ├── section_i_adapter.py  # Section I: Active Diagnoses
            ├── section_n_adapter.py  # Section N: Medications
            ├── section_o_adapter.py  # Section O: Special Treatments
            ├── mappings/             # Pre-built code-to-MDS mapping tables
            │   ├── icd_to_section_i.json
            │   ├── drug_class_keywords.json
            │   └── procedure_to_section_o.json
            └── prompts/              # LLM prompt templates
                └── extraction_prompt.txt
```

---

## Module Responsibilities

### Layer 1 — Core (`ie/core/`)

| Module | Responsibility |
|--------|---------------|
| `models.py` | Defines all canonical data structures as Python dataclasses: `ExtractionSchema`, `EntityType`, `AttributeDimension`, `TargetItem`, `SourceConfig`, `Mention`, `StructuredEvidence`, `EvidenceRecord`, `EvidencePackage`, `Document`. These are the shared vocabulary across all modules. |
| `interfaces.py` | Defines abstract base classes (`IEAdapter`, `StructuredCodeMapper`, `TemporalResolver`, `FusionPolicy`) that Layer 2 adapters must implement. Layer 1 code depends only on these interfaces, never on concrete adapters. |
| `llm_client.py` | Thin wrapper around OpenAI-compatible API. Handles prompt sending, response parsing, retries, and error handling. Used by `unstructured_pipeline.py`. |
| `unstructured_pipeline.py` | Takes documents + schema → returns `List[Mention]`. Builds an LLM prompt dynamically from the schema's entity types and attribute dimensions, sends it, and parses the structured JSON response into `Mention` objects. |
| `structured_pipeline.py` | Takes DataFrames + schema + code mapper + temporal resolver → returns `List[StructuredEvidence]`. Maps codes to target items, filters by time window, checks active status. Pure data processing, no LLM calls. |
| `fusion.py` | Takes mentions + structured evidence + schema + fusion policy → returns `EvidencePackage`. Groups evidence by target item, detects agreements/conflicts, applies the fusion policy, computes confidence scores. |
| `orchestrator.py` | Coordinates the full IE flow: load schema → load data → run unstructured pipeline → run structured pipeline → fuse → return `EvidencePackage`. This is the single entry point that downstream components call. |

### Layer 2 — MIMIC Adapter (`ie/adapters/mds_mimic/`)

| Module | Responsibility |
|--------|---------------|
| `base_adapter.py` | Shared MIMIC infrastructure: data path resolution, loading compressed CSVs, loading discharge notes, segmenting notes by section headers, admission window computation. |
| `section_i_adapter.py` | Provides the extraction schema for MDS Section I (diagnoses), ICD-to-Section-I code mapper, and Section I–specific fusion policy. Targets 10 diagnosis items. |
| `section_n_adapter.py` | Provides the extraction schema for MDS Section N (medications), drug-name-to-class keyword mapper, and medication-specific data loading (eMAR, prescriptions). Targets 13 medication items. |
| `section_o_adapter.py` | Provides the extraction schema for MDS Section O (treatments/procedures), ICD-PCS/ICU-item-to-Section-O mapper, and procedure-specific data loading. Targets 9+ treatment items. |

### Mappings (`ie/adapters/mds_mimic/mappings/`)

| File | Content |
|------|---------|
| `icd_to_section_i.json` | ICD-10/ICD-9 code patterns (regex) → Section I item IDs |
| `drug_class_keywords.json` | Drug name keywords → Section N medication class items |
| `procedure_to_section_o.json` | ICD-PCS codes, HCPCS codes, and ICU itemids → Section O items |

---

## Data Flow

```
                          ┌─────────────────────┐
                          │    IEOrchestrator    │
                          │   orchestrator.py    │
                          └──────────┬──────────┘
                                     │
                     ┌───────────────┼───────────────┐
                     │               │               │
              ┌──────▼──────┐  ┌─────▼─────┐  ┌─────▼─────┐
              │   Adapter    │  │ Unstruct. │  │  Struct.  │
              │ (Layer 2)    │  │ Pipeline  │  │ Pipeline  │
              │ get_schema() │  │ LLM-based │  │ code map  │
              └──────┬───────┘  └─────┬─────┘  └─────┬─────┘
                     │                │               │
                     │          List[Mention]   List[StructEvid]
                     │                │               │
                     │                └───────┬───────┘
                     │                        │
                     │                 ┌──────▼──────┐
                     │                 │   Fusion    │
                     │                 │  fusion.py  │
                     │                 └──────┬──────┘
                     │                        │
                     │                 EvidencePackage
                     │                        │
                     └────────────────────────▼
                                        (output)
```

**Execution sequence:**
1. Caller selects an adapter (e.g., `SectionIAdapter`)
2. Orchestrator calls `adapter.get_extraction_schema()` to get the schema
3. Orchestrator calls `adapter.load_unstructured_data(episode_id)` → documents
4. Orchestrator calls `adapter.load_structured_data(episode_id)` → DataFrames
5. `UnstructuredPipeline.extract(documents, schema)` → `List[Mention]`
6. `StructuredPipeline.extract(dataframes, schema, code_mapper, temporal_resolver)` → `List[StructuredEvidence]`
7. `EvidenceFusion.fuse(mentions, structured_evidence, schema, fusion_policy)` → `EvidencePackage`
8. Return `EvidencePackage`

---

## Key Design Patterns & Conventions

### Naming
- Classes: `PascalCase` (e.g., `EvidenceRecord`, `SectionIAdapter`)
- Functions/methods: `snake_case` (e.g., `get_extraction_schema`, `map_code_to_target_items`)
- Constants: `UPPER_SNAKE_CASE` (e.g., `DEFAULT_MODEL`)
- Files: `snake_case.py`
- JSON mappings: `snake_case.json`

### Imports
- Core modules import only from `models.py` and `interfaces.py`, never from adapters
- Adapters import from `models.py` and `interfaces.py` to implement the interfaces
- The orchestrator imports from all core modules but never from specific adapters (it receives an adapter instance)
- External dependencies: `pandas`, `openai`, `dataclasses`, `json`, `pathlib`, `abc`, `logging`

### Error Handling
- Use Python's `logging` module (logger per module: `logger = logging.getLogger(__name__)`)
- Extraction failures (LLM errors, parse errors) → log warning, return empty list, don't crash
- Missing data files → raise `FileNotFoundError` with descriptive message
- No normalization failures possible in v1 (normalization is skipped)

### Data Serialization
- All dataclasses implement `to_dict()` → `dict` for JSON serialization
- Class method `from_dict(data)` → instance for deserialization
- `EvidencePackage` has `to_json(path)` and `from_json(path)` convenience methods

### Configuration
- All paths and API keys flow through `ie/config.py`
- Environment variables for secrets: `AUTORAI_LLM_API_KEY`, `AUTORAI_LLM_BASE_URL`
- Data paths default to project-relative paths but are overridable

---

## Canonical Data Model Reference

### Input Objects
- **`ExtractionSchema`** — The control document for the entire IE pipeline. Contains entity types, attribute dimensions, target items, source configs, fusion policy config.
- **`Document`** — One unstructured text document with metadata (note_id, hadm_id, text, sections).

### Intermediate Objects
- **`Mention`** — One extracted span from text: text, entity_type, attributes (assertion, temporality), source info, optional target_item_candidates.
- **`StructuredEvidence`** — One coded record after mapping/filtering: raw code, mapped target items, timestamp, active status, source table.

### Output Objects
- **`EvidenceRecord`** — Fused evidence for one target item: supporting mentions, supporting structured evidence, final status, confidence, conflict flags.
- **`EvidencePackage`** — Collection of EvidenceRecords for one episode + section. This is the IE component's output consumed by RAG and prediction components.
