"""
MDS Section I (Active Diagnoses) adapter for MIMIC-IV.

Provides:
- Extraction schema for disease extraction with assertion/temporality
- ICD-to-Section-I code mapper
- Data loading from diagnoses_icd + discharge notes

Target items (v1):
  I0200  Anemia
  I0300  Atrial fibrillation / other dysrhythmias
  I0400  Coronary artery disease
  I0600  Heart failure
  I0700  Hypertension
  I0900  Peripheral vascular disease / PAD
  I2900  Diabetes mellitus
  I4500  CVA / TIA / stroke
  I4900  Hemiplegia or hemiparesis
  I6200  Asthma / COPD / chronic lung disease
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd

from ie.config import IEConfig, get_config
from ie.core.interfaces import (
    FusionPolicy,
    IEAdapter,
    StructuredCodeMapper,
    TemporalResolver,
)
from ie.core.models import (
    AttributeDimension,
    Document,
    EntityType,
    ExtractionSchema,
    FusionPolicyConfig,
    LookbackConfig,
    StructuredSourceConfig,
    TargetItem,
)
from ie.adapters.mds_mimic.base_adapter import (
    ClinicalFusionPolicy,
    MIMICTemporalResolver,
    load_table_for_episode,
    load_mimic_table,
    segment_discharge_note,
)

logger = logging.getLogger(__name__)

# Path to mapping files
_MAPPINGS_DIR = Path(__file__).parent / "mappings"


# ---------------------------------------------------------------------------
# ICD-to-Section-I Code Mapper
# ---------------------------------------------------------------------------

class SectionICodeMapper(StructuredCodeMapper):
    """
    Maps ICD-10/ICD-9 diagnosis codes to MDS Section I items.

    Uses regex patterns from the icd_to_section_i.json mapping file.
    """

    def __init__(self, mapping_path: Path | None = None):
        self.mapping_path = mapping_path or (_MAPPINGS_DIR / "icd_to_section_i.json")
        self._load_mappings()

    def _load_mappings(self) -> None:
        """Load ICD-to-MDS mapping patterns."""
        with open(self.mapping_path) as f:
            self.mappings = json.load(f)

        # Pre-compile regex patterns
        self._compiled: dict[str, dict[str, list[re.Pattern]]] = {}
        for item_id, item_data in self.mappings.items():
            self._compiled[item_id] = {
                "icd10": [
                    re.compile(p.replace(".", r"\.").replace("*", ".*"))
                    for p in item_data.get("icd10_patterns", [])
                ],
                "icd9": [
                    re.compile(p.replace(".", r"\.").replace("*", ".*"))
                    for p in item_data.get("icd9_patterns", [])
                ],
            }

        logger.info(f"Loaded Section I mappings: {len(self.mappings)} items")

    def map_code_to_target_items(
        self, code: str, code_system: str
    ) -> list[str]:
        """
        Map an ICD code to Section I target item(s).

        Args:
            code: The ICD code (e.g., "I50.9", "4280").
            code_system: "ICD10" or "ICD9".

        Returns:
            List of matching target item IDs.
        """
        matched_items = []
        code_clean = code.strip().upper()
        key = "icd10" if "10" in code_system.upper() else "icd9"

        for item_id, patterns in self._compiled.items():
            for pattern in patterns.get(key, []):
                if pattern.fullmatch(code_clean):
                    matched_items.append(item_id)
                    break

        return matched_items


# ---------------------------------------------------------------------------
# Section I Adapter
# ---------------------------------------------------------------------------

class SectionIAdapter(IEAdapter):
    """
    MDS Section I (Active Diagnoses) adapter for MIMIC-IV.

    Implements the IEAdapter interface to provide:
    - An extraction schema for disease mentions
    - ICD-to-Section-I code mapper
    - Data loading from diagnoses_icd and discharge notes
    """

    def __init__(self, config: IEConfig | None = None):
        self.config = config or get_config()
        self.paths = self.config.mimic_paths
        self._code_mapper = SectionICodeMapper()
        self._temporal_resolver = MIMICTemporalResolver()
        self._fusion_policy = ClinicalFusionPolicy()

        # Cache for expensive table loads
        self._admissions_cache: pd.DataFrame | None = None

    def get_extraction_schema(self) -> ExtractionSchema:
        """Return the extraction schema for Section I (diagnoses)."""

        # Load prompt template if available
        prompt_template = ""
        prompt_path = Path(__file__).parent / "prompts" / "extraction_prompt.txt"
        if prompt_path.exists():
            prompt_template = prompt_path.read_text()

        return ExtractionSchema(
            task_name="MDS Section I: Active Diagnoses",
            target_domain="clinical",
            entity_types=[
                EntityType(
                    name="disease",
                    description=(
                        "A clinical condition, disease, or diagnosis relevant to "
                        "the patient's current or recent health status. Include "
                        "both specific conditions (e.g., 'type 2 diabetes mellitus') "
                        "and general descriptions (e.g., 'heart disease'). Extract "
                        "conditions mentioned anywhere in the note, including those "
                        "described in abbreviated form (MI, CHF, COPD, etc.)."
                    ),
                    examples=[
                        "heart failure", "atrial fibrillation", "diabetes mellitus",
                        "COPD", "hypertension", "stroke", "anemia", "CHF",
                        "coronary artery disease", "peripheral vascular disease",
                        "hemiparesis", "asthma", "TIA",
                    ],
                ),
            ],
            attributes=[
                AttributeDimension(
                    name="assertion",
                    labels=["affirmed", "negated", "uncertain"],
                    description=(
                        "Whether the condition is affirmed (present), negated "
                        "(explicitly denied or ruled out), or uncertain (possible, "
                        "suspected, cannot rule out). Examples:\n"
                        "- 'Patient has diabetes' → affirmed\n"
                        "- 'No evidence of pneumonia' → negated\n"
                        "- 'Possible UTI' → uncertain\n"
                        "- 'History of MI' → affirmed (but may be historical)"
                    ),
                    default_label="affirmed",
                ),
                AttributeDimension(
                    name="temporality",
                    labels=["current", "historical"],
                    description=(
                        "Whether the condition is current (active during this "
                        "hospitalization, relevant to current care) or historical "
                        "(past condition, not actively affecting current care). "
                        "Use section context as a signal:\n"
                        "- Mentions in 'Hospital Course', 'Active Issues', or "
                        "'Discharge Diagnoses' → likely current\n"
                        "- Mentions in 'Past Medical History' → likely historical "
                        "unless described as active or ongoing\n"
                        "- 'History of X' → historical\n"
                        "- 'Admitted for X' → current"
                    ),
                    default_label="current",
                ),
            ],
            target_items=self._get_target_items(),
            structured_sources=[
                StructuredSourceConfig(
                    source_name="diagnoses_icd",
                    table_path="hosp/diagnoses_icd.csv.gz",
                    code_column="icd_code",
                    code_system="ICD",
                    additional_columns=["seq_num", "icd_version"],
                ),
            ],
            lookback=LookbackConfig(type="admission_window"),
            fusion_policy=FusionPolicyConfig(),
            extraction_prompt_template=prompt_template,
        )

    def _get_target_items(self) -> list[TargetItem]:
        """Define the target items for Section I."""
        return [
            TargetItem(
                item_id="I0200",
                name="Anemia",
                description="Anemia (e.g., aplastic, iron deficiency, pernicious, sickle cell)",
                keywords=["anemia", "anemic", "low hemoglobin", "low hgb", "pancytopenia"],
            ),
            TargetItem(
                item_id="I0300",
                name="Atrial Fibrillation or Other Dysrhythmias",
                description="Atrial fibrillation, atrial flutter, or other cardiac dysrhythmias",
                keywords=["atrial fibrillation", "afib", "a-fib", "atrial flutter", "dysrhythmia", "arrhythmia", "svt", "vtach", "bradycardia", "tachycardia"],
            ),
            TargetItem(
                item_id="I0400",
                name="Coronary Artery Disease",
                description="Coronary artery disease (CAD), ischemic heart disease",
                keywords=["coronary artery disease", "cad", "ischemic heart", "myocardial infarction", "mi", "nstemi", "stemi", "angina"],
            ),
            TargetItem(
                item_id="I0600",
                name="Heart Failure",
                description="Heart failure (e.g., CHF, systolic, diastolic, HFrEF, HFpEF)",
                keywords=["heart failure", "chf", "congestive heart failure", "hfref", "hfpef", "systolic dysfunction", "diastolic dysfunction", "cardiomyopathy"],
            ),
            TargetItem(
                item_id="I0700",
                name="Hypertension",
                description="Hypertension (high blood pressure)",
                keywords=["hypertension", "htn", "high blood pressure", "hypertensive"],
            ),
            TargetItem(
                item_id="I0900",
                name="Peripheral Vascular Disease (PVD) or Peripheral Arterial Disease (PAD)",
                description="Peripheral vascular or arterial disease",
                keywords=["peripheral vascular disease", "pvd", "peripheral arterial disease", "pad", "claudication", "peripheral ischemia"],
            ),
            TargetItem(
                item_id="I2900",
                name="Diabetes Mellitus",
                description="Diabetes mellitus (type 1, type 2, or unspecified)",
                keywords=["diabetes", "diabetic", "dm", "type 1 diabetes", "type 2 diabetes", "t2dm", "t1dm", "insulin dependent", "niddm", "iddm", "hyperglycemia"],
            ),
            TargetItem(
                item_id="I4500",
                name="Cerebrovascular Accident (CVA), Transient Ischemic Attack (TIA), or Stroke",
                description="Stroke, TIA, or cerebrovascular accident",
                keywords=["stroke", "cva", "cerebrovascular accident", "tia", "transient ischemic attack", "cerebral infarction", "intracranial hemorrhage", "ich"],
            ),
            TargetItem(
                item_id="I4900",
                name="Hemiplegia or Hemiparesis",
                description="Hemiplegia or hemiparesis (one-sided weakness or paralysis)",
                keywords=["hemiplegia", "hemiparesis", "hemiparetic", "hemiplegic", "one-sided weakness", "unilateral weakness"],
            ),
            TargetItem(
                item_id="I6200",
                name="Asthma, COPD, or Chronic Lung Disease",
                description="Asthma, COPD, chronic bronchitis, emphysema, or other chronic lung disease",
                keywords=["asthma", "copd", "chronic obstructive pulmonary", "emphysema", "chronic bronchitis", "chronic lung disease", "interstitial lung disease", "pulmonary fibrosis", "ild"],
            ),
        ]

    def get_structured_code_mapper(self) -> StructuredCodeMapper:
        return self._code_mapper

    def get_temporal_resolver(self) -> TemporalResolver:
        return self._temporal_resolver

    def get_fusion_policy(self) -> FusionPolicy:
        return self._fusion_policy

    def load_structured_data(
        self, episode_id: str
    ) -> dict[str, pd.DataFrame]:
        """Load diagnoses_icd for the given episode."""
        df = load_table_for_episode(
            paths=self.paths,
            module="hosp",
            table_name="diagnoses_icd",
            episode_id=episode_id,
            episode_column="hadm_id",
            usecols=["subject_id", "hadm_id", "seq_num", "icd_code", "icd_version"],
            dtype={"icd_code": str},
        )

        # Determine ICD version and set code_system marker
        if not df.empty and "icd_version" in df.columns:
            df["code_system"] = df["icd_version"].apply(
                lambda v: "ICD10" if str(v) == "10" else "ICD9"
            )

        return {"diagnoses_icd": df}

    def load_unstructured_data(
        self, episode_id: str
    ) -> list[Document]:
        """Load discharge notes for the given episode."""
        df = load_table_for_episode(
            paths=self.paths,
            module="note",
            table_name="discharge",
            episode_id=episode_id,
            episode_column="hadm_id",
            usecols=["note_id", "subject_id", "hadm_id", "note_type", "charttime", "text"],
        )

        documents = []
        for _, row in df.iterrows():
            text = str(row.get("text", ""))
            sections = segment_discharge_note(text)

            doc = Document(
                document_id=str(row.get("note_id", "")),
                episode_id=str(row.get("hadm_id", "")),
                patient_id=str(row.get("subject_id", "")),
                text=text,
                document_type="discharge_summary",
                timestamp=str(row.get("charttime", "")),
                sections=sections,
            )
            documents.append(doc)

        logger.info(
            f"Loaded {len(documents)} discharge notes for episode {episode_id}"
        )
        return documents

    def get_episode_metadata(self, episode_id: str) -> dict[str, Any]:
        """Load admission metadata for temporal context."""
        if self._admissions_cache is None:
            self._admissions_cache = load_mimic_table(
                paths=self.paths,
                module="hosp",
                table_name="admissions",
                usecols=[
                    "subject_id", "hadm_id", "admittime", "dischtime",
                    "admission_type", "admission_location", "discharge_location",
                ],
            )

        try:
            episode_val = int(float(episode_id))
            row = self._admissions_cache[
                self._admissions_cache["hadm_id"] == episode_val
            ]
        except (ValueError, TypeError):
            row = self._admissions_cache[
                self._admissions_cache["hadm_id"].astype(str) == str(episode_id)
            ]

        if row.empty:
            logger.warning(f"No admission found for episode {episode_id}")
            return {}

        row = row.iloc[0]
        return {
            "subject_id": str(row.get("subject_id", "")),
            "hadm_id": str(row.get("hadm_id", "")),
            "admittime": str(row.get("admittime", "")),
            "dischtime": str(row.get("dischtime", "")),
            "admission_type": str(row.get("admission_type", "")),
        }
