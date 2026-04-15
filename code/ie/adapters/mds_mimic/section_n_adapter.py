"""
MDS Section N (Medications) adapter for MIMIC-IV.

Provides:
- Extraction schema for medication extraction
- Drug-name-to-medication-class keyword mapper (v1, no normalization)
- Data loading from eMAR, prescriptions, and discharge notes

Target items (v1):
  N0415A1  Antipsychotic: Has received
  N0415B1  Antianxiety: Has received
  N0415C1  Antidepressant: Has received
  N0415D1  Hypnotic: Has received
  N0415E1  Anticoagulant: Has received
  N0415F1  Antibiotic: Has received
  N0415G1  Diuretic: Has received
  N0415H1  Opioid: Has received
  N0415I1  Antiplatelet: Has received
  N0415J1  Hypoglycemic: Has received
  N0415K1  Anticonvulsant: Has received
  N0350A   Insulin injections
  N0350B   Orders for insulin
"""

from __future__ import annotations

import json
import logging
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

_MAPPINGS_DIR = Path(__file__).parent / "mappings"


# ---------------------------------------------------------------------------
# Drug Name → Medication Class Keyword Mapper
# ---------------------------------------------------------------------------

class SectionNCodeMapper(StructuredCodeMapper):
    """
    Maps drug names to MDS Section N medication class items
    using keyword matching.

    This is a v1 approach — no ontology normalization. Keywords
    are matched case-insensitively against the drug name column.
    """

    def __init__(self, mapping_path: Path | None = None):
        self.mapping_path = mapping_path or (_MAPPINGS_DIR / "drug_class_keywords.json")
        self._load_mappings()

    def _load_mappings(self) -> None:
        """Load drug class keyword mappings."""
        with open(self.mapping_path) as f:
            self.mappings = json.load(f)

        # Pre-process keywords to lowercase
        self._keyword_index: dict[str, list[str]] = {}
        for item_id, item_data in self.mappings.items():
            keywords = [k.lower() for k in item_data.get("keywords", [])]
            self._keyword_index[item_id] = keywords

        logger.info(f"Loaded Section N mappings: {len(self.mappings)} medication classes")

    def map_code_to_target_items(
        self, code: str, code_system: str
    ) -> list[str]:
        """
        Map a drug name to Section N medication class items.

        For Section N, 'code' is typically a drug name from the
        prescriptions or eMAR table, not a formal code.

        Args:
            code: Drug name (e.g., "Furosemide", "Heparin").
            code_system: Expected to be "drug_name" for Section N.

        Returns:
            List of matching medication class item IDs.
        """
        drug_lower = code.strip().lower()
        if not drug_lower:
            return []

        matched_items = []
        for item_id, keywords in self._keyword_index.items():
            for keyword in keywords:
                if keyword in drug_lower or drug_lower in keyword:
                    matched_items.append(item_id)
                    break

        return matched_items


# ---------------------------------------------------------------------------
# Section N Adapter
# ---------------------------------------------------------------------------

class SectionNAdapter(IEAdapter):
    """
    MDS Section N (Medications) adapter for MIMIC-IV.

    Data priority:
    - eMAR (actual administrations) > prescriptions (orders)
    - eMAR event_txt "Administered"/"Applied" = positive evidence
    - eMAR event_txt "Not Given"/"Delayed" = non-positive
    """

    def __init__(self, config: IEConfig | None = None):
        self.config = config or get_config()
        self.paths = self.config.mimic_paths
        self._code_mapper = SectionNCodeMapper()
        self._temporal_resolver = MIMICTemporalResolver()
        self._fusion_policy = ClinicalFusionPolicy(
            code_without_text_confidence=0.6,  # structured meds are stronger
            text_without_code_confidence=0.4,   # text-only med evidence is weaker
        )
        self._admissions_cache: pd.DataFrame | None = None

    def get_extraction_schema(self) -> ExtractionSchema:
        """Return the extraction schema for Section N (medications)."""

        prompt_template = ""
        prompt_path = Path(__file__).parent / "prompts" / "extraction_prompt.txt"
        if prompt_path.exists():
            prompt_template = prompt_path.read_text()

        return ExtractionSchema(
            task_name="MDS Section N: Medications",
            target_domain="clinical",
            entity_types=[
                EntityType(
                    name="medication",
                    description=(
                        "A medication, drug, or pharmaceutical mentioned in the "
                        "text. Include both brand and generic names. Extract "
                        "medications from any part of the note (hospital course, "
                        "discharge medications, medication lists, etc.). Include "
                        "both specific drugs (e.g., 'furosemide 40mg') and drug "
                        "classes when the specific drug is unclear."
                    ),
                    examples=[
                        "furosemide", "Lasix", "metoprolol", "insulin",
                        "heparin", "vancomycin", "quetiapine", "oxycodone",
                        "aspirin", "clopidogrel", "levetiracetam", "lorazepam",
                    ],
                ),
            ],
            attributes=[
                AttributeDimension(
                    name="assertion",
                    labels=["affirmed", "negated", "uncertain"],
                    description=(
                        "Whether the medication was actually given/taken "
                        "(affirmed), explicitly not given or discontinued "
                        "(negated), or uncertain/conditional."
                    ),
                    default_label="affirmed",
                ),
                AttributeDimension(
                    name="temporality",
                    labels=["current", "historical"],
                    description=(
                        "Whether the medication was given during this "
                        "hospitalization (current) or only in the past "
                        "(historical). Medications in 'discharge medications' "
                        "or 'hospital course' are current. 'Home medications' "
                        "that were continued are also current."
                    ),
                    default_label="current",
                ),
            ],
            target_items=self._get_target_items(),
            structured_sources=[
                StructuredSourceConfig(
                    source_name="emar",
                    code_column="medication",
                    code_system="drug_name",
                    date_column="charttime",
                    additional_columns=["emar_id", "event_txt"],
                ),
                StructuredSourceConfig(
                    source_name="prescriptions",
                    code_column="drug",
                    code_system="drug_name",
                    date_column="starttime",
                    date_end_column="stoptime",
                    additional_columns=["pharmacy_id", "drug_type", "route", "dose_val_rx", "dose_unit_rx"],
                ),
            ],
            lookback=LookbackConfig(type="admission_window"),
            fusion_policy=FusionPolicyConfig(
                code_without_text_confidence=0.6,
                text_without_code_confidence=0.4,
            ),
            extraction_prompt_template=prompt_template,
        )

    def _get_target_items(self) -> list[TargetItem]:
        """Define the target items for Section N."""
        return [
            TargetItem(
                item_id="N0415A1", name="Antipsychotic: Has received",
                description="Any antipsychotic medication received during the look-back period",
                keywords=["antipsychotic", "haloperidol", "quetiapine", "olanzapine", "risperidone", "aripiprazole", "ziprasidone", "chlorpromazine", "haldol", "seroquel", "zyprexa"],
            ),
            TargetItem(
                item_id="N0415B1", name="Antianxiety: Has received",
                description="Any antianxiety medication received",
                keywords=["antianxiety", "anxiolytic", "lorazepam", "diazepam", "alprazolam", "clonazepam", "buspirone", "hydroxyzine", "ativan", "valium", "xanax"],
            ),
            TargetItem(
                item_id="N0415C1", name="Antidepressant: Has received",
                description="Any antidepressant medication received",
                keywords=["antidepressant", "sertraline", "fluoxetine", "citalopram", "escitalopram", "venlafaxine", "duloxetine", "mirtazapine", "trazodone", "bupropion", "amitriptyline", "zoloft", "prozac", "lexapro", "effexor", "cymbalta"],
            ),
            TargetItem(
                item_id="N0415D1", name="Hypnotic: Has received",
                description="Any hypnotic/sedative medication received for sleep",
                keywords=["hypnotic", "zolpidem", "eszopiclone", "zaleplon", "ambien", "lunesta", "temazepam", "ramelteon"],
            ),
            TargetItem(
                item_id="N0415E1", name="Anticoagulant: Has received",
                description="Any anticoagulant medication received",
                keywords=["anticoagulant", "heparin", "enoxaparin", "warfarin", "coumadin", "apixaban", "rivaroxaban", "dabigatran", "lovenox", "eliquis", "xarelto", "fondaparinux"],
            ),
            TargetItem(
                item_id="N0415F1", name="Antibiotic: Has received",
                description="Any antibiotic medication received",
                keywords=["antibiotic", "vancomycin", "ceftriaxone", "piperacillin", "tazobactam", "ciprofloxacin", "levofloxacin", "metronidazole", "azithromycin", "amoxicillin", "ampicillin", "cefazolin", "cefepime", "meropenem", "doxycycline", "trimethoprim", "sulfamethoxazole", "clindamycin", "gentamicin", "zosyn", "flagyl"],
            ),
            TargetItem(
                item_id="N0415G1", name="Diuretic: Has received",
                description="Any diuretic medication received",
                keywords=["diuretic", "furosemide", "lasix", "hydrochlorothiazide", "hctz", "spironolactone", "bumetanide", "metolazone", "torsemide", "chlorthalidone", "bumex", "aldactone"],
            ),
            TargetItem(
                item_id="N0415H1", name="Opioid: Has received",
                description="Any opioid pain medication received",
                keywords=["opioid", "morphine", "oxycodone", "hydromorphone", "fentanyl", "hydrocodone", "codeine", "methadone", "tramadol", "dilaudid", "percocet", "oxycontin", "buprenorphine", "nalbuphine"],
            ),
            TargetItem(
                item_id="N0415I1", name="Antiplatelet: Has received",
                description="Any antiplatelet medication received",
                keywords=["antiplatelet", "aspirin", "clopidogrel", "plavix", "ticagrelor", "prasugrel", "dipyridamole"],
            ),
            TargetItem(
                item_id="N0415J1", name="Hypoglycemic: Has received",
                description="Any oral hypoglycemic/antidiabetic medication received (not insulin)",
                keywords=["hypoglycemic", "metformin", "glipizide", "glyburide", "sitagliptin", "empagliflozin", "dapagliflozin", "pioglitazone", "glucophage", "januvia", "jardiance", "liraglutide", "semaglutide"],
            ),
            TargetItem(
                item_id="N0415K1", name="Anticonvulsant: Has received",
                description="Any anticonvulsant/antiepileptic medication received",
                keywords=["anticonvulsant", "antiepileptic", "levetiracetam", "phenytoin", "valproic", "valproate", "carbamazepine", "lamotrigine", "gabapentin", "topiramate", "keppra", "dilantin", "depakote", "neurontin", "lacosamide", "oxcarbazepine"],
            ),
            TargetItem(
                item_id="N0350A", name="Insulin Injections",
                description="Insulin given via injection during the look-back period",
                keywords=["insulin"],
            ),
            TargetItem(
                item_id="N0350B", name="Orders for Insulin",
                description="Any orders for insulin during the look-back period",
                keywords=["insulin"],
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
        """
        Load medication structured data for the given episode.

        Loads eMAR (primary, actual administrations) and prescriptions
        (secondary, orders). Filters eMAR to positive administrations.
        """
        result: dict[str, pd.DataFrame] = {}

        # Load eMAR (primary)
        try:
            emar_df = load_table_for_episode(
                paths=self.paths,
                module="hosp",
                table_name="emar",
                episode_id=episode_id,
                episode_column="hadm_id",
                usecols=["subject_id", "hadm_id", "emar_id", "charttime", "medication", "event_txt"],
            )
            # Filter to positive administration events
            positive_events = ["Administered", "Applied", "Started", "Restarted"]
            if not emar_df.empty and "event_txt" in emar_df.columns:
                emar_df = emar_df[
                    emar_df["event_txt"].isin(positive_events)
                ]
            result["emar"] = emar_df
            logger.info(f"eMAR: {len(emar_df)} positive administration records")
        except Exception as e:
            logger.warning(f"Failed to load eMAR: {e}")
            result["emar"] = pd.DataFrame()

        # Load prescriptions (secondary)
        try:
            rx_df = load_table_for_episode(
                paths=self.paths,
                module="hosp",
                table_name="prescriptions",
                episode_id=episode_id,
                episode_column="hadm_id",
                usecols=[
                    "subject_id", "hadm_id", "pharmacy_id", "starttime",
                    "stoptime", "drug_type", "drug", "route",
                    "dose_val_rx", "dose_unit_rx",
                ],
            )
            result["prescriptions"] = rx_df
            logger.info(f"Prescriptions: {len(rx_df)} records")
        except Exception as e:
            logger.warning(f"Failed to load prescriptions: {e}")
            result["prescriptions"] = pd.DataFrame()

        return result

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

        return documents

    def get_episode_metadata(self, episode_id: str) -> dict[str, Any]:
        """Load admission metadata."""
        if self._admissions_cache is None:
            self._admissions_cache = load_mimic_table(
                paths=self.paths,
                module="hosp",
                table_name="admissions",
                usecols=["subject_id", "hadm_id", "admittime", "dischtime"],
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
            return {}

        row = row.iloc[0]
        return {
            "subject_id": str(row.get("subject_id", "")),
            "hadm_id": str(row.get("hadm_id", "")),
            "admittime": str(row.get("admittime", "")),
            "dischtime": str(row.get("dischtime", "")),
        }
