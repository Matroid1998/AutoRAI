"""
MDS Section O (Special Treatments, Procedures, and Programs) adapter for MIMIC-IV.

Provides:
- Extraction schema for treatment/procedure extraction
- Procedure code + ICU item mapper to Section O items
- Data loading from procedures_icd, hcpcsevents, inputevents,
  procedureevents, and discharge notes

Target items (v1):
  O0110H1   IV medications
  O0110H2   IV vasoactive medications
  O0110H3   IV antibiotics
  O0110H4   IV anticoagulant
  O0110H10  IV other
  O0110I1   Transfusions
  O0110J1   Dialysis
  O0110J2   Hemodialysis
  O0110J3   Peritoneal dialysis
  O0110G2   BiPAP
  O0110G3   CPAP
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

_MAPPINGS_DIR = Path(__file__).parent / "mappings"


# ---------------------------------------------------------------------------
# Procedure/Treatment → Section O Code Mapper
# ---------------------------------------------------------------------------

class SectionOCodeMapper(StructuredCodeMapper):
    """
    Maps procedure codes and ICU item IDs to MDS Section O items.

    Handles multiple code systems:
    - ICD-10-PCS procedure codes
    - ICD-9 procedure codes
    - HCPCS codes
    - d_items itemid labels (for ICU events)
    - Drug names for IV medication classification
    """

    def __init__(self, mapping_path: Path | None = None):
        self.mapping_path = mapping_path or (_MAPPINGS_DIR / "procedure_to_section_o.json")
        self._load_mappings()

    def _load_mappings(self) -> None:
        """Load procedure-to-Section O mappings."""
        with open(self.mapping_path) as f:
            self.mappings = json.load(f)

        # Build keyword index for fast matching
        self._keyword_index: dict[str, list[str]] = {}
        self._code_patterns: dict[str, list[re.Pattern]] = {}

        for item_id, item_data in self.mappings.items():
            # Keywords for label-based matching
            self._keyword_index[item_id] = [
                k.lower() for k in item_data.get("keywords", [])
            ]
            # ICD code patterns
            patterns = []
            for p in item_data.get("icd_patterns", []):
                patterns.append(
                    re.compile(p.replace(".", r"\.").replace("*", ".*"), re.IGNORECASE)
                )
            self._code_patterns[item_id] = patterns

        logger.info(f"Loaded Section O mappings: {len(self.mappings)} items")

    def map_code_to_target_items(
        self, code: str, code_system: str
    ) -> list[str]:
        """
        Map a procedure code or item label to Section O items.

        Args:
            code: The code value — can be an ICD code, HCPCS code,
                  d_items label, or ordercategoryname.
            code_system: "ICD10_PCS", "ICD9_PROC", "HCPCS",
                         "icu_label", or "drug_name".

        Returns:
            List of matching Section O item IDs.
        """
        code_clean = code.strip()
        if not code_clean:
            return []

        matched_items = []

        if code_system in ("ICD10_PCS", "ICD9_PROC", "HCPCS"):
            # Match against ICD/HCPCS code patterns
            code_upper = code_clean.upper()
            for item_id, patterns in self._code_patterns.items():
                for pattern in patterns:
                    if pattern.fullmatch(code_upper):
                        matched_items.append(item_id)
                        break

        # Also try keyword matching (for labels, drug names, etc.)
        code_lower = code_clean.lower()
        for item_id, keywords in self._keyword_index.items():
            if item_id not in matched_items:
                for keyword in keywords:
                    if keyword in code_lower or code_lower in keyword:
                        matched_items.append(item_id)
                        break

        return matched_items


# ---------------------------------------------------------------------------
# Section O Adapter
# ---------------------------------------------------------------------------

class SectionOAdapter(IEAdapter):
    """
    MDS Section O (Special Treatments/Procedures) adapter for MIMIC-IV.

    Structured evidence is primary for Section O. Text evidence is
    secondary and used mainly for confirmation.
    """

    def __init__(self, config: IEConfig | None = None):
        self.config = config or get_config()
        self.paths = self.config.mimic_paths
        self._code_mapper = SectionOCodeMapper()
        self._temporal_resolver = MIMICTemporalResolver()
        self._fusion_policy = ClinicalFusionPolicy(
            code_without_text_confidence=0.7,   # structured is strong for procedures
            text_without_code_confidence=0.4,    # text-only procedure evidence is weaker
        )
        self._admissions_cache: pd.DataFrame | None = None
        self._d_items_cache: pd.DataFrame | None = None

    def get_extraction_schema(self) -> ExtractionSchema:
        """Return the extraction schema for Section O (treatments)."""

        prompt_template = ""
        prompt_path = Path(__file__).parent / "prompts" / "extraction_prompt.txt"
        if prompt_path.exists():
            prompt_template = prompt_path.read_text()

        return ExtractionSchema(
            task_name="MDS Section O: Special Treatments and Procedures",
            target_domain="clinical",
            entity_types=[
                EntityType(
                    name="treatment",
                    description=(
                        "A medical treatment, procedure, or therapeutic "
                        "intervention. Include IV medications, transfusions, "
                        "dialysis, respiratory support (BiPAP, CPAP, ventilator), "
                        "and other special treatments. Extract from any part of "
                        "the note."
                    ),
                    examples=[
                        "IV antibiotics", "blood transfusion", "hemodialysis",
                        "peritoneal dialysis", "BiPAP", "CPAP", "ventilator",
                        "IV heparin", "IV fluids", "vasopressors", "norepinephrine drip",
                        "dialysis", "packed red blood cells", "PRBC",
                    ],
                ),
            ],
            attributes=[
                AttributeDimension(
                    name="assertion",
                    labels=["affirmed", "negated", "uncertain"],
                    description=(
                        "Whether the treatment was actually performed/given "
                        "(affirmed), explicitly not performed (negated), "
                        "or uncertain. 'Patient received hemodialysis' → affirmed. "
                        "'No need for dialysis' → negated."
                    ),
                    default_label="affirmed",
                ),
                AttributeDimension(
                    name="temporality",
                    labels=["current", "historical"],
                    description=(
                        "Whether the treatment occurred during this "
                        "hospitalization (current) or only in the past "
                        "(historical). Focus on current treatments."
                    ),
                    default_label="current",
                ),
            ],
            target_items=self._get_target_items(),
            structured_sources=[
                StructuredSourceConfig(
                    source_name="procedures_icd",
                    code_column="icd_code",
                    code_system="ICD_PROC",
                    additional_columns=["seq_num", "icd_version"],
                ),
                StructuredSourceConfig(
                    source_name="hcpcsevents",
                    code_column="hcpcs_cd",
                    code_system="HCPCS",
                    additional_columns=["seq_num", "short_description"],
                ),
                StructuredSourceConfig(
                    source_name="inputevents",
                    code_column="ordercategoryname",
                    code_system="icu_label",
                    date_column="starttime",
                    date_end_column="endtime",
                    additional_columns=["itemid", "amount", "amountuom", "rate", "rateuom", "ordercategorydescription", "statusdescription"],
                ),
                StructuredSourceConfig(
                    source_name="procedureevents",
                    code_column="itemid_label",
                    code_system="icu_label",
                    date_column="starttime",
                    additional_columns=["itemid", "value", "valueuom", "statusdescription"],
                ),
            ],
            lookback=LookbackConfig(type="admission_window"),
            fusion_policy=FusionPolicyConfig(
                code_without_text_confidence=0.7,
                text_without_code_confidence=0.4,
            ),
            extraction_prompt_template=prompt_template,
        )

    def _get_target_items(self) -> list[TargetItem]:
        """Define the target items for Section O."""
        return [
            TargetItem(
                item_id="O0110H1", name="IV Medications",
                description="Any medication administered intravenously",
                keywords=["iv", "intravenous", "iv medication", "iv med", "iv push", "iv drip", "iv infusion"],
            ),
            TargetItem(
                item_id="O0110H2", name="IV Vasoactive Medications",
                description="Vasoactive medications given IV (vasopressors, inotropes)",
                keywords=["vasopressor", "vasoactive", "norepinephrine", "levophed", "dopamine", "dobutamine", "epinephrine", "vasopressin", "phenylephrine", "milrinone", "neosynephrine"],
            ),
            TargetItem(
                item_id="O0110H3", name="IV Antibiotics",
                description="Antibiotics administered intravenously",
                keywords=["iv antibiotic", "iv vancomycin", "iv ceftriaxone", "iv piperacillin", "iv meropenem", "iv cefepime", "iv cefazolin", "iv ampicillin", "iv zosyn", "iv metronidazole"],
            ),
            TargetItem(
                item_id="O0110H4", name="IV Anticoagulant",
                description="Anticoagulants administered intravenously (e.g., IV heparin)",
                keywords=["iv heparin", "heparin drip", "heparin infusion", "iv anticoagulant", "heparin gtt"],
            ),
            TargetItem(
                item_id="O0110H10", name="IV Other",
                description="Other IV medications not classified above",
                keywords=["iv fluids", "iv fluid", "normal saline", "lactated ringers", "iv push", "iv piggyback"],
            ),
            TargetItem(
                item_id="O0110I1", name="Transfusions",
                description="Blood product transfusions (PRBC, platelets, FFP, cryoprecipitate)",
                keywords=["transfusion", "prbc", "packed red blood cells", "platelets", "ffp", "fresh frozen plasma", "cryoprecipitate", "blood products", "blood transfusion"],
            ),
            TargetItem(
                item_id="O0110J1", name="Dialysis",
                description="Any form of dialysis (general)",
                keywords=["dialysis", "renal replacement therapy", "rrt", "crrt", "cvvh", "cvvhd", "cvvhdf"],
            ),
            TargetItem(
                item_id="O0110J2", name="Hemodialysis",
                description="Hemodialysis specifically",
                keywords=["hemodialysis", "hd", "intermittent hemodialysis", "ihd"],
            ),
            TargetItem(
                item_id="O0110J3", name="Peritoneal Dialysis",
                description="Peritoneal dialysis specifically",
                keywords=["peritoneal dialysis", "pd", "capd", "ccpd"],
            ),
            TargetItem(
                item_id="O0110G2", name="BiPAP",
                description="Bilevel positive airway pressure (BiPAP/BPAP)",
                keywords=["bipap", "bilevel", "bpap", "bilevel positive airway"],
            ),
            TargetItem(
                item_id="O0110G3", name="CPAP",
                description="Continuous positive airway pressure (CPAP)",
                keywords=["cpap", "continuous positive airway"],
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
        Load structured treatment/procedure data for the given episode.

        Loads from multiple sources:
        - procedures_icd: billed procedure codes
        - hcpcsevents: HCPCS procedure events
        - inputevents: ICU input events (IV meds, fluids, blood products)
        - procedureevents: ICU procedure events (ventilation, dialysis, etc.)
        """
        result: dict[str, pd.DataFrame] = {}

        # Load procedures_icd
        try:
            proc_df = load_table_for_episode(
                paths=self.paths,
                module="hosp",
                table_name="procedures_icd",
                episode_id=episode_id,
                episode_column="hadm_id",
                usecols=["subject_id", "hadm_id", "seq_num", "icd_code", "icd_version"],
                dtype={"icd_code": str},
            )
            # Set code system based on version
            if not proc_df.empty and "icd_version" in proc_df.columns:
                proc_df["code_system"] = proc_df["icd_version"].apply(
                    lambda v: "ICD10_PCS" if str(v) == "10" else "ICD9_PROC"
                )
            result["procedures_icd"] = proc_df
        except Exception as e:
            logger.warning(f"Failed to load procedures_icd: {e}")
            result["procedures_icd"] = pd.DataFrame()

        # Load hcpcsevents
        try:
            hcpcs_df = load_table_for_episode(
                paths=self.paths,
                module="hosp",
                table_name="hcpcsevents",
                episode_id=episode_id,
                episode_column="hadm_id",
                usecols=["subject_id", "hadm_id", "hcpcs_cd", "seq_num", "short_description"],
            )
            result["hcpcsevents"] = hcpcs_df
        except Exception as e:
            logger.warning(f"Failed to load hcpcsevents: {e}")
            result["hcpcsevents"] = pd.DataFrame()

        # Load ICU inputevents (need stay_id linkage)
        try:
            # First get stay_ids for this hadm_id
            stays_df = load_table_for_episode(
                paths=self.paths,
                module="icu",
                table_name="icustays",
                episode_id=episode_id,
                episode_column="hadm_id",
                usecols=["subject_id", "hadm_id", "stay_id"],
            )

            if not stays_df.empty:
                stay_ids = stays_df["stay_id"].tolist()
                # Load inputevents for those stays
                input_df = load_mimic_table(
                    paths=self.paths,
                    module="icu",
                    table_name="inputevents",
                    usecols=[
                        "subject_id", "hadm_id", "stay_id", "starttime",
                        "endtime", "itemid", "amount", "amountuom",
                        "rate", "rateuom", "ordercategoryname",
                        "ordercategorydescription", "statusdescription",
                    ],
                )
                input_df = input_df[input_df["stay_id"].isin(stay_ids)]
                result["inputevents"] = input_df
                logger.info(f"inputevents: {len(input_df)} records")
            else:
                result["inputevents"] = pd.DataFrame()
        except Exception as e:
            logger.warning(f"Failed to load inputevents: {e}")
            result["inputevents"] = pd.DataFrame()

        # Load ICU procedureevents
        try:
            if not stays_df.empty:
                proc_events_df = load_mimic_table(
                    paths=self.paths,
                    module="icu",
                    table_name="procedureevents",
                    usecols=[
                        "subject_id", "hadm_id", "stay_id", "itemid",
                        "starttime", "value", "valueuom", "statusdescription",
                    ],
                )
                proc_events_df = proc_events_df[
                    proc_events_df["stay_id"].isin(stay_ids)
                ]

                # Enrich with d_items labels
                d_items = self._get_d_items()
                if not d_items.empty:
                    proc_events_df = proc_events_df.merge(
                        d_items[["itemid", "label"]],
                        on="itemid",
                        how="left",
                    )
                    proc_events_df.rename(
                        columns={"label": "itemid_label"}, inplace=True
                    )
                else:
                    proc_events_df["itemid_label"] = ""

                result["procedureevents"] = proc_events_df
                logger.info(f"procedureevents: {len(proc_events_df)} records")
            else:
                result["procedureevents"] = pd.DataFrame()
        except Exception as e:
            logger.warning(f"Failed to load procedureevents: {e}")
            result["procedureevents"] = pd.DataFrame()

        return result

    def _get_d_items(self) -> pd.DataFrame:
        """Load and cache the ICU d_items lookup table."""
        if self._d_items_cache is None:
            try:
                self._d_items_cache = load_mimic_table(
                    paths=self.paths,
                    module="icu",
                    table_name="d_items",
                    usecols=["itemid", "label", "category"],
                )
            except Exception as e:
                logger.warning(f"Failed to load d_items: {e}")
                self._d_items_cache = pd.DataFrame()
        return self._d_items_cache

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
