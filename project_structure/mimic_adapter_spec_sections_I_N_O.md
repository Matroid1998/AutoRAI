# Concrete MIMIC Adapter Spec for MDS Sections I, N, and O

This document provides a concrete **Layer-2 MIMIC adapter specification** for a first implementation of the Information Extraction (IE) component for MDS form completion. The goal is to define exactly which MIMIC tables to use, which fields to keep as evidence, and which first-pass subset of MDS items to target for Sections **I**, **N**, and **O**.

This adapter is designed to fit the two-layer IE architecture described in the main system design: the **Layer-1 core** remains schema-driven and domain-agnostic, while this MIMIC adapter supplies MIMIC-specific tables, field mappings, heuristics, and item subsets. The adapter should be version-locked to **MIMIC-IV v3.1**, **MIMIC-IV-Note v2.2**, and a single CMS MDS manual version. MIMIC-IV is split into `hosp` and `icu`, while note data is provided in a separate module and linked through `subject_id`, `hadm_id`, and aligned deidentified timestamps. ([PhysioNet MIMIC-IV](https://physionet.org/content/mimiciv/))

---

## 1. Global Adapter Settings

### 1.1 Episode and Patient Keys

For Sections I, N, and O, use:

- **`hadm_id`** as the primary episode key
- **`subject_id`** as the patient key
- **`stay_id`** only when using ICU-specific evidence from tables such as `inputevents` or `procedureevents`

Use the hospitalization window from:

- `admissions.admittime`
- `admissions.dischtime`

The `admissions` table includes fields such as `subject_id`, `hadm_id`, `admittime`, `dischtime`, `deathtime`, `admission_type`, `admission_location`, and `discharge_location`. ([MIMIC-IV demo admissions schema](https://physionet.org/content/mimic-iv-demo/2.2/hosp/admissions.csv.gz))

### 1.2 Primary Unstructured Text Source

For the first version, use **`note.discharge`** as the main unstructured source.

The MIMIC-IV note module contains four note-related tables, but `discharge` is the most useful for this project because discharge summaries contain the most consolidated narrative evidence for diagnosis, medications, treatments, and hospital course. Discharge summaries typically include sections such as:

- Chief Complaint
- History of Present Illness (HPI)
- Past Medical History (PMH)
- Brief Hospital Course
- Physical Exam
- Discharge Diagnoses

([MIMIC-IV-Note](https://www.physionet.org/content/mimic-iv-note/2.2/))

For v1, keep these fields from `discharge`:

- `note_id`
- `subject_id`
- `hadm_id`
- `note_type`
- `note_seq`
- `charttime`
- `storetime`
- `text`

For the first iteration, `discharge_detail` can be ignored unless a later need emerges.

### 1.3 Adapter Philosophy

This MIMIC adapter should be viewed as a **proxy-domain adapter**, not as the definition of the full project. MIMIC is a hospital dataset, while MDS is designed for long-term care and post-acute assessment workflows. Therefore, the first implementation should target only the subset of MDS items that can be observed with reasonable fidelity from hospital notes and hospital structured data.

---

## 2. Section I Adapter: Active Diagnoses

Section I is the cleanest and strongest entry point for the MIMIC adapter.

### 2.1 Primary Structured Tables

Use the following diagnosis tables:

- **`hosp.diagnoses_icd`**
  - `subject_id`
  - `hadm_id`
  - `seq_num`
  - `icd_code`
  - `icd_version`

- **`hosp.d_icd_diagnoses`**
  - `icd_code`
  - `icd_version`
  - `long_title`

The official MIMIC documentation states that `diagnoses_icd` contains diagnoses billed for the hospitalization, assigned after review of signed notes. The `seq_num` field indicates rough ordering or relative importance, but it should **not** be treated as a gold measure of clinical salience. ([diagnoses_icd docs](https://raw.githubusercontent.com/MIT-LCP/mimic-iv-website/master/content/hosp/diagnoses_icd.md))

### 2.2 Primary Text Sections

Use spans from discharge summaries, especially from:

- **Discharge Diagnoses**
- **Brief Hospital Course**
- **History of Present Illness**
- **Past Medical History**

These sections help with activeness, temporality, and assertion. Mentions in `Past Medical History` are more likely historical, while mentions in `Brief Hospital Course` or `Discharge Diagnoses` are more likely to support active coding.

### 2.3 Evidence Fields to Keep

For the **unstructured path**, store:

- `note_id`
- `hadm_id`
- `section_name`
- `span_text`
- `span_start`
- `span_end`
- `mention_text`
- `assertion` = affirmed / negated / uncertain
- `temporality` = current / historical
- `candidate_icd_code` or `null`
- `confidence`

For the **structured path**, store:

- `hadm_id`
- `icd_code`
- `icd_version`
- `long_title`
- `seq_num`
- `source = diagnoses_icd`

### 2.4 First-Pass Section I Item Subset

A good first-pass subset is the set of conditions that are:

1. common in MIMIC,
2. visible in both ICD codes and discharge summaries,
3. feasible to map to MDS without a massive crosswalk effort.

Recommended initial Section I item subset:

- `I0200` Anemia
- `I0300` Atrial fibrillation and other dysrhythmias
- `I0400` Coronary artery disease
- `I0600` Heart failure
- `I0700` Hypertension
- `I0900` Peripheral vascular disease / PAD
- `I2900` Diabetes mellitus
- `I4500` CVA / TIA / stroke
- `I4900` Hemiplegia or hemiparesis
- `I6200` Asthma / COPD / chronic lung disease

([CMS MDS item matrix](https://www.cms.gov/files/document/finalmds-30-item-matrix-v1201v3october2025.pdf))

### 2.5 Section I Notes for Implementation

For v1, the adapter should not try to solve the full MDS notion of “active diagnosis” perfectly. Instead:

- ICD presence provides **candidate silver evidence**
- discharge note mention provides **assertion and temporality refinement**
- fusion rules decide whether structured and text evidence agree or conflict

That makes Section I a natural first section for implementing dual-path IE.

---

## 3. Section N Adapter: Medications

Section N is a strong second target because MIMIC provides both medication orders and medication administrations.

### 3.1 Primary Structured Tables

Use:

- **`hosp.prescriptions`**
  - `subject_id`
  - `hadm_id`
  - `pharmacy_id`
  - `starttime`
  - `stoptime`
  - `drug_type`
  - `drug`
  - `gsn`
  - `ndc`
  - `prod_strength`
  - `dose_val_rx`
  - `dose_unit_rx`
  - `doses_per_24_hrs`
  - `route`

- **`hosp.pharmacy`**
  - `subject_id`
  - `hadm_id`
  - `pharmacy_id`
  - `starttime`
  - `stoptime`
  - `medication`
  - `proc_type`
  - `status`
  - `entertime`
  - `verifiedtime`
  - `route`
  - `frequency`
  - `infusion_type`
  - `doses_per_24_hrs`
  - `poe_id`

- **`hosp.emar`**
  - `subject_id`
  - `hadm_id`
  - `emar_id`
  - `emar_seq`
  - `poe_id`
  - `charttime`
  - `medication`
  - `event_txt`
  - `scheduletime`
  - `storetime`

- **`hosp.emar_detail`**
  - `subject_id`
  - `emar_id`
  - `emar_seq`
  - `administration_types`
  - `pharmacy_id`
  - `Dose_Due`
  - `Dose_Given`
  - `Dose_Given_Unit`
  - `Product_Code`
  - `Product_Description`
  - `Infusion_Rate`
  - `Infusion_Rate_Units`
  - `Route`

- **`hosp.poe`**
  - `poe_id`
  - `poe_seq`
  - `subject_id`
  - `hadm_id`
  - `ordertime`
  - `order_type`
  - `order_subtype`
  - `transaction_type`
  - `order_status`

- **`hosp.poe_detail`**
  - `poe_id`
  - `poe_seq`
  - `subject_id`
  - `field_name`
  - `field_value`

([prescriptions docs](https://raw.githubusercontent.com/MIT-LCP/mimic-iv-website/master/content/hosp/prescriptions.md))

### 3.2 Medication Evidence Logic

For v1, prefer the following logic:

- Use **`emar` / `emar_detail`** as primary evidence of actual administration
- Use **`prescriptions` / `pharmacy`** as secondary evidence of medication orders
- Use **`poe` / `poe_detail`** to recover additional order metadata and support for route or IV classification

Practical rules:

- treat `emar.event_txt` values such as **Administered** or **Applied** as positive evidence
- treat values such as **Not Given** or **Delayed** as non-positive or lower-confidence evidence
- use `emar.charttime` as the main medication event timestamp
- use `emar_detail.Route` and `administration_types` to distinguish IV / infusion / oral / patch patterns
- backfill from order tables when eMAR is absent or weak, especially for older admissions

The MIMIC paper notes that medication administration capture becomes much stronger after 2016 because the hospital-wide eMAR rollout was completed by then.

### 3.3 Primary Text Source

Use `discharge.text` as the main unstructured medication source, especially sections mentioning discharge medications or medication changes in the hospital course.

For v1, note-based medication extraction should be treated as **secondary evidence**, mostly to:

- resolve ambiguous class assignments,
- support missing administration records,
- provide evidence when structured med coverage is incomplete.

### 3.4 First-Pass Section N Item Subset

For a first implementation, target **“Has received / Is taking”** medication-class items only, not the harder “Indication noted” items.

Recommended initial Section N subset:

- `N0415A1` Antipsychotic: Has received
- `N0415B1` Antianxiety: Has received
- `N0415C1` Antidepressant: Has received
- `N0415D1` Hypnotic: Has received
- `N0415E1` Anticoagulant: Has received
- `N0415F1` Antibiotic: Has received
- `N0415G1` Diuretic: Has received
- `N0415H1` Opioid: Has received
- `N0415I1` Antiplatelet: Has received
- `N0415J1` Hypoglycemic: Has received
- `N0415K1` Anti-convulsant: Has received
- `N0350A` Insulin injections
- `N0350B` Orders for insulin

([CMS MDS item matrix](https://www.cms.gov/files/document/finalmds-30-item-matrix-v1201v3october2025.pdf))

### 3.5 Section N Notes for Implementation

The key modeling distinction is:

- **received/administered** versus
- **ordered/prescribed**

This is especially important for MDS-like coding because actual medication receipt often matters more than mere order presence. The adapter should preserve both signals separately so the fusion layer can choose how to weigh them.

---

## 4. Section O Adapter: Special Treatments / Procedures / Programs

Section O is usable in MIMIC only if scoped carefully. For the first pass, target only treatments and procedures that are strongly supported by structured hospital or ICU evidence.

### 4.1 Primary Structured Tables

Use:

- **`hosp.procedures_icd`**
  - `subject_id`
  - `hadm_id`
  - `seq_num`
  - `icd_code`
  - `icd_version`

- **`hosp.d_icd_procedures`**
  - `icd_code`
  - `icd_version`
  - `long_title`

- **`hosp.hcpcsevents`**
  - `subject_id`
  - `hadm_id`
  - `hcpcs_cd`
  - `seq_num`
  - `short_description`

- **`hosp.poe`**
  - `poe_id`
  - `poe_seq`
  - `subject_id`
  - `hadm_id`
  - `ordertime`
  - `order_type`
  - `order_subtype`
  - `transaction_type`
  - `order_status`

- **`hosp.poe_detail`**
  - `poe_id`
  - `poe_seq`
  - `subject_id`
  - `field_name`
  - `field_value`

- **`icu.inputevents`**
  - `subject_id`
  - `hadm_id`
  - `stay_id`
  - `starttime`
  - `endtime`
  - `itemid`
  - `amount`
  - `amountuom`
  - `rate`
  - `rateuom`
  - `ordercategoryname`
  - `ordercategorydescription`
  - `statusdescription`

- **`icu.procedureevents`**
  - `subject_id`
  - `hadm_id`
  - `stay_id`
  - `itemid`
  - `charttime`
  - `value`
  - `valuenum`
  - `valueuom`
  - `location`
  - `statusdescription`

- **`icu.d_items`**
  - `itemid`
  - `label`
  - `abbreviation`
  - `linksto`
  - `category`
  - `unitname`
  - `param_type`

([procedures_icd docs](https://raw.githubusercontent.com/MIT-LCP/mimic-iv-website/master/content/hosp/procedures_icd.md))

### 4.2 Primary Unstructured Source

Use discharge summaries only as **supporting evidence** for Section O in v1. Structured procedure/order/event tables should be primary for this section.

Text can help in cases such as:

- explicit narrative mention of dialysis,
- mention of transfusion,
- mention of respiratory support,
- confirmation of IV antibiotic treatment.

### 4.3 First-Pass Section O Item Subset

Recommended first-pass subset:

- `O0110H1` IV medications
- `O0110H2` IV vasoactive medications
- `O0110H3` IV antibiotics
- `O0110H4` IV anticoagulant
- `O0110H10` IV other
- `O0110I1` Transfusions
- `O0110J1` Dialysis
- `O0110J2` Hemodialysis
- `O0110J3` Peritoneal dialysis
- `O0110G2` BiPAP
- `O0110G3` CPAP
- `O0110C1` Oxygen therapy (optional in phase 1.5, not the first core target)

([CMS MDS item matrix update](https://www.cms.gov/files/document/final-mds-3-0-item-matrix-v1-20-1v4-update-j1900-october-2025.pdf))

### 4.4 Section O Notes for Implementation

For MIMIC, do **not** try to model the full LTC-style admission/while-resident/discharge subcolumns for all Section O items in the first version. Instead, create hospitalization-level concept labels such as:

- dialysis occurred during this admission,
- IV antibiotics occurred during this admission,
- transfusion occurred during this admission.

This is much more defensible as a proxy benchmark. Later, once real home-care or actual MDS-linked data is available, this adapter can be extended to support finer-grained MDS temporal columns.

---

## 5. Recommended Common Output Format for the Adapter

To make the MIMIC adapter usable by the Layer-1 IE core, all extracted evidence should be emitted in a common normalized record structure.

For every candidate MDS item, output:

- `mds_item_id`
- `episode_id = hadm_id`
- `patient_id = subject_id`
- `source_type` = text / diagnosis_code / med_admin / med_order / procedure_code / icu_event / order
- `source_table`
- `source_row_key` (for example `note_id`, `emar_id`, `poe_id`, `itemid`)
- `event_time_start`
- `event_time_end`
- `raw_value`
- `normalized_value`
- `assertion`
- `temporality`
- `route`
- `dose_or_amount`
- `confidence`
- `provenance_text`

This output schema allows the Layer-1 fusion logic to remain domain-agnostic while still preserving enough provenance and structured detail for downstream verification.

---

## 6. Recommended v1 Implementation Scope

For a first implementation, the most realistic and high-value scope is:

### Section I

- `I0200`
- `I0300`
- `I0400`
- `I0600`
- `I0700`
- `I0900`
- `I2900`
- `I4500`
- `I4900`
- `I6200`

### Section N

- `N0415A1`
- `N0415B1`
- `N0415C1`
- `N0415D1`
- `N0415E1`
- `N0415F1`
- `N0415G1`
- `N0415H1`
- `N0415I1`
- `N0415J1`
- `N0415K1`
- `N0350A`
- `N0350B`

### Section O

- `O0110H1`
- `O0110H2`
- `O0110H3`
- `O0110H4`
- `O0110H10`
- `O0110I1`
- `O0110J1`
- `O0110J2`
- `O0110J3`

This gives a manageable first benchmark that is both technically meaningful and reasonably supported by MIMIC.

---

## 7. Final Recommendation

The first implementation should not try to solve “full MDS completion from MIMIC.” Instead, it should be positioned as:

**a concrete MIMIC adapter for a subset of Sections I, N, and O that are well-supported by hospital structured data and discharge summaries.**

That framing is honest, technically strong, and consistent with the larger research goal of building a reusable, specification-grounded structured prediction pipeline whose core is independent of any one dataset.
