
from enum import Enum
from pydantic import BaseModel

SUBSTANCE_PROMPT = """
You are an expert in the pharmaceutical industry and are reviewing the properties of two active substances:
For each of the provided mappings, determine if active_substance_1 and active_substance_2 :
- Have the same base molecule.
- Have the same complete form.
- Is one of the substances is a diluted form of the other.
Example:
- active_substance_1: "NAPROXEN SODIUM", active_substance_2: "Naproxen Na" -> same_base: true, same_form: true, is_diluted: false
- active_substance_1: "NAPROXEN SODIUM", active_substance_2: "Naproxeno sodico" -> same_base: true (assuming it's just Spanish name), same_form: true, is_diluted: false
- active_substance_1: "NAPROXEN SODIUM", active_substance_2: "Naproxeno HYDROCHLORIDE" -> same_base: true, same_form: false, is_diluted: false
- active_substance_1: "NAPROXEN SODIUM", active_substance_2: "Naproxen base" -> same_base: false, same_form: false, is_diluted: false
- active_substance_1: "NAPROXEN SODIUM", active_substance_2: "Ibuprofen" -> same_base: false, same_form: false, is_diluted: false
- active_substance_1: "NAPROXEN SODIUM", active_substance_2: "Ketorolac" -> same_base: false, same_form: false, is_diluted: false
- active_substance_1: "NAPROXEN SODIUM", active_substance_2: "Naproxen Sodium Monohydrate" -> same_base: true, same_form: false, is_diluted: false (it's a different form of the same base)
- active_substance_1: "NAPROXEN SODIUM", active_substance_2: "Naproxene Sódica" -> same_base: true (assuming it's just a different spelling / language for Naproxen Sodium), same_form: true, is_diluted: false
- active_substance_1: "Docetaxel", active_substance_2: "Anhydrous Docetaxel" -> same_base: true, same_form: false, is_diluted: false
- active_substance_1: "Pemetrexed", active_substance_2: "Pemetrexed disodium 2.5-hydrate" -> same_base: true, same_form: false, is_diluted: false
- active_substance_1: "Zoledronic Acid Monohydrate", active_substance_2: "Zoledronic acid monohydrate" -> same_base: true, same_form: true, is_diluted: false
- active_substance_1: "Urea (13C)", active_substance_2: "13c-Urea" -> same_base: true, same_form: true, is_diluted: false
- active_substance_1: "Naproxeno HYDROCHLORIDE", active_substance_2: "Naproxeno HCL" -> same_base: true, same_form: true, is_diluted: false
- active_substance_1: "Naproxeno HYDROCHLORIDE", active_substance_2: "Naproxeno NA" -> same_base: true, same_form: false, is_diluted: false
- active_substance_1: "Acidum Picrinicum D4", active_substance_2: "Acidum picrinicum for homoeopathic preparations" -> same_base: true, same_form: false, is_diluted: true
- active_substance_1: "Docetaxel", active_substance_2: "Docetaxel USP" -> same_base: true, same_form: true, is_diluted: false (assuming USP, United States Pharmacopeia, is just a standard)
"""

SUPPLIER_PROMPT = """
You are a pharmaceutical industry analysis AI. Your task is to process provided mappings, each containing `item_1` and `item_2`. For each mapping, follow the steps below precisely to determine the required output values.

**--- Definitions (Apply in Steps 1 & 2) ---**

* **Supplier Site:** A specific manufacturing plant/facility/named location (e.g., "Pfizer Ringaskiddy," "Site B", "Plant Y").
* **Supplier Company:** The overall corporate/legal entity or potentially a distinct regional branch/subsidiary (e.g., "Pfizer Inc.," "Bayer AG", "Agno China").
* **Ambiguity Rule (Option A):** If a name *clearly* indicates a specific physical plant or facility using terms like 'Plant', 'Site', 'Facility', or a specific known plant name (e.g., 'Pfizer Ringaskiddy'), classify it as **Supplier Site**. If a name includes a geographic term but lacks specific site language (e.g., 'Agno China', 'Bayer Hispania'), lean towards classifying it as **Supplier Company** representing regional operations, unless the context strongly implies a single site.

**--- Analysis Steps per Mapping ---**

**Input:** Each step applies to a single mapping containing `item_1` (string) and `item_2` (string).

**Step 1: Classify `item_1`**

* Analyze `item_1` using the Definitions.
* **Determine:** The value for `is_item_1_supplier_site` (boolean: `true` if Supplier Site, `false` otherwise). Record this value.

**Step 2: Classify `item_2`**

* Analyze `item_2` using the Definitions.
* **Determine:** The value for `is_item_2_supplier_site` (boolean: `true` if Supplier Site, `false` otherwise). Record this value.

**Step 3: Assess Site Equivalence**

* Using the results from Step 1 and Step 2 (`is_item_1_supplier_site`, `is_item_2_supplier_site`), assess the likelihood that `item_1` and `item_2` represent the exact same physical site.
* *Constraint:* A high score (e.g., > 0.9) is only possible if *both* boolean results from Steps 1 & 2 are `true` *and* evidence confirms they represent the same location. Otherwise, the score must be low (e.g., <= 0.1).
* **Determine:** The value for `confidence_score_match_site_level` (float: 0.0 to 1.0). Record this value.

**Step 4: Assess Shared Company Ownership**

* Assess the likelihood `item_1` and `item_2` belong to the same company group, focusing on the ultimate parent entity.

* **Step 4a: Identify Core Company Names:** For both `item_1` and `item_2`, identify the primary **Core Company Name**. This generally involves mentally (or actually) stripping away the following elements for comparison purposes:
    * Common legal suffixes (see list in 4b).
    * Explicit site/plant designators (e.g., '- Site X', 'Plant Y', ', Location Z').
    * **Common Geographic Prefixes:** City, province, or state names appearing *at the beginning* of the name (e.g., strip 'Chongqing ' from 'Chongqing United...', strip 'Jiangsu ' from 'Jiangsu Hengrui...').
    * *(Note:* Geographic terms used as suffixes, potentially indicating a regional entity like 'Agno China', are generally *retained* as part of the Core Company Name for comparison in this step, unlike prefixes).*

* **Step 4b: Check for High Score (> 0.9):** Compare the **Core Company Names** identified in 4a. Assign **> 0.9** if EITHER of the following is true:
    * i. External evidence strongly confirms they share the **same ultimate parent** corporate entity.
    * ii. The **Core Company Names** are **virtually identical**. This means they match exactly or differ *only* by:
        * Common legal suffixes (e.g., Inc, Ltd, LLC, GmbH, AG, SL, SAS, Pvt Ltd, Co Ltd, Corp, PLC).
        * Common abbreviations or variations (e.g., Pharma/Pharmaceuticals, Labs/Laboratories, Chem/Chemicals, Intl/International).
        * Minor variations in punctuation, capitalization, or spacing.
    * **Crucial Clarification:** If the Core Company Names are deemed virtually identical under ii), differences in the *original strings* related ONLY to **legal suffixes** OR **explicit site/plant designators** MUST be **ignored** for this High Score check. (E.g., 'ALKALOIDS CORP' vs 'Alkaloids Private Limited - Site Medchal' -> Core 'Alkaloids' matches -> High Score).
    * If High Score assigned, proceed to Step 5.

* **Step 4c: Check for Low Score (< 0.3):** If High Score conditions (4b) were not met, check for Low Score conditions. Assign **< 0.3** if ANY of the following are true:
    * i. Evidence confirms **different** ultimate parent companies.
    * ii. The **Core Company Names** (from 4a) have **significantly different primary distinguishing keywords** (e.g., Core 'United Pharmaceutical' vs Core 'Carelife Pharmaceutical'; Core 'Teva Pharm' vs Core 'Actavis Pharm'). Do *not* trigger this solely based on differences explicitly ignored in 4b(ii).
    * iii. The **primary similarity relies *solely* on shared Geographic Prefixes** (like 'Chongqing'), generic industry terms ('Pharmaceutical'), or ambiguous regional terms ('Polfa'), AND the **Core Company Names (or other key distinguishing parts like 'United' vs 'Carelife') are clearly different**. This condition strongly applies when a Geographic Prefix is the main link.
    * **Caveat Emphasis:** The condition in 4c(iii) targets cases where similarity is superficial (like sharing a city name prefix) while the core identities differ. It does **NOT** apply if the Core Company Names themselves are identical or highly similar (as defined in 4b).
    * If Low Score assigned, proceed to Step 5.

* **Step 4d: Assign Intermediate Score (0.3 - 0.9):** If neither High Score (4b) nor Low Score (4c) conditions were clearly met, assign an intermediate score based on the following:
    * **Assign specifically 0.7** if the **Core Company Names (from 4a) match well** (identical or near-identical base), but the original names failed the 'virtually identical' test (4b ii) primarily due to **added or missing secondary descriptors or identifiers**. Examples include:
        * Missing industry type (e.g., "PHARMA" in 'AGNO PHARMA' vs 'Agno China').
        * Added project/division/collaboration identifier (e.g., "-Odyssea" in 'ABBVIE INC' vs 'AbbVie-Odyssea').
        * Presence of a geographic suffix that denotes a potential regional entity (e.g., 'China' in 'Agno China').
        * Missing standard corporate identifiers like 'INC' if not already stripped as a legal suffix.
        *(This score reflects a judged high likelihood of relation where one name appears to be a variant or sub-unit of the other, despite name differences preventing a >0.9 score based on name alone).*
    * Assign **other scores within the 0.3 - 0.9 range** for different kinds of intermediate similarities where the connection is less direct than the 0.7 scenario (e.g., comparing 'Acme Pharma' vs 'Acme Labs' might warrant 0.6, or cases with weaker partial matches might be closer to 0.3-0.4). The specific score requires judgment based on the degree and nature of the similarity.
* **Determine:** The value for `confidence_score_are_part_of_same_company` (float: 0.0 to 1.0). Record this value.

**Step 5: Consolidate and Finalize Output Values**

* Gather the following seven values for the mapping being processed:
    1.  `item_1`: The original input `item_1`.
    2.  `is_item_1_supplier_site`: The boolean value determined in Step 1.
    3.  `item_2`: The original input `item_2`.
    4.  `is_item_2_supplier_site`: The boolean value determined in Step 2.
    5.  `confidence_score_match_site_level`: The float value determined in Step 3.
    6.  `confidence_score_are_part_of_same_company`: The float value determined in Step 4.
* These seven values constitute the complete output required for the current mapping. (The surrounding JSON format will be handled externally).

**--- End of Steps ---**

Repeat these steps for each input mapping provided.
"""

class SubstanceMapping(BaseModel):
    active_substance_1: str
    active_substance_2: str
    have_same_base: bool
    have_same_form: bool
    is_diluted: bool

class SubstanceMappings(BaseModel):
    mappings: list[SubstanceMapping]

class SupplierMapping(BaseModel):
    item_1: str
    is_item_1_supplier_site: bool
    item_2: str
    is_item_2_supplier_site: bool
    confidence_score_match_site_level: float
    confidence_score_are_part_of_same_company: float

class SupplierMappings(BaseModel):
    mappings: list[SupplierMapping]


class MappingType(Enum):
    SUBSTANCE = "Substance"
    SUPPLIER = "Supplier"
    MERGE = "Merge"


class MappingConfig:
    def __init__(self,
        source_1_filename: str, source_1_id: str, source_1_prefix: str, source_1_separator: str,
        source_2_filename: str, source_2_id: str, source_2_prefix: str, source_2_separator: str,
        mapping_name: str, mapping_type: MappingType, fuzzy_match_threshold: float,
        cleaned_fuzzy_match_threshold: float, request_item_size: int, batch_size: int
    ):
        self.source_1_filename = source_1_filename
        self.source_1_id = source_1_id
        self.source_1_id_cleaned = f"{source_1_id}_cleaned"
        self.source_1_prefix = source_1_prefix
        self.source_1_separator = source_1_separator
        self.source_2_filename = source_2_filename
        self.source_2_id = source_2_id
        self.source_2_id_cleaned = f"{source_2_id}_cleaned"
        self.source_2_prefix = source_2_prefix
        self.source_2_separator = source_2_separator
        self.mapping_name = mapping_name
        self.mapping_type = mapping_type
        self.mapping_output_filename = f"{mapping_name}_mapping.csv"
        self.merge_output_filename_prefix = f"{mapping_name}_merged"
        self.fuzzy_match_threshold = fuzzy_match_threshold
        self.cleaned_fuzzy_match_threshold = cleaned_fuzzy_match_threshold
        self.request_item_size = request_item_size
        self.batch_size = batch_size
        if mapping_type == MappingType.SUBSTANCE:
            self.system_prompt = SUBSTANCE_PROMPT
            self.response_format = SubstanceMappings
            self.source_1_mapping_column = f"{source_1_prefix}_mapped_substance"
            self.source_2_mapping_column = f"{source_2_prefix}_mapped_substance"
        elif mapping_type == MappingType.SUPPLIER:
            self.system_prompt = SUPPLIER_PROMPT
            self.response_format = SupplierMappings
            self.source_1_mapping_column = f"{source_1_prefix}_mapped_supplier"
            self.source_1_is_supplier_site_column = f"{source_1_prefix}_is_supplier_site"
            self.source_2_mapping_column = f"{source_2_prefix}_mapped_supplier"
            self.source_2_is_supplier_site_column = f"{source_2_prefix}_is_supplier_site"
        else:
            self.system_prompt = None
            self.response_format = None
        
        if mapping_type in [MappingType.SUBSTANCE, MappingType.SUPPLIER]:
            self.source_1_mapping_column_cleaned = f"{self.source_1_mapping_column}_cleaned"
            self.source_2_mapping_column_cleaned = f"{self.source_2_mapping_column}_cleaned"


# print(MappingType.SUBSTANCE.value)

possible_mappings : list[MappingConfig] = [
    MappingConfig(
        source_1_filename="ob_cleaned.csv",
        source_1_id="Ingredient",
        source_1_prefix="OB",
        source_1_separator=None,
        source_2_filename="usdmf_cleaned.csv",
        source_2_id="SUBJECT",
        source_2_prefix="US_DMF",
        source_2_separator=None,
        mapping_name="substance_orange_book_to_usdmf",
        mapping_type=MappingType.SUBSTANCE,
        fuzzy_match_threshold=70,
        cleaned_fuzzy_match_threshold=80,
        request_item_size=100,
        batch_size=5000,
    ),
    MappingConfig(
        source_1_filename="a57_cleaned.csv",
        source_1_id="Active_substance",
        source_1_prefix="A57",
        source_1_separator=None,
        source_2_filename="cep_cleaned.csv",
        source_2_id="englishName",
        source_2_prefix="CEP",
        source_2_separator=None,
        mapping_name="substance_a57_to_cep",
        mapping_type=MappingType.SUBSTANCE,
        fuzzy_match_threshold=70,
        cleaned_fuzzy_match_threshold=80,
        request_item_size=100,
        batch_size=5000,
    ),
    MappingConfig(
        source_1_filename="public_manufacturer_required_apis.csv",
        source_1_id="manufacturer_required_api",
        source_1_prefix="PUBLIC",
        source_1_separator=None,
        source_2_filename="qf_supplier_sites_products_cleaned.csv",
        source_2_id="qf_supplier_site_audited_requested_product",
        source_2_prefix="QF",
        source_2_separator=None,
        mapping_name="substance_public_manufacturer_to_qf",
        mapping_type=MappingType.SUBSTANCE,
        fuzzy_match_threshold=70,
        cleaned_fuzzy_match_threshold=80,
        request_item_size=50,
        batch_size=5000,
    ),
    MappingConfig(
        source_1_filename="public_supplier_names.csv",
        source_1_id="supplier_name",
        source_1_prefix="PUBLIC",
        source_1_separator=None,
        source_2_filename="qf_supplier_sites_names_cleaned.csv",
        source_2_id="qf_supplier_site_name",
        source_2_prefix="QF",
        source_2_separator=None,
        mapping_name="supplier_public_to_qf",
        mapping_type=MappingType.SUPPLIER,
        fuzzy_match_threshold=50,
        cleaned_fuzzy_match_threshold=50,
        request_item_size=100,
        batch_size=5000,
    )
]

def get_mapping_config(mapping_name: str) -> MappingConfig:
    """
    Get the mapping configuration for a given mapping name.
    """
    for mapping in possible_mappings:
        if mapping.mapping_name == mapping_name:
            return mapping
    raise ValueError(f"Mapping '{mapping_name}' not found.")

if __name__ == "__main__":
    pass
