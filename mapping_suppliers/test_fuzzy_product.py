from utils import fuzzy_matching
from utils import config, str_processing
import rapidfuzz.fuzz as fuzz
import rapidfuzz.process as process
from main import process_source1_item
import pandas as pd


SCORERS_ORIG = [fuzz.token_set_ratio, fuzz.ratio, fuzz.partial_ratio, fuzz.token_sort_ratio]
SCORERS_CLEANED = [fuzz.token_sort_ratio, fuzz.token_set_ratio]


cases = [
    # {
    #     "str_1": "LOTEPREDNOL ETABONATE",
    #     "str_2": "RED CONCENTRATE (20PPP-681)",
    #     "expected": False,
    # },
    # # {
    # #     "str_1": "TESTOSTERONE ENANTHATE",
    # #     "str_2": "TESTOSTERONE PROPIONATE",
    # #     "expected": False,
    # # },
    # {
    #     "str_1": "ACITRETIN",
    #     "str_2": "METHOXY-C15-WITTIG SALT (TGN05), INTERMEDIATE FOR ACITRETIN API",
    #     "expected": False,
    # },
    # {
    #     "str_1": "ETIDOCAINE HYDROCHLORIDE",
    #     "str_2": "DYCLONINE HCL",
    #     "expected": False,
    # },
    # {
    #     "str_1": "DIPHENHYDRAMINE CITRATE",
    #     "str_2": "DIPHENHYDRAMINE HCL",
    #     "expected": False,
    # },
    {
        "str_1": "AMIODARONE HYDROCHLORIDE",
        "str_2": "AMIODARONE HCL USP",
        "expected": True,
    },
    {
        "str_1": "MESALAZINE N",
        "str_2": "MESALAMINE",
        "expected": True,
    },
    {
        "str_1": "PSEUDOEPHEDRINE SULFATE",
        "str_2": "EPHEDRINE SULFATE",
        "expected": True,
    },
    {
        "str_1": "PRAZEPAM",
        "str_2": "LORAZEPAM",
        "expected": True,
    },
    {
        "str_1": "BUPIVACAINE",
        "str_2": "LEVOBUPIVACAINE",
        "expected": True,
    },
    {
        "str_1": "BUPIVACAINE",
        "str_2": "ROPIVACAINE",
        "expected": True,
    },
    {
        "str_1": "TROMETHAMINE",
        "str_2": "RS-37619-00-31-3 (RS-37619, TROMETHAMINE SALT)",
        "expected": True,
    },
    {
        "str_1": "BROMPHENIRAMINE MALEATE",
        "str_2": "BROMPHENIRAMINE MALEATE/PHENYLPROPANOLAMINE C.R. TABLET",
        "expected": True,
    },
    {
        "str_1": "IBRUTINIB",
        "str_2": "ZANUBRUTINIB",
        "expected": True,
    },
    {
        "str_1": "FORMOTEROL FUMARATE DIHYDRATE",
        "str_2": "FORMOTEROL FUMARATE",
        "expected": True,
    },
    {
        "str_1": "ACETYLCYSTEINE",
        "str_2": "L-CYSTEINE",
        "expected": True,
    },
    {
        "str_1": "XENON XE-133",
        "str_2": "XENON -133 GAS",
        "expected": True,
    },
    {
        "str_1": "ACETAMINOPHEN",
        "str_2": "ACETAMINOFEN",
        "expected": True,
    },
    {
        "str_1": "ADENOSINE USP",
        "str_2": "Adenosine",
        "expected": True,
    },
    {
        "str_1": "Adenosine USP",
        "str_2": "adenosine",
        "expected": True,
    },
    {
        "str_1": "Adenosine USP",
        "str_2": "Dextromethorphan Hydrobromid, Adenosin",
        "expected": True,
    },
    {
        "str_1": "Adenosine",
        "str_2": "Dextromethorphan Hydrobromid, Adenosin",
        "expected": True,
    },
    {
        "str_1": "Adenosine",
        "str_2": "adenosine, usp",
        "expected": True,
    },
    {
        "str_1": "Adenosine",
        "str_2": "dextromethorphan hbr,adenosine (inj. grade)",
        "expected": True,
    },

]

for case in cases:
    str_1 = str_processing.cleaning_id(case["str_1"])
    str_2 = str_processing.cleaning_id(case["str_2"])
    expected = case["expected"]
    mapping_config = config.MappingConfig(
        source_1_filename="ob.csv",
        source_1_id="Ingredient",
        source_1_prefix="OB",
        source_1_separator=";",
        source_2_filename="us_dmf.csv",
        source_2_id="SUBJECT",
        source_2_prefix="US_DMF",
        source_2_separator=None,
        mapping_name="substance_orange_book_to_usdmf",
        mapping_type=config.MappingType.SUBSTANCE,
        fuzzy_match_threshold=70,
        cleaned_fuzzy_match_threshold=60,
        request_item_size=100,
        batch_size=5000,
    )

    ff = (
        0,
        str_1,
        fuzzy_matching.clean_product_name_aggressively_pharma(str_1),
        'intermediate' in fuzzy_matching.clean_product_name_aggressively_pharma(str_1)
    )

    source_1_data_tuples = list(ff)
    source_2_data = {
        'orig_list': [[str_2]],
        'cleaned_list': [fuzzy_matching.clean_product_name_aggressively_pharma(str_2)],
        'indices': [0], # Index of row in df_unique_source_2
        'index_to_orig': { 0: str_2 }, # Map df index to orig name
        'has_intermediate_list': ['intermediate' in fuzzy_matching.clean_product_name_aggressively_pharma(str_2)]
    }

    source_1_length = len(source_1_data_tuples); source_2_length = len(source_2_data['orig_list'])
    # print(f"Prepared {source_1_length} Source 1 items and {source_2_length} Source 2 items.")
    

    # --- Parallel Processing ---
    mappings_candidate = process_source1_item(
        source_1_data_tuples,
        source_2_data,
        mapping_config,
    )
    matching_result = False 
    
    if mappings_candidate is not None and len(mappings_candidate) > 0 and mappings_candidate[0] is not None and mappings_candidate[0]['item_1'] == str_1 and mappings_candidate[0]['item_2'] == str_2:
        matching_result = True
    
    assert matching_result == expected, f"Expected {expected} but got {matching_result} for {str_1} and {str_2}"