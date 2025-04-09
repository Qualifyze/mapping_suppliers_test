import re
import rapidfuzz.fuzz as fuzz
from utils import config # Assuming config defines MappingType (e.g., enum)
import logging

# Configure basic logging for this module if needed
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Pre-compile Regex patterns ---
_parentheses_regex = re.compile(r'\s*\([^)]*\)')
_punctuation_regex = re.compile(r'[^\w\s-]')
_digits_regex = re.compile(r'\d+')
_extra_whitespace_regex = re.compile(r'\s+')
_sep_regex = re.compile(r'[/-]')

# --- Supplier Name Cleaning ---
SUPPLIER_NAME_SUFFIXES = [
    # General
    'inc', 'incorporated', 'llc', 'limited', 'ltd', 'corp', 'corporation',
    'co', 'company', 'gmbh', 'ag', 'sa', 'sarl', 'srl', 'plc', 'bv', 'nv',
    'lp', 'llp', 'oyj', 'ab', 'as', 'asa', 'spa', 'pty', 'group', 'holding', 'holdings', 'sl', 'pvt', 'cts', 'ltda',
    'div', 'in', 'de', 'ch',
    # Pharma/Bio specific (Add ONLY if you are sure you want to remove them)
    'pharma', 'pharmaceuticals', 'pharmaceutical', 'therapeutics', 'biosciences', 'bioscience',
    'biopharma', 'biopharmaceutical', 'biotech', 'biotechnology', 'diagnostics', 'labotatories',
    'unit', 'site', 'joint', 'stock', 'private', 'solutions', 'solution', 'plot',
    'labs', 'laboratories', 'health', 'healthcare', 'medical', 'sciences'
    # Add others like 'genetics', 'genomics', 'research' carefully if needed
]
# Sort by length descending to handle potential overlaps correctly during regex sub
SUPPLIER_NAME_SUFFIXES.sort(key=len, reverse=True)
_supplier_suffix_pattern = re.compile(r'[\s,]+\b(' + '|'.join(SUPPLIER_NAME_SUFFIXES) + r')\b[,.]?', flags=re.IGNORECASE)

def clean_supplier_name_aggressively_pharma(name):
    """More aggressive cleaning for supplier names, removes common terms."""
    if not isinstance(name, str):
        return ""
    name = name.lower()

    # Remove the parentheses and their contents if the parentheses is not in the first position
    if name.find('(') > 3:
        name = _parentheses_regex.sub('', name)

    # Remove punctuation
    name = _punctuation_regex.sub('', name)

    # Remove digits
    name = _digits_regex.sub('', name)

    # Remove common suffixes
    name = _supplier_suffix_pattern.sub('', name).strip()

    # Remove extra whitespace
    name = _extra_whitespace_regex.sub(' ', name).strip()
    return name

# --- Product/Substance Name Cleaning ---
SUBSTANCE_ABBREVIATIONS = {
    # --- Salt Forms & Counter-ions ---
    r'\bhcl\b': 'hydrochloride', r'\bhbr\b': 'hydrobromide', r'\bhi\b': 'hydroiodide',
    r'\bsulfate\b': 'sulfate', r'\bsulphate\b': 'sulfate',
    r'\bmesylate\b': 'methanesulfonate', r'\btosylate\b': 'toluenesulfonate',
    r'\bbesylate\b': 'benzenesulfonate', r'\besylate\b': 'ethanesulfonate',
    r'\bedta\b': 'edetate',
    r'\bna\b': 'sodium', r'\bk\b': 'potassium', r'\bca\b': 'calcium', r'\bmg\b': 'magnesium',
    r'\bnh4\b': 'ammonium', r'\btris\b': 'tromethamine',
    # --- Pharmacopeias / Grades / Standards (Remove) ---
    r'\busp\b': '', r'\bnf\b': '', r'\bbp\b': '', r'\bjp\b': '', r'\bep\b': '',
    r'\bph eur\b': '', r'\beur ph\b': '', r'\bdab\b': '', r'\bph helv\b': '',
    r'\bint ph\b': '', r'\bph int\b': '', r'\bind\b': '',
    r'\bfcc\b': '', r'\bacs\b': '',
    r'\breagent grade\b': '', r'\breag\b': '',
    r'\btechnical grade\b': '', r'\btech\b': '',
    r'\banalytical grade\b': '', r'\bpa\b': '', # Assuming removal
    r'\bpure\b': '', r'\bpuriss\b': '',
    # --- Hydration / Solvation State ---
    r'\banhyd\b': 'anhydrous', r'\banhydr\b': 'anhydrous',
    r'\bmono hydrate\b': 'monohydrate', r'\bdi hydrate\b': 'dihydrate',
    r'\btri hydrate\b': 'trihydrate', r'\bhemi hydrate\b': 'hemihydrate',
    r'\bsesqui hydrate\b': 'sesquihydrate',
    r'\betoh\b': 'ethanolate', r'\bmeoh\b': 'methanolate',
    # --- Formulation Types / Dosage Forms (Remove/Normalize - decide based on goal) ---
    # Example: Removing them for substance matching
    r'\bsr\b': '', r'\ber\b': '', r'\bxr\b': '', r'\bxl\b': '', r'\bdr\b': '',
    r'\bir\b': '', r'\bodt\b': '', r'\btds\b': '', r'\btd\b': '', r'\binj\b': '',
    r'\bsoln\b': '', r'\bsol\b': '', r'\bsusp\b': '', r'\bconc\b': '', r'\btab\b': '',
    r'\bcap\b': '', r'\bcaps\b': '', r'\boint\b': '', r'\bcrm\b': '', r'\bsupp\b': '',
    r'\binh\b': '', r'\bneb\b': '', r'\bamp\b': '', r'\bpfs\b': '', r'\bmdi\b': '',
    r'\bdpi\b': '', r'\bgtt\b': '', r'\blot\b': '', r'\bpwd\b': '', r'\bpowd\b': '',
    r'\bgran\b': '', r'\bchew\b': '', r'\beff\b': '',
    # --- Routes of Administration (Remove) ---
    r'\bsubling\b': '', r'\bsl\b': '', r'\bbucc\b': '', r'\biv\b': '', r'\bim\b': '',
    r'\bsc\b': '', r'\bsubcut\b': '', r'\bpo\b': '', r'\bpr\b': '', r'\btop\b': '',
    # --- Stereochemistry (Careful - usually keep these distinctions) ---
    # r'\brac\b': 'racemic', r'\bdl\b': 'racemic', # Only if needed
    # --- General Chemical / Biological ---
    r'\baq\b': 'aqueous', r'\bdil\b': 'dilute', r'\bsat\b': 'saturated',
    r'\brec\b': 'recombinant', r'\bvet\b': 'veterinary'
}
# Compile substance abbreviations regex
# Sort keys by length descending to avoid partial replacements
_sorted_abbr_keys = sorted(SUBSTANCE_ABBREVIATIONS.keys(), key=len, reverse=True)
_compiled_substance_abbrs = {
    re.compile(p, flags=re.IGNORECASE): SUBSTANCE_ABBREVIATIONS[p]
    for p in _sorted_abbr_keys
}



def clean_product_name_aggressively_pharma(name):
    """More aggressive cleaning for product/substance names."""
    if not isinstance(name, str):
        return ""
    
    name = name.lower()
    name = name.replace('(', ' ').replace(')', ' ')  # Remove parentheses for easier regex matching
    name = name.replace('y', 'i')  # Normalize 'y' to 'i' for consistency,
    # replace ',' directly in contact of letters with ' , ' 

    # # Remove the parentheses and their contents if the parentheses is not in the first position
    # # Check if '(' exists and its position is > 3 before attempting removal
    # paren_pos = name.find('(')
    # if paren_pos > 3:
    #     name = _parentheses_regex.sub('', name) # Remove content within parentheses

    # Apply normalization/removal rules using pre-compiled regex
    for pattern, replacement in _compiled_substance_abbrs.items():
        name = pattern.sub(replacement, name)

    # Sep normalization
    name = _sep_regex.sub(' ', name)

    # Remove punctuation AFTER abbreviation expansion
    name = _punctuation_regex.sub('', name)

    # Remove extra whitespace and strip
    name = _extra_whitespace_regex.sub(' ', name).strip()

    # Return empty string if name is too short after cleaning, might indicate over-cleaning
    # if len(name) < 3:
    #     return "" # Optional: depends on requirements

    return name


def fuzzy_matched(str1, str2, FUZZY_MATCH_THRESHOLD, CLEANED_FUZZY_MATCH_THRESHOLD, mapping_type: config.MappingType, do_cleaning=True):
   """Checks if two strings are fuzzy matched based on multiple ratios."""
   # ... (This logic is now integrated into process_source1_item in the main script using rapidfuzz.process.extract)
   pass


if __name__ == "__main__":
    # Example usage for cleaning functions
    supplier1 = "PharmaCorp Solutions Inc."
    supplier2 = "Pharma Corp solutions, pvt ltd."
    print(f"Original Supplier 1: '{supplier1}'")
    print(f"Cleaned Supplier 1: '{clean_supplier_name_aggressively_pharma(supplier1)}'")
    print(f"Original Supplier 2: '{supplier2}'")
    print(f"Cleaned Supplier 2: '{clean_supplier_name_aggressively_pharma(supplier2)}'")

    product1 = "dd AMIODARONE HYDROCHLORIDE USP (for injection) 50mg"
    product2 = "AMIODARONE HCL BP (INJ)"
    print(f"Original Product 1: '{product1}'")
    print(f"Cleaned Product 1: '{clean_product_name_aggressively_pharma(product1)}'")
    print(f"Original Product 2: '{product2}'")
    print(f"Cleaned Product 2: '{clean_product_name_aggressively_pharma(product2)}'")