import re
# import thefuzz # No longer needed if using RapidFuzz
import rapidfuzz.fuzz as fuzz # Use RapidFuzz
import functools
# import config
from utils import config # Assuming this works as before

# --- Constants and Pre-compiled Patterns ---

# Sort suffixes ONCE
_SUPPLIER_NAME_SUFFIXES_LIST = [
    # General (Longer first for safety, though regex boundaries help)
    'incorporated', 'corporation', 'pharmaceuticals', 'pharmaceutical', 'therapeutics',
    'biosciences', 'bioscience', 'biopharma', 'biopharmaceutical', 'biotech',
    'biotechnology', 'diagnostics', 'laboratories', 'healthcare', 'solutions',
    'holding', 'holdings', 'limited', 'company', 'private', 'ltda', 'joint', 'stock',
    'inc', 'llc', 'ltd', 'corp', 'gmbh', 'sarl', 'srl', 'plc', 'llp', 'oyj',
    'pty', 'pvt', 'spa', 'ag', 'sa', 'bv', 'nv', 'lp', 'ab', 'as', 'asa', 'sl',
    'group', 'labs', 'health', 'medical', 'sciences', 'unit', 'site', 'plot',
    'div', 'co', 'in', 'de', 'ch', 'cts',
    # Pharma/Bio specific already included above
]
# Build the suffix pattern ONCE and compile it
_SUFFIX_PATTERN_STR = r'[\s,]+\b(' + '|'.join(re.escape(s) for s in _SUPPLIER_NAME_SUFFIXES_LIST) + r')\b[,.]?'
_COMPILED_SUFFIX_PATTERN = re.compile(_SUFFIX_PATTERN_STR, flags=re.IGNORECASE)

# Compile other static patterns
_PARENTHESIS_PATTERN = re.compile(r'\s*\([^)]*\)')
_PUNCTUATION_PATTERN = re.compile(r'[^\w\s-]') # Keep hyphen
_DIGIT_PATTERN = re.compile(r'\d+')
_EXTRA_WHITESPACE_PATTERN = re.compile(r'\s+')


# --- Abbreviation Normalization (Pre-compiled) ---
# Compile abbreviation patterns ONCE
# Store as (compiled_regex, replacement_string) tuples
_SUBSTANCE_ABBREVIATIONS_COMPILED = []
# Original dictionary (keeping it for readability)
substance_abbreviations_map = {
    # --- Salt Forms & Counter-ions ---
    r'\bhcl\b': 'hydrochloride', r'\bhbr\b': 'hydrobromide', r'\bhi\b': 'hydroiodide',
    r'\bsulfate\b': 'sulfate', r'\bsulphate\b': 'sulfate', r'\bmesylate\b': 'methanesulfonate',
    r'\btosylate\b': 'toluenesulfonate', r'\bbesylate\b': 'benzenesulfonate', r'\besylate\b': 'ethanesulfonate',
    r'\bedta\b': 'edetate', r'\bna\b': 'sodium', r'\bk\b': 'potassium', r'\bca\b': 'calcium',
    r'\bmg\b': 'magnesium', r'\bnh4\b': 'ammonium', r'\btris\b': 'tromethamine',
    # --- Pharmacopeias / Grades / Standards (Remove) ---
    r'\b(usp|nf|bp|jp|ep|ph\s+eur|eur\s+ph|dab|ph\s+helv|int\s+ph|ph\s+int|ind|fcc|acs)\b': '',
    # --- Other Standards/Grades (Normalize/Remove) ---
    r'\breagent\s+grade\b': 'reagent', r'\breag\b': 'reagent',
    r'\btechnical\s+grade\b': 'technical', r'\btech\b': 'technical',
    r'\banalytical\s+grade\b': 'analytical', r'\bpa\b': 'pro analysi', # Or remove?
    r'\bpure\b': 'pure', r'\bpuriss\b': 'pure',
    # If removing grades: r'\b(reagent|technical|analytical|pure)\b': '',
    # --- Hydration / Solvation State ---
    r'\banhyd\b': 'anhydrous', r'\banhydr\b': 'anhydrous',
    r'\bmono\s+hydrate\b': 'monohydrate', r'\bdi\s+hydrate\b': 'dihydrate',
    r'\btri\s+hydrate\b': 'trihydrate', r'\bhemi\s+hydrate\b': 'hemihydrate',
    r'\bsesqui\s+hydrate\b': 'sesquihydrate',
    r'\betoh\b': 'ethanolate', r'\bmeoh\b': 'methanolate',
    # --- Formulation Types / Dosage Forms ---
    r'\bsr\b': 'sustained release', r'\b(er|xr|xl)\b': 'extended release',
    r'\bdr\b': 'delayed release', r'\bir\b': 'immediate release',
    r'\bodt\b': 'orally disintegrating tablet', r'\btds\b': 'transdermal system',
    r'\btd\b': 'transdermal', r'\binj\b': 'injection',
    r'\b(soln|sol)\b': 'solution', r'\bsusp\b': 'suspension',
    r'\bconc\b': 'concentrate', r'\btab\b': 'tablet',
    r'\b(cap|caps)\b': 'capsule', r'\boint\b': 'ointment',
    r'\bcrm\b': 'cream', r'\bsupp\b': 'suppository',
    r'\binh\b': 'inhalation', r'\bneb\b': 'nebule',
    r'\bamp\b': 'ampoule', r'\bpfs\b': 'prefilled syringe',
    r'\bmdi\b': 'metered dose inhaler', r'\bdpi\b': 'dry powder inhaler',
    r'\bgtt\b': 'drops', r'\blot\b': 'lotion',
    r'\b(pwd|powd)\b': 'powder', r'\bgran\b': 'granules',
    r'\bchew\b': 'chewable', r'\beff\b': 'effervescent',
    # --- Routes of Administration ---
    r'\b(subling|sl)\b': 'sublingual', r'\bbucc\b': 'buccal',
    r'\biv\b': 'intravenous', r'\bim\b': 'intramuscular',
    r'\b(sc|subcut)\b': 'subcutaneous', r'\bpo\b': 'oral',
    r'\bpr\b': 'rectal', r'\btop\b': 'topical',
    # --- General Chemical / Biological ---
    r'\baq\b': 'aqueous', r'\bdil\b': 'dilute', r'\bsat\b': 'saturated',
    r'\brec\b': 'recombinant', r'\bvet\b': 'veterinary',
}

# Compile regex patterns, ensuring longer patterns are processed first if they overlap implicitly
# Sorting by pattern length descending helps avoid partial matches (e.g., 'ph eur' before 'ep')
# Note: Regex alternation `(a|b)` often handles this internally, but explicit sorting is safer for complex rules.
_sorted_abbr_patterns = sorted(substance_abbreviations_map.keys(), key=len, reverse=True)

for abbr_pattern in _sorted_abbr_patterns:
    replacement = substance_abbreviations_map[abbr_pattern]
    # Use re.IGNORECASE for case-insensitive matching during compilation
    compiled_pattern = re.compile(abbr_pattern, flags=re.IGNORECASE)
    _SUBSTANCE_ABBREVIATIONS_COMPILED.append((compiled_pattern, replacement))


# --- Optimized Cleaning Functions with Caching ---

@functools.lru_cache(maxsize=1024) # Cache results for recently cleaned names
def clean_supplier_name_aggressively_pharma(name: str) -> str:
    """More aggressive cleaning using pre-compiled patterns and caching."""
    if not isinstance(name, str) or not name:
        return ""
    original_name = name # Keep for comparison if needed, though not strictly used here
    name = name.lower()

    # Remove parentheses (if not at the start) using pre-compiled pattern
    paren_match = name.find('(')
    if paren_match > 3:
        name = _PARENTHESIS_PATTERN.sub('', name) # Only removes if pattern matches

    # Remove punctuation using pre-compiled pattern
    name = _PUNCTUATION_PATTERN.sub('', name)

    # Remove digits using pre-compiled pattern
    name = _DIGIT_PATTERN.sub('', name)

    # Remove common suffixes using pre-compiled pattern
    name = _COMPILED_SUFFIX_PATTERN.sub('', name).strip()

    # Remove extra whitespace using pre-compiled pattern
    name = _EXTRA_WHITESPACE_PATTERN.sub(' ', name).strip()
    return name

@functools.lru_cache(maxsize=4096) # Cache more product names if memory allows
def clean_product_name_aggressively_pharma(name: str) -> str:
    """More aggressive cleaning for product names with caching and compiled regex."""
    if not isinstance(name, str) or not name:
        return ""
    original_name = name # Keep for comparison
    name = name.lower()

    # Remove parentheses (if not at the start) using pre-compiled pattern
    paren_match = name.find('(')
    if paren_match > 3:
        name = _PARENTHESIS_PATTERN.sub('', name) # Only removes if pattern matches

    # Apply pre-compiled abbreviation normalization rules
    for compiled_pattern, replacement in _SUBSTANCE_ABBREVIATIONS_COMPILED:
        name = compiled_pattern.sub(replacement, name)

    # Remove punctuation AFTER abbreviation normalization (might introduce symbols)
    name = _PUNCTUATION_PATTERN.sub('', name) # Keep hyphen

    # Remove digits
    name = _DIGIT_PATTERN.sub('', name)

    # Final whitespace cleanup
    name = _EXTRA_WHITESPACE_PATTERN.sub(' ', name).strip()

    # Return empty string if cleaning removed everything useful (optional)
    if len(name) < 3:
         return ""

    return name


# --- Optimized Fuzzy Matching Function with Caching ---

# Cache results for recently compared pairs (adjust maxsize based on memory/usage)
# Note: Caching assumes (str1, str2, ...) arguments are hashable and results deterministic
@functools.lru_cache(maxsize=8192)
def fuzzy_matched(str1: str, str2: str, FUZZY_MATCH_THRESHOLD: int, CLEANED_FUZZY_MATCH_THRESHOLD: int, mapping_type: config.MappingType, do_cleaning=True) -> bool:
    """
    Checks if two strings are fuzzy matched using RapidFuzz and caching.
    Uses optimized cleaning functions.
    """
    # Basic type/empty checks
    if not isinstance(str1, str) or not isinstance(str2, str) or not str1 or not str2:
        return False
    if str1 == str2: # Exact match is always true
        return True

    # --- Phase 1: Check Raw Strings ---
    # Using RapidFuzz functions - often significantly faster
    # Check cheaper ratios first if thresholds allow for early exit
    if fuzz.ratio(str1, str2) >= FUZZY_MATCH_THRESHOLD: return True
    if fuzz.partial_ratio(str1, str2) >= FUZZY_MATCH_THRESHOLD: return True
    # Token ratios are more expensive, calculate if cheaper ones fail
    if fuzz.token_set_ratio(str1, str2) >= FUZZY_MATCH_THRESHOLD: return True
    if fuzz.token_sort_ratio(str1, str2) >= FUZZY_MATCH_THRESHOLD: return True

    # --- Phase 2: Check Cleaned Strings (if cleaning enabled and needed) ---
    if not do_cleaning:
        return False # Failed raw check and cleaning is disabled

    # Select appropriate cleaning function
    if mapping_type == config.MappingType.SUBSTANCE:
        # Specific rule for SUBSTANCE type
        if ('intermediate' in str1.lower()) != ('intermediate' in str2.lower()):
            return False
        clean_func = clean_product_name_aggressively_pharma
    elif mapping_type == config.MappingType.SUPPLIER:
        clean_func = clean_supplier_name_aggressively_pharma
    else:
        # Handle unknown mapping type if necessary, maybe default to no cleaning or raise error
        return False # Or raise ValueError("Unsupported mapping_type")

    # Call cached cleaning functions
    cleaned_str1 = clean_func(str1)
    cleaned_str2 = clean_func(str2)

    # If cleaning resulted in empty strings or they became identical
    if not cleaned_str1 or not cleaned_str2:
        return False
    if cleaned_str1 == cleaned_str2:
        # Consider if cleaning making them identical should be a match,
        # even if raw comparison failed. Often, yes.
        # Check against CLEANED_FUZZY_MATCH_THRESHOLD implicitly >= 100
        return CLEANED_FUZZY_MATCH_THRESHOLD <= 100 # Assuming 100 is always a match


    # Check for single-word mismatch after cleaning (optimization)
    # Simplified check: split creates list, check length
    cleaned_str1_words = cleaned_str1.split()
    cleaned_str2_words = cleaned_str2.split()
    if len(cleaned_str1_words) == 1 and len(cleaned_str2_words) == 1 and cleaned_str1 != cleaned_str2:
         return False # Single different words after cleaning shouldn't match usually

    # Calculate ratios on cleaned strings
    # Choose ratios based on mapping type (as per original logic)
    if mapping_type == config.MappingType.SUBSTANCE:
        if fuzz.token_sort_ratio(cleaned_str1, cleaned_str2) >= CLEANED_FUZZY_MATCH_THRESHOLD: return True
        if fuzz.token_set_ratio(cleaned_str1, cleaned_str2) >= CLEANED_FUZZY_MATCH_THRESHOLD: return True
    elif mapping_type == config.MappingType.SUPPLIER:
        # Check cheaper ratios first on cleaned strings too
        if fuzz.ratio(cleaned_str1, cleaned_str2) >= CLEANED_FUZZY_MATCH_THRESHOLD: return True
        if fuzz.partial_ratio(cleaned_str1, cleaned_str2) >= CLEANED_FUZZY_MATCH_THRESHOLD: return True
        # Then token-based ones
        if fuzz.token_sort_ratio(cleaned_str1, cleaned_str2) >= CLEANED_FUZZY_MATCH_THRESHOLD: return True
        if fuzz.token_set_ratio(cleaned_str1, cleaned_str2) >= CLEANED_FUZZY_MATCH_THRESHOLD: return True

    # If none of the conditions were met
    return False
