import re
import pandas as pd

# --- Product/Substance Name Cleaning ---
MISSPELLED = {
    r'XENON -133': 'XENON XE-133',
    r'ACETAMINOFEN': 'ACETAMINOPHEN',
    r'ACETAZOLAMIDE 500MG SRC': 'ACETAZOLAMIDE',
}
# Compile substance abbreviations regex
# Sort keys by length descending to avoid partial replacements
_sorted_abbr_keys = sorted(MISSPELLED.keys(), key=len, reverse=True)
_compiled_misspelled_abbrs = {
    re.compile(p, flags=re.IGNORECASE): MISSPELLED[p]
    for p in _sorted_abbr_keys
}

def cleaning_id(id_str):
    # 0. Handle None or non-string input
    if id_str is None:
        return None # Or return empty string '' depending on desired output for None
    if not isinstance(id_str, str):
        id_str = str(id_str)

    # Apply normalization/removal rules using pre-compiled regex
    for pattern, replacement in _compiled_misspelled_abbrs.items():
        id_str = pattern.sub(replacement, id_str)

    return id_str

print(cleaning_id("ACETAZOLAMIDE 500MG SRC"))
print(cleaning_id("ACETAMINOFEN"))