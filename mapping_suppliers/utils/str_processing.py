import re
import pandas as pd
from unidecode import unidecode
import string

# --- Product/Substance Name Cleaning ---
MISSPELLED = {
    r'\bXENON -133\b': 'XENON XE-133',
    r'\bACETAMINOFEN\b': 'ACETAMINOPHEN',
    r'\bACETAZOLAMIDE 500MG SRC\b': 'ACETAZOLAMIDE',
    r'\bAdenosin\b': 'Adenosine',
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

    id_str = unidecode(id_str) # Convert to ASCII, removing diacritics
    # 1. Initial Unicode Normalization (Optional but recommended for complex text)
    # NFKD decomposes characters (e.g., 'é' -> 'e' + '´') and handles ligatures.
    # The encode/decode trick attempts to remove combining diacritics.
    # try:
    #    # Decompose and remove combining marks
    #    nfkd_form = unicodedata.normalize('NFKD', id_str)
    #    cleaned_name = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    # except Exception:
    #    # Fallback if normalization fails
    #    cleaned_name = id_str
    cleaned_name = id_str # Using simpler replacements without full normalization for now

    # # 2. Convert to lowercase
    # cleaned_name = cleaned_name.lower()

    # 3. Specific replacements (including symbols like R, C, TM, ss, dashes)
    replacements = {
        # Provided
        '®': ' _r_ ', '©': ' _c_ ', '™': ' _tm_ ',
        'ß': 'ss',
        'α': ' _alpha_ ', 'β': ' _beta_ ', 'γ': ' _gamma_ ',
        'δ': ' _delta_ ', 'ε': ' _epsilon_ ', 'ζ': ' _zeta_ ',
        'η': ' _eta_ ', 'θ': ' _theta_ ', 'ι': ' _iota_ ',
        'κ': ' _kappa_ ', 'λ': ' _lambda_ ', 'μ': ' _mu_ ',
        'ν': ' _nu_ ', 'ξ': ' _xi_ ', 'ο': ' _omicron_ ',
        'π': ' _pi_ ', 'ρ': ' _rho_ ', 'σ': ' _sigma_ ',
        'τ': ' _tau_ ', 'υ': ' _upsilon_ ', 'φ': ' _phi_ ',
        'χ': ' _chi_ ', 'ψ': ' _psi_ ', 'ω': ' _omega_ ',
        '–': '-', '—': '-',
        '\x00': ' ', # Null byte
        '\u001d': ' ', # Group Separator
        '\u200b': ' ', # Zero Width Space
        '\u001c': ' ', # File Separator
        '': ' ', # File Separator
        '': ' ', # Group Separator
        '': ' ', # Device Control 2
        '': ' ', # Device Control 3
        # Ligatures (examples)
        'æ': 'ae', 'œ': 'oe',
        # Symbols with common text equivalents or standardization
        '&': ' and ', # Or just ' '
        '×': 'x',
        '±': '+-', # Or '+-'
        # Standardize or remove quotes/apostrophes (Option: Remove all)
        '"': '"', '‘': "'", '’': "'", '“': '"', '”': '"', '`': '', '´': '', "'": "'",
        # Standalone diacritics (if not handled by normalization)
        '¨': '',
        # parentheses type
        '{': '(', '}': ')',
        '[': '(', ']': ')'
    }
    for old, new in replacements.items():
        cleaned_name = cleaned_name.replace(old, new)

    # # 4. Replace brackets with parentheses (Optional: or remove, or just normalize space)
    # cleaned_name = cleaned_name.replace('[', '(').replace(']', ')')
    # cleaned_name = cleaned_name.replace('{', '(').replace('}', ')')

    # 5. Normalize spacing inside parentheses (do after potential bracket replacement)
    cleaned_name = cleaned_name.replace('( ', '(').replace(' )', ')')

    # 6. Replace remaining non-alphanumeric characters (excluding '.', '-', '(', ')') with space
    # Keep period '.', hyphen '-', parentheses '()' as they might be significant
    # Characters to replace with space: '#$%*+/\\:;<=>?@_\|~• (and any quotes if not removed above)

    # cleaned_name = re.sub(r'[#\$%\*\+/\\:;<=>\?@_\|~•]', ' ', cleaned_name)

    # Escape punctuation characters that are special in regex (like ., *, +, ?)
    escaped_punctuation = re.escape(string.punctuation)
    # Pattern 1: Add space AFTER punctuation if followed by a word character
    cleaned_name = re.sub(rf'([{escaped_punctuation}])(\w)', r'\1 \2', cleaned_name)
    # Result so far: "Hello! Howareyou? Let'sgo-fast... V2.0"

    # Pattern 2: Add space BEFORE punctuation if preceded by a word character
    cleaned_name = re.sub(rf'(\w)([{escaped_punctuation}])', r'\1 \2', cleaned_name)
    # Result: "Hello ! Howareyou ? Let ' sgo - fast ... V 2 . 0"


    # 7. Collapse multiple whitespace characters (including spaces from replacements) into one
    cleaned_name = re.sub(r'\s+', ' ', cleaned_name)
    # collapsed multiple _ to single _
    cleaned_name = re.sub(r'_{2,}', '_', cleaned_name)

    # 8. Remove leading/trailing whitespace
    cleaned_name = cleaned_name.strip()

    # 9. Aggressive final option: Remove all non alphanumeric except space and hyphen
    # cleaned_name = re.sub(r'[^a-z0-9 \-]', '', cleaned_name)

    return cleaned_name

def remove_chars_in_match(match, separator=None):
  # Get the content captured inside the parentheses (group 1)
  inside_content = match.group(1)
  # Remove the unwanted characters
  modified_content = inside_content.replace(separator, '').replace('|', '')
  # Return the content wrapped in parentheses again
  return f"({modified_content})"

def get_splitted_items(df: pd.DataFrame, item_column: str, separator = None, row = None):
    items_for_row = []
    cleaned_parentheses = re.sub(
        r'\((.*?)\)',
        lambda match: remove_chars_in_match(match, separator),
        row[item_column]
    )

    pipe_segments = cleaned_parentheses.split('|')

    for segment in pipe_segments:
        # Trim whitespace from the segment
        trimmed_segment = segment.strip()
        if not trimmed_segment: # Skip empty segments
            continue

        # Split the segment by comma followed by one or more spaces
        # This pattern acts as the delimiter between item names
        comma_segments = re.split(rf'{separator}\s+', trimmed_segment)

        # Add the resulting item names (stripped again) to the list for this row
        items_for_row.extend([s.strip() for s in comma_segments if s.strip()])
    
    return items_for_row

def get_unique_items(df: pd.DataFrame, item_column: str, separator = None):
    result = []
    for i, row in df.iterrows():
        
        # print(row)
        if not pd.isna(row[item_column]):
            if separator is not None:
                items_for_row = get_splitted_items(df, item_column, separator, row)
                for item in items_for_row:
                    if item != "":
                        result.append(item)
            else:
                # If separator is None, just append the item directly
                result.append(row[item_column])
    return list(set(result))

def get_splitted_rows(df: pd.DataFrame, item_column: str, prefix: str, separator = None):
    result = []
    for i, row in df.iterrows():
        items_for_row = []
        
        # print(row)
        if not pd.isna(row[item_column]):
            if separator is not None:
                items_for_row = get_splitted_items(df, item_column, separator, row)

                for item in items_for_row:
                    if item != "":
                        # Add the full row with the new item to the result and add the prefix to all column names
                        new_row = row.copy()
                        new_row = new_row.rename(lambda x: f"{prefix}_{x}")
                        new_row[f"{prefix}_{item_column}"] = item
                        result.append(new_row.to_dict())

            else:
                # remove continous spaces
                new_row = row.copy()
                new_row = new_row.rename(lambda x: f"{prefix}_{x}")
                new_row[f"{prefix}_{item_column}"] = item
                result.append(new_row.to_dict())

    return result

if __name__ == "__main__":

    # --- Example Usage ---
    test_ids = [
        "TEST ® ID / V.2",
        "   Substance—X™   ",
        "Beta αnd γ Kräuter", # Assuming 'α' and 'γ' should be handled if present
        "Compound [A+B]",
        "‘Test’ – Product #1",
        "What־s This?", # Example with a different dash U+05BE
        "Na±ion",
        "Müller & Söhne",
        "Substance X Y Z", # Already clean
        "substance__x__y__z",
        "AT&T",
        None,
        12345
    ]

    print("Original -> Cleaned")
    for test_id in test_ids:
        # Handle potential Unicode normalization errors if uncommented above
        cleaned = cleaning_id(test_id)
        print(f"'{test_id}' -> '{cleaned}'")

    # Example showing effect of normalization (if uncommented in function)
    # print(cleaning_id("Crème brûlée")) # -> creme brulee (if normalization works)
    # print(cleaning_id("Müller")) # -> muller (if normalization works)