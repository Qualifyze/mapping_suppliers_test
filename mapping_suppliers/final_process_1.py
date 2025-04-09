import pandas as pd
from utils import str_processing

final_columns = [
    "source",
    "manufacturer_name",
    "drug_parent_name",
    "administration_route",
    "manufacturer_required_api",
    "manufacturer_required_api_strength",
    "supplier_name",
    "supplier_offered_api",
    "supplier_offered_api_has_same_base",
    "supplier_offered_api_has_same_form",
    "supplier_offered_api_is_diluted",
    "qf_supplier_id",
    "qf_supplier_site_id",
    "qf_supplier_site_name",
    "qf_supplier_site_valid_audit_count",
    "qf_supplier_site_has_valid_audit",
    "qf_supplier_site_audited_quality_types",
    "qf_supplier_site_first_audit_booked",
    "qf_supplier_site_last_expiration",
    "qf_supplier_site_audited_requested_api", # Note: This might often be NaN after the joins if not consistently populated
    "qf_supplier_site_audited_requested_product_list", # This is the column we want to retrieve conditionally
    "qf_is_same_base",
    "qf_is_same_form",
    "qf_is_diluted",
    "qf_is_audit_valid",
    "qf_qcr",
    "qf_qcr_date",
]

cleaned_suffix = "_cleaned"

# --- Load DataFrames (assuming CSVs exist with expected columns) ---
# Create dummy dataframes for demonstration if files don't exist
try:
    df_unionned = pd.read_csv("outputs/hikma_union_usa_europe.csv")
except FileNotFoundError:
    print("Warning: hikma_union_us_europe.csv not found. Creating dummy data.")
    df_unionned = pd.DataFrame({
        "source": ["US", "EU"], "manufacturer_name": ["M1", "M1"],
        "drug_parent_name": ["DrugA", "DrugB"], "administration_route": ["Oral", "IV"],
        "manufacturer_required_api": ["API_X v1", "API_Y"], "manufacturer_required_api_strength": ["10mg", "5%"],
        "supplier_name": ["S1", "S2"], "supplier_offered_api": ["API_X v1 Base", "API_Y Salt"],
        "supplier_offered_api_has_same_base": [True, True], "supplier_offered_api_has_same_form": [False, False],
        "supplier_offered_api_is_diluted": [False, True], "qf_supplier_site_id": [101, 102],
    })
# ---

df_unionned["supplier_offered_api" + cleaned_suffix] = df_unionned["supplier_offered_api"].apply(str_processing.cleaning_id)
df_unionned["qf_supplier_site_id"] = df_unionned["qf_supplier_site_id"].astype('Int64')
print(f"Loaded {df_unionned.shape[0]} rows from hikma_union_us_europe.csv")

# ---
try:
    df_qf = pd.read_csv("inputs/qf_supplier_sites_full.csv")
except FileNotFoundError:
     print("Warning: qf_supplier_sites_full.csv not found. Creating dummy data.")
     df_qf = pd.DataFrame({
         "qf_supplier_id": [1, 2], "qf_supplier_site_id": [101, 102],
         "qf_supplier_name": ["S1 Name", "S2 Name"], "qf_supplier_site_name": ["Site A", "Site B"],
         "qf_supplier_site_valid_audit_count": [2, 1], "qf_supplier_site_has_valid_audit": [True, True],
         "qf_supplier_site_audited_quality_types": ["GMP", "GDP"], "qf_supplier_site_first_audit_booked": ["2023-01-01", "2023-05-01"],
         "qf_supplier_site_last_expiration": ["2025-01-01", "2024-12-31"],
         "qf_supplier_site_audited_requested_api": ["API_X", "API_Y"], # Original API name from QF Audit
         "qf_supplier_site_audited_requested_product": ["API_X Base", "API_Y Salt"] # Original Product name from QF Audit
     })
# ---
df_qf["qf_supplier_site_audited_requested_product" + cleaned_suffix] = df_qf["qf_supplier_site_audited_requested_product"].apply(str_processing.cleaning_id)
df_qf["qf_supplier_site_id"] = df_qf["qf_supplier_site_id"].astype('Int64')
print(f"Loaded {df_qf.shape[0]} rows from qf_supplier_sites_full.csv")

# ---
try:
    df_substance_mapping = pd.read_csv("outputs/substance_hikma_to_qf_mapping.csv")
except FileNotFoundError:
     print("Warning: substance_hikma_to_qf_mapping.csv not found. Creating dummy data.")
     df_substance_mapping = pd.DataFrame({
         "PUBLIC_mapped_substance": ["API_X v1 Base", "API_Y Salt", "API_Z"], # From Hikma/Public
         "QF_mapped_substance": ["API_X Base", "API_Y Salt", "API_Z hydrate"], # From QF
         "have_same_base": [True, True, True],
         "have_same_form": [True, True, False],
         "is_diluted": [False, False, False]
     })
# ---
df_substance_mapping["PUBLIC_mapped_substance" + cleaned_suffix] = df_substance_mapping["PUBLIC_mapped_substance"].apply(str_processing.cleaning_id)
df_substance_mapping["QF_mapped_substance" + cleaned_suffix] = df_substance_mapping["QF_mapped_substance"].apply(str_processing.cleaning_id)
print(f"Loaded {df_substance_mapping.shape[0]} rows from substance_hikma_to_qf_mapping.csv")
df_substance_mapping = df_substance_mapping.rename(
    columns={
        "have_same_base": "qf_is_same_base",
        "have_same_form": "qf_is_same_form",
        "is_diluted": "qf_is_diluted" # Note: original code used this name, aligning with it.
    }
)

print(f"Merging dataframes hikma_union_us_europe.csv, qf_supplier_sites_full.csv")
df_intermediate = pd.merge(
    df_unionned,
    df_qf,
    on="qf_supplier_site_id", # Simplified merge key if names match
    how="left"
)
print(f"Merged {df_intermediate.shape[0]} rows -> df_intermediate")

print(f"Merging dataframes df_intermediate, substance_hikma_to_qf_mapping.csv")
# Merge intermediate results with substance mapping
# This merge links the supplier's offered API (from Hikma) and the audited product (from QF) via the mapping table
df_intermediate_2 = pd.merge(
    df_intermediate,
    df_substance_mapping,
    left_on=["supplier_offered_api" + cleaned_suffix, "qf_supplier_site_audited_requested_product" + cleaned_suffix],
    right_on=["PUBLIC_mapped_substance" + cleaned_suffix, "QF_mapped_substance" + cleaned_suffix],
    how="left" # Keep all rows from df_intermediate, add mapping info where available
)
print(f"Merged {df_intermediate_2.shape[0]} rows -> df_intermediate_2")

# --- Load and Process QCR Data ---
# Create dummy QCR data if files don't exist
try:
    df_2024 = pd.read_csv("inputs/qcr_2024.csv")
except FileNotFoundError:
    print("Warning: qcr_2024.csv not found. Creating dummy data.")
    df_2024 = pd.DataFrame({'Final QCR': ['Pass', 'Fail'], 'auditID': [1001, 1003], 'qualityType': ['GMP_API', 'GMP_API']})
# ---
df_2024 = df_2024[['Final QCR', 'auditID', 'qualityType']]
df_2024.rename(columns={'auditID': 'id_audit'}, inplace=True)

# ---
try:
    df_2025 = pd.read_csv("inputs/qcr_2025.csv")
except FileNotFoundError:
    print("Warning: qcr_2025.csv not found. Creating dummy data.")
    df_2025 = pd.DataFrame({'Final QCR': ['Pass', 'Missing/wrong Data'], 'auditID': [1002, 1004], 'qualityType': ['GMP_API', 'GMP_API']})
# ---
df_2025 = df_2025[['Final QCR', 'auditID', 'qualityType']]
df_2025.rename(columns={'auditID': 'id_audit'}, inplace=True)

all_qcr = pd.concat([df_2024, df_2025], axis=0)
all_qcr_filtered = all_qcr[all_qcr['qualityType'] == 'GMP_API']
all_qcr_filtered = all_qcr_filtered[all_qcr_filtered['Final QCR'] != 'Missing/wrong Data']

# ---
try:
    df_audits = pd.read_csv("inputs/audits.csv")
except FileNotFoundError:
    print("Warning: audits.csv not found. Creating dummy data.")
    df_audits = pd.DataFrame({
        'id_audit': [1001, 1002, 1003, 1004, 1005],
        'ceapp_id_supplier_site': [101, 102, 101, 102, 103], # Site IDs matching df_unionned/df_qf
        'audit_date': ['2024-01-15', '2024-03-10', '2023-11-01', '2023-12-01', '2024-02-01']
    })
#---
df_audits['audit_date_cmp'] = pd.to_datetime(df_audits['audit_date'])

qcr_merged = pd.merge(df_audits, all_qcr_filtered, on='id_audit', how='inner')
qcr_merged_sorted = qcr_merged.sort_values(by=['ceapp_id_supplier_site', 'audit_date_cmp'], ascending=[True, False])

latest_qcr_per_site = qcr_merged_sorted.groupby('ceapp_id_supplier_site').first().reset_index()
qcr_final = latest_qcr_per_site[['ceapp_id_supplier_site', 'id_audit', 'audit_date', 'Final QCR']]
qcr_final = qcr_final.rename(columns={'Final QCR': 'qf_qcr', 'audit_date': 'qf_qcr_date'})
print(f"Loaded qcr_final with {qcr_final.shape[0]} latest QCR per site.")

# --- Merge QCR data with the main dataframe ---
print(f"Merging dataframes df_intermediate_3, qcr_final")
# Merge QCR data onto df_intermediate_3 (which now has the correct product column)
df_final = pd.merge(
    df_intermediate_2,
    qcr_final,
    left_on=["qf_supplier_site_id"],
    right_on=["ceapp_id_supplier_site"],
    how="left"
)
print(f"Merged {df_final.shape[0]} rows -> df_final")
# --- Aggregation Step ---
print("\n--- Starting Aggregation ---")

# Define the columns to group by
grouping_columns = [
    "source",
    "manufacturer_name",
    "drug_parent_name",
    "administration_route",
    "manufacturer_required_api",
    "manufacturer_required_api_strength",
    "supplier_name",
    "supplier_offered_api",
    "supplier_offered_api_has_same_base",
    "supplier_offered_api_has_same_form",
    "supplier_offered_api_is_diluted",
    "qf_supplier_id",
    "qf_supplier_site_id",
    "qf_supplier_site_name",
    "qf_supplier_site_valid_audit_count",
    "qf_supplier_site_has_valid_audit",
    "qf_supplier_site_audited_quality_types",
    "qf_supplier_site_first_audit_booked",
    "qf_supplier_site_last_expiration",
    "qf_supplier_site_audited_requested_api", # Included as per your list
    "qf_qcr",
    "qf_qcr_date",
]

# Define the aggregation logic
# Note: 'any()' performs a logical OR (True if any value in the group is True, ignores NaN by default)
#       'all()' performs a logical AND (True if all values in the group are True, ignores NaN by default)
#       For the product list, we define a lambda function to join unique, non-null string values.
aggregation_rules = {
    'qf_supplier_site_audited_requested_product': lambda x: '|||'.join(x.dropna().astype(str).unique()),
    'qf_is_same_base': 'any',  # Logical OR
    'qf_is_same_form': 'any',  # Logical OR
    'qf_is_diluted': 'all',    # Logical AND (as requested by 'and' in prompt)
    'qf_is_audit_valid': 'any' # Logical OR
}

# Perform the aggregation
# Using as_index=False keeps the grouping columns as regular columns
print(f"Grouping by {len(grouping_columns)} columns and aggregating...")
df_aggregated = df_final.groupby(grouping_columns, as_index=False, dropna=False).agg(aggregation_rules)
print(f"Aggregation resulted in {df_aggregated.shape[0]} rows.")

# Rename the aggregated product column
df_aggregated = df_aggregated.rename(columns={
    'qf_supplier_site_audited_requested_product': 'qf_supplier_site_audited_requested_product_list'
})

# --- Replace df_final with the aggregated result ---
df_final = df_aggregated # Overwrite df_final with the aggregated data

print("--- Aggregation Complete ---")

# --- Final Processing ---

# Ensure all final columns exist, add NaN if missing (e.g., QCR columns if no match found)
for col in final_columns:
    if col not in df_final.columns:
        df_final[col] = pd.NA # Or np.nan, appropriate null value

# Select and order final columns
df_final = df_final[final_columns]

# Remove duplicates based on the final set of columns
df_final = df_final.drop_duplicates()

# Sort as required
df_final = df_final.sort_values(by=["source", "manufacturer_name", "drug_parent_name"])

# Reset index
df_final = df_final.reset_index(drop=True)

print(f"Final dataframe df_final has {df_final.shape[0]} rows")
print("Final Columns:", df_final.columns.tolist())
# Set the display option to show all columns
pd.set_option('display.max_columns', None)
print("Sample Data:\n", df_final.head())

# Export df_final to csv
try:
    df_final.to_csv("outputs/final_output.csv", index=False)
    print("Exported df_final to outputs/final_output.csv")
except Exception as e:
    print(f"Error exporting CSV: {e}")