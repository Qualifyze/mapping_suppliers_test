import pandas as pd
from utils import str_processing

cleaned_suffix = "_cleaned"
final_file = "outputs/final_output.csv"

df_final = pd.read_csv(final_file)
# add cleaned columns
df_final["supplier_name" + cleaned_suffix] = df_final["supplier_name"].apply(str_processing.cleaning_id)
df_final["qf_supplier_site_name" + cleaned_suffix] = df_final["qf_supplier_site_name"].apply(str_processing.cleaning_id)
print(f"Loaded {final_file} with {df_final.shape[0]} rows")

# cep_supplier_name,is_cep_supplier_name_supplier_site,ceapp_supplier_site_name,is_ceapp_supplier_site_name_supplier_site,confidence_score_match_site_level,confidence_score_are_part_of_same_company
eu_mapping = "outputs/hikma_europe_to_qf_mapping.csv"
df_eu_mapping = pd.read_csv(eu_mapping)
df_eu_mapping["source"] = "eu"
df_eu_mapping.rename(
    columns={
        "cep_supplier_name": "supplier_name_mapping",
        "is_cep_supplier_name_supplier_site": "is_supplier_site",
        "ceapp_supplier_site_name": "qf_supplier_site_name_mapping",
        "is_ceapp_supplier_site_name_supplier_site": "qf_is_supplier_site",
        "confidence_score_match_site_level": "qf_confidence_score_match_site_level",
        "confidence_score_are_part_of_same_company": "qf_confidence_score_are_part_of_same_company",
    },
    inplace=True,
)
print(f"Loaded {eu_mapping} with {df_eu_mapping.shape[0]} rows")

# ob_supplier_name,is_ob_supplier_name_supplier_site,ceapp_supplier_site_name,is_ceapp_supplier_site_name_supplier_site,confidence_score_match_site_level,confidence_score_are_part_of_same_company
usa_mapping = "outputs/hikma_orange_to_qf_mapping.csv"
df_usa_mapping = pd.read_csv(usa_mapping)
df_usa_mapping["source"] = "usa"
# rename the columns
df_usa_mapping.rename(
    columns={
        "ob_supplier_name": "supplier_name_mapping",
        "is_ob_supplier_name_supplier_site": "is_supplier_site",
        "ceapp_supplier_site_name": "qf_supplier_site_name_mapping",
        "is_ceapp_supplier_site_name_supplier_site": "qf_is_supplier_site",
        "confidence_score_match_site_level": "qf_confidence_score_match_site_level",
        "confidence_score_are_part_of_same_company": "qf_confidence_score_are_part_of_same_company",
    },
    inplace=True,
)
print(f"Loaded {usa_mapping} with {df_usa_mapping.shape[0]} rows")


# union the two dataframes
df_mapping = pd.concat([df_eu_mapping, df_usa_mapping], ignore_index=True)
print(f"Union of {eu_mapping} and {usa_mapping} with {df_mapping.shape[0]} rows")

# clean the mapping
df_mapping["supplier_name" + cleaned_suffix] = df_mapping["supplier_name_mapping"].apply(str_processing.cleaning_id)
df_mapping["qf_supplier_site_name" + cleaned_suffix] = df_mapping["qf_supplier_site_name_mapping"].apply(str_processing.cleaning_id)

# df_intermediate -> left join with df_mapping on source, supplier_name 
print(f"Merging {final_file} with {usa_mapping} and {eu_mapping}")
df_intermediate = pd.merge(
    df_final,
    df_mapping,
    on=["source", "supplier_name" + cleaned_suffix, "qf_supplier_site_name" + cleaned_suffix],
    how="left"
)
print(f"Merged {final_file} with {usa_mapping} and {eu_mapping} with {df_intermediate.shape[0]} rows")

# is Exact Match if both are site and confidence score site level > 0.8 and qf_is_same_base is True and qf_is_same_form is True

df_intermediate["is_exact_match"] = (
    (df_intermediate["is_supplier_site"] == True) &
    (df_intermediate["qf_is_supplier_site"] == True) &
    (df_intermediate["qf_confidence_score_match_site_level"] > 0.8) &
    (df_intermediate["qf_is_same_base"] == True) &
    (df_intermediate["qf_is_same_form"] == True)
)
print(f"Added is_exact_match to rows")

# remove columns that are not needed
df_intermediate.drop(
    columns=[
        "supplier_name" + cleaned_suffix,
        "qf_supplier_site_name" + cleaned_suffix,
        "supplier_name_mapping",
        "qf_supplier_site_name_mapping",
    ],
    inplace=True
)

# remove duplicates
df_intermediate.drop_duplicates(inplace=True)

print(f"Removed duplicates, {df_intermediate.shape[0]} rows left")

print(f"Saving {df_intermediate.shape[0]} rows to ultimate_output.csv")
# write to the ultimate file
df_intermediate.to_csv("outputs/ultimate_output.csv", index=False)