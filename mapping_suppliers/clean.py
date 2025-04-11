import pandas as pd
from utils import str_processing

# #  hikma_union_usa_europe
# file = "outputs/hikma_union_usa_europe.csv"

# df = pd.read_csv(file)
# print(f"Loaded {file} with {df.shape[0]} rows")

# df["supplier_name_cleaned"] = df["supplier_name"].apply(str_processing.cleaning_id)
# df["supplier_offered_api_cleaned"] = df["supplier_offered_api"].apply(str_processing.cleaning_id)

# print(f"Cleaned {file} with {df.shape[0]} rows")
# # save the cleaned file
# df.to_csv(file, index=False)



# #  substance_hikma_to_qf_mapping
# file = "outputs/substance_hikma_to_qf_mapping.csv"

# df = pd.read_csv(file)
# print(f"Loaded {file} with {df.shape[0]} rows")

# df["PUBLIC_mapped_substance_cleaned"] = df["PUBLIC_mapped_substance"].apply(str_processing.cleaning_id)
# df["QF_mapped_substance_cleaned"] = df["QF_mapped_substance"].apply(str_processing.cleaning_id)

# print(f"Cleaned {file} with {df.shape[0]} rows")
# # save the cleaned file
# df.to_csv(file, index=False)



#  qf_supplier_sites_full
file = "inputs/qf_supplier_sites.csv"

df = pd.read_csv(file)
print(f"Loaded {file} with {df.shape[0]} rows")

df["ceapp_supplier_site_name_cleaned"] = df["ceapp_supplier_site_name"].apply(str_processing.cleaning_id)

print(f"Cleaned {file} with {df.shape[0]} rows")
# save the cleaned file
df.to_csv("outputs/qf_supplier_sites.csv", index=False)

