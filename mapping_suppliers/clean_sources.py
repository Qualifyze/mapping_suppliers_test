import pandas as pd
from utils import str_processing

a57_file = "inputs/a57_raw.csv"
df_europe_base = pd.read_csv(a57_file)


europe_rows = []
for i, row in df_europe_base.iterrows():
    items_for_row = []
    
    # print(row)
    if not pd.isna(row['Route_of_admin']):
        items_for_row = row['Route_of_admin'].split("|")

        for item in items_for_row:
            if item != "":
                # Add the full row with the new item to the result and add the prefix to all column names
                new_row = row.copy()
                new_row['Route_of_admin'] = item.strip()
                europe_rows.append(new_row.to_dict())

df_europe_intermediate = pd.DataFrame(europe_rows)

df_europe = pd.DataFrame(str_processing.get_splitted_rows(df_europe_intermediate, 'Active_substance', None, ','))
print(df_europe.head())

df_europe['Active_substance_cleaned'] = df_europe['Active_substance'].apply(str_processing.cleaning_id)
df_europe['is_discontinued'] = False

# write the cleaned file
df_europe.to_csv("outputs/a57_cleaned.csv", index=False)

cep_file = "inputs/cep_raw.csv"
df_cep = pd.read_csv(cep_file)
df_cep['certificateHolder_cleaned'] = df_cep['certificateHolder'].apply(str_processing.cleaning_id)
df_cep['englishName_cleaned'] = df_cep['englishName'].apply(str_processing.cleaning_id)

# write the cleaned file
df_cep.to_csv("outputs/cep_cleaned.csv", index=False)

ob_file = "inputs/ob.csv"
df_ob_base = pd.read_csv(ob_file)
df_ob = pd.DataFrame(str_processing.get_splitted_rows(df_ob_base, 'Ingredient', None, ';'))
df_ob['Ingredient_cleaned'] = df_ob['Ingredient'].apply(str_processing.cleaning_id)
df_ob['is_discontinued'] = df_ob['Type'].apply(lambda x: True if x == 'DISCN' else False)

# write the cleaned file
df_ob.to_csv("outputs/ob_cleaned.csv", index=False)

usdmf_file = "inputs/us_dmf.csv"
df_usdmf_base = pd.read_csv(usdmf_file)
df_usdmf_base['SUBJECT_cleaned'] = df_usdmf_base['SUBJECT'].apply(str_processing.cleaning_id)
df_usdmf_base['HOLDER_cleaned'] = df_usdmf_base['HOLDER'].apply(str_processing.cleaning_id)

# write the cleaned file
df_usdmf_base.to_csv("outputs/usdmf_cleaned.csv", index=False)


qf = pd.read_csv("inputs/qf_supplier_sites_products.csv")
qf['qf_supplier_site_audited_requested_product_cleaned'] = qf['qf_supplier_site_audited_requested_product'].apply(str_processing.cleaning_id)
# save the dataframe to a csv file
qf.to_csv("outputs/qf_supplier_sites_products_cleaned.csv", index=False)


qf = pd.read_csv("inputs/qf_supplier_sites_names.csv")
qf['qf_supplier_site_name_cleaned'] = qf['qf_supplier_site_name'].apply(str_processing.cleaning_id)
# save the dataframe to a csv file
qf.to_csv("outputs/qf_supplier_sites_names_cleaned.csv", index=False)