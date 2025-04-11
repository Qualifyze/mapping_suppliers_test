import pandas as pd

eu = pd.read_csv("inputs/cep_raw.csv")
# keep the columns certificateHolder and rename it to supplier_name
eu = eu[['certificateHolder']]
eu = eu.rename(columns={'certificateHolder': 'supplier_name'})

usa = pd.read_csv("inputs/us_dmf.csv")
# keep the columns DMF Holder and rename it to supplier_name
usa = usa[['HOLDER']]
usa = usa.rename(columns={'HOLDER': 'supplier_name'})

# union the two dataframes
df_union = pd.concat([eu, usa], ignore_index=True)
# deduplicate the dataframe
df_union = df_union.drop_duplicates()
# remove empty strings
df_union = df_union[df_union['supplier_name'] != '']
# save the dataframe to a csv file
df_union.to_csv("outputs/public_supplier_names.csv", index=False)