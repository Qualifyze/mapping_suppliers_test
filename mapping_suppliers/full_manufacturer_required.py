import pandas as pd

eu = pd.read_csv("outputs/a57_cleaned.csv")
# keep the columns certificateHolder and rename it to supplier_name
eu = eu[['Active_substance_cleaned']]
eu = eu.rename(columns={'Active_substance_cleaned': 'manufacturer_required_api_cleaned'})

usa = pd.read_csv("outputs/ob_cleaned.csv")
# keep the columns DMF Holder and rename it to supplier_name
usa = usa[['Ingredient_cleaned']]
usa = usa.rename(columns={'Ingredient_cleaned': 'manufacturer_required_api_cleaned'})

# union the two dataframes
df_union = pd.concat([eu, usa], ignore_index=True)
# deduplicate the dataframe
df_union = df_union.drop_duplicates()
# remove empty strings
df_union = df_union[df_union['manufacturer_required_api_cleaned'] != '']
# save the dataframe to a csv file
df_union.to_csv("outputs/public_manufacturer_required_apis.csv", index=False)
