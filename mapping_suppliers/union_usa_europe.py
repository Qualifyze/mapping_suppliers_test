# FINAL	USA	EU	TYPE
# source	"usa"	"eu"	public
# manufacturer_name	OB Applicant Full Name	A57 Marketing Auth Holder	public
# drug_parent_name	Trade Name	A57 Product Name	public
# administration_route	OB_DF_ROUTE	A57 Route of Admin	public
# manufacturer_required_api	OB Ingredient	A57 Active Substance	public
# manufacturer_required_api_strenght	OB Strengh		
# supplier_name	US DMF Holder	CEP Certificate Holder	public
# supplier_offered_api	US DMF Subject	English Name	public
# supplier_offered_api_has_same_base			
# supplier_offered_api_has_same_form			
# supplier_offered_api_is_diluted			

import pandas as pd

df_europe = pd.read_csv("outputs/hikma_europe_merged_all.csv")

# df_europe['supplier_combination_is_active'] = df_europe[df_europe['

# limit the columns to the ones we need and rename them
df_europe = df_europe[['A57_Marketing_auth_holder', 'A57_Product_name', 'A57_Route_of_admin', 'A57_Active_substance', 'CEP_certificateHolder', 'CEP_englishName', 'have_same_base', 'have_same_form', 'is_diluted', 'QF_ceapp_id_supplier_site']]

df_europe = df_europe.rename(columns={
    'A57_Marketing_auth_holder': 'manufacturer_name',
    'A57_Product_name': 'drug_parent_name',
    'A57_Route_of_admin': 'administration_route',
    'A57_Active_substance': 'manufacturer_required_api',
    'CEP_certificateHolder': 'supplier_name',
    'CEP_englishName': 'supplier_offered_api',
    'have_same_base': 'supplier_offered_api_has_same_base',
    'have_same_form': 'supplier_offered_api_has_same_form',
    'is_diluted': 'supplier_offered_api_is_diluted',
    'QF_ceapp_id_supplier_site': 'qf_supplier_site_id'
})

df_europe['source'] = 'eu'

df_usa = pd.read_csv("outputs/hikma_usa_merged_all.csv")

# limit the columns to the ones we need and rename them
df_usa = df_usa[['OB_Applicant_Full_Name', 'OB_Trade_Name', 'OB_DF;Route', 'OB_Ingredient', 'OB_Strength', 'US_DMF_HOLDER', 'US_DMF_SUBJECT', 'have_same_base', 'have_same_form', 'is_diluted', 'QF_ceapp_id_supplier_site']]
df_usa = df_usa.rename(columns={
    'OB_Applicant_Full_Name': 'manufacturer_name',
    'OB_Trade_Name': 'drug_parent_name',
    'OB_DF;Route': 'administration_route',
    'OB_Ingredient': 'manufacturer_required_api',
    'OB_Strength': 'manufacturer_required_api_strength',
    'US_DMF_HOLDER': 'supplier_name',
    'US_DMF_SUBJECT': 'supplier_offered_api',
    'have_same_base': 'supplier_offered_api_has_same_base',
    'have_same_form': 'supplier_offered_api_has_same_form',
    'is_diluted': 'supplier_offered_api_is_diluted',
    'QF_ceapp_id_supplier_site': 'qf_supplier_site_id'
})
df_usa['source'] = 'usa'

# union the two dataframes
df_union = pd.concat([df_europe, df_usa], ignore_index=True)
# deduplicate the dataframe
df_union = df_union.drop_duplicates()
# save the dataframe to a csv file
df_union.to_csv("outputs/hikma_union_usa_europe.csv", index=False)