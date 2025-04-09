import pandas as pd

filepath = '../outputs/substance_orange_book_to_usdmf_merged_all.csv'

df_merged = pd.read_csv(filepath)

# # filter have_same_base is null
# df_merged = df_merged[df_merged['have_same_base'].isnull()]

# print(f"Number of rows with have_same_base is null: {len(df_merged)}")
# print(df_merged.head())

# filter have_same_base is not null and SUBJECT_cleaned is null
df_merged = df_merged[df_merged['have_same_base'].notnull() & df_merged['US_DMF_SUBJECT_cleaned'].isnull()]
print(f"Number of rows with have_same_base is not null and SUBJECT_cleaned is null: {len(df_merged)}")
print(df_merged.head())

# orig = "FAMOTIDINE YUNG SHIN"
# print(orig)
# print(orig.replace("", "jfndofjidl").replace("", "pppppp"))