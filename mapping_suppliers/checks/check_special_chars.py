import pandas as pd


sources = [
    {
        'name': 'a57_raw.csv',
        'key': 'Active_substance'
    },
    {
        'name': 'cep_raw.csv',
        'key': 'englishName'
    },
    {
        'name': 'ob.csv',
        'key': 'Ingredient'
    },
    {
        'name': 'us_dmf.csv',
        'key': 'SUBJECT'
    }
]

main_df = pd.DataFrame()

for source in sources:
    df = pd.read_csv(f'inputs/{source["name"]}')
    df = df[[source['key']]]
    df.rename(columns={source['key']: 'ids'}, inplace=True)
    # union of all dataframes
    main_df = pd.concat([main_df, df], ignore_index=True)

main_df.drop_duplicates(inplace=True)


ids_series = main_df['ids'].dropna().astype(str)

all_chars_string = "".join(ids_series)

special_chars_set = set()

for char in all_chars_string:
    if not char.isalnum() and not char.isspace():
        special_chars_set.add(char)

# Convert the set to a sorted list for ordered output
distinct_special_chars = sorted(list(special_chars_set))

# Print the result
print("\n--------------------------------------------------")
print("Distinct special characters found in 'ids' column:")
if distinct_special_chars:
    print(distinct_special_chars)
else:
    print("No special characters found.")
print("--------------------------------------------------")