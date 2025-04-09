import argparse
import sys
import pandas as pd

from utils import str_processing
from utils import config
from utils import fuzzy_matching
from utils import batch_gen_util, batch_ret_util

import os
import json
import csv
import uuid
from openai.lib._parsing._completions import type_to_response_format_param

import rapidfuzz.fuzz as fuzz
import rapidfuzz.process as process
import multiprocessing
from functools import partial

SCORERS_ORIG = [fuzz.token_set_ratio, fuzz.ratio, fuzz.partial_ratio, fuzz.token_sort_ratio]
SCORERS_CLEANED = [fuzz.token_sort_ratio, fuzz.token_set_ratio]


def process_source1_item(source_1_tuple, source_2_data, mapping_config: config.MappingConfig):
    """
    Processes a source_1 item against source_2.
    Compares two approaches:
    1. Matching Original strings directly (Original Only).
    2. Matching Cleaned strings, then post-filtering with Original strings (Combined).
    Filters source_2 based on 'intermediate' status for SUBSTANCE mapping BEFORE matching.
    Returns discrepancy details between the two approaches & candidates from the Combined approach.
    """
    original_index_1, name_1_orig, name_1_cleaned, source1_has_intermediate = source_1_tuple
    threshold_orig = mapping_config.fuzzy_match_threshold
    threshold_cleaned = mapping_config.cleaned_fuzzy_match_threshold

    # Unpack source_2 data
    s2_orig_list_full = source_2_data['orig_list']
    s2_cleaned_list_full = source_2_data['cleaned_list']
    s2_indices_full = source_2_data['indices']
    s2_has_intermediate_list = source_2_data['has_intermediate_list']
    s2_index_to_orig_full = source_2_data['index_to_orig']


    matches_combined_approach_indices = set() # Stores results of Cleaned + Original Post-Filter
    item_candidates = [] # Candidates from the Combined approach

    # --- Pre-filter Source 2 based on 'intermediate' status ---
    if mapping_config.mapping_type == config.MappingType.SUBSTANCE:
        matching_status_orig_indices = [
            idx for idx, status in enumerate(s2_has_intermediate_list)
            if status == source1_has_intermediate
        ]
    else:
        matching_status_orig_indices = list(range(len(s2_orig_list_full)))

    if not matching_status_orig_indices:
        # logging.debug(f"Skipping '{name_1_orig}' - no source 2 items match 'intermediate' status.")
        return None, []

    # Create filtered lists and index mapping for the matching process
    filtered_s2_orig_list = [s2_orig_list_full[i] for i in matching_status_orig_indices]
    filtered_s2_cleaned_list = [s2_cleaned_list_full[i] for i in matching_status_orig_indices]
    filtered_idx_to_orig_s2_df_idx = {
        new_idx: s2_indices_full[orig_list_idx]
        for new_idx, orig_list_idx in enumerate(matching_status_orig_indices)
    }
    # --- End Pre-filtering ---

    try:
        # --- Approach Matching WITH Cleaning + Original Post-Filter (Combined) ---
        if name_1_cleaned and name_1_cleaned != "" and filtered_s2_cleaned_list:
            # cutoff_for_extract_cleaned = max(1, threshold_cleaned - 15)
            cutoff_for_extract_cleaned = 0
            # Step 2.1: Initial match using CLEANED names and CLEANED threshold
            results_cleaned = process.extract(
                name_1_cleaned,
                filtered_s2_cleaned_list,
                scorer=fuzz.token_set_ratio, # Example primary scorer for extraction
                limit=None,
                score_cutoff=cutoff_for_extract_cleaned
            )

            for choice_cleaned, score, idx_in_filtered_list in results_cleaned:
                name_2_cleaned_filtered = filtered_s2_cleaned_list[idx_in_filtered_list]
                if not name_2_cleaned_filtered or name_2_cleaned_filtered == "": continue

                # Check *all* cleaned scorers against the CLEANED threshold
                all_scores_cleaned = [scorer(name_1_cleaned, name_2_cleaned_filtered) for scorer in SCORERS_CLEANED]
                name_1_split = name_1_cleaned.split()
                name_2_cleaned_split = name_2_cleaned_filtered.split()
                original_s2_df_index = filtered_idx_to_orig_s2_df_idx[idx_in_filtered_list]

                # print(f"Cleaned match: '{name_1_cleaned}' vs '{name_2_cleaned_filtered}' - Scores: {all_scores_cleaned} - Index: {original_s2_df_index}")

                if any(s >= threshold_cleaned for s in all_scores_cleaned):
                    # This item passed the CLEANED match check. Now apply post-filter.
                    name_2_orig = s2_index_to_orig_full[original_s2_df_index] # Get original name 2

                    # Step 2.2: Post-filter using ORIGINAL names and ORIGINAL threshold
                    all_scores_orig_postfilter = [scorer(name_1_orig.lower(), name_2_orig.lower()) for scorer in SCORERS_ORIG]

                    if any(s >= threshold_orig for s in all_scores_orig_postfilter):
                        # Passed BOTH cleaned match and original post-filter
                        matches_combined_approach_indices.add(original_s2_df_index)

                    # print(f"Post-filter match: '{name_1_orig}' vs '{name_2_orig}' - Scores: {all_scores_orig_postfilter} - Index: {original_s2_df_index}")
                elif len(name_1_split) > 0 and len(name_2_cleaned_split) > 0 and name_1_split[0].lower() == name_2_cleaned_split[0].lower():
                    # print("Need to check the first word of the cleaned name")
                    matches_combined_approach_indices.add(original_s2_df_index)

        # --- Collect Candidates based on the "Combined" approach results ---
        for original_s2_df_index in matches_combined_approach_indices:
            mapping = { "item_1": name_1_orig,
                        "item_2": s2_index_to_orig_full[original_s2_df_index] }
            item_candidates.append(mapping)

    except Exception as e:
        print(f"ERROR processing source_1 item {original_index_1} ('{name_1_orig}'): {e}", exc_info=True)

    return item_candidates


def generate_batch():
    # Prep folders
    date_time_str = pd.Timestamp.now().strftime('%Y_%m_%d_%H_%M_%S')
    batch_folder = f'{mapping_config.mapping_name}/batches/{date_time_str}/inputs'
    os.makedirs(batch_folder, exist_ok=True)

    inputs_folder = os.path.join(os.getcwd(), batch_folder)
    Batch_Gen_Util = batch_gen_util.BatchGenUtil(main_folder=inputs_folder, batch_size=mapping_config.batch_size, dry_run=args.dry_run)

    print(f"Getting unique items from {mapping_config.source_1_filename} and {mapping_config.source_2_filename}...")
    df_unique_source_1 = pd.DataFrame(str_processing.get_unique_items(df_source_1, mapping_config.source_1_id_cleaned, mapping_config.source_1_separator))
    df_unique_source_2 = pd.DataFrame(str_processing.get_unique_items(df_source_2, mapping_config.source_2_id_cleaned, mapping_config.source_2_separator))
    df_unique_source_1[mapping_config.source_1_id_cleaned] = df_unique_source_1[0]
    df_unique_source_2[mapping_config.source_2_id_cleaned] = df_unique_source_2[0]

    print(f"Loaded {len(df_unique_source_1)} unique items from {mapping_config.source_1_filename} and {len(df_unique_source_2)} unique items from {mapping_config.source_2_filename}.")

    if mapping_config.mapping_type == config.MappingType.SUBSTANCE:
        df_unique_source_1['has_intermediate'] = df_unique_source_1[mapping_config.source_1_id_cleaned].str.lower().str.contains('intermediate', na=False, regex=False)
        df_unique_source_2['has_intermediate'] = df_unique_source_2[mapping_config.source_2_id_cleaned].str.lower().str.contains('intermediate', na=False, regex=False)
    else:
        # Add columns with default False to avoid errors later if accessed
        df_unique_source_1['has_intermediate'] = False
        df_unique_source_2['has_intermediate'] = False

    if mapping_config.mapping_type == config.MappingType.SUBSTANCE:
        clean_func = fuzzy_matching.clean_product_name_aggressively_pharma
    elif mapping_config.mapping_type == config.MappingType.SUPPLIER:
        clean_func = fuzzy_matching.clean_supplier_name_aggressively_pharma

    df_unique_source_1['aggressively_cleaned_name'] = df_unique_source_1[mapping_config.source_1_id_cleaned].apply(clean_func)
    df_unique_source_2['aggressively_cleaned_name'] = df_unique_source_2[mapping_config.source_2_id_cleaned].apply(clean_func)

    df_unique_source_1 = df_unique_source_1.reset_index(drop=True)
    df_unique_source_2 = df_unique_source_2.reset_index(drop=True)

    source_1_data_tuples = list(zip(
        df_unique_source_1.index,
        df_unique_source_1[mapping_config.source_1_id_cleaned],
        df_unique_source_1['aggressively_cleaned_name'],
        df_unique_source_1['has_intermediate'] # Already computed or set to False
    ))
    source_2_data = {
        'orig_list': df_unique_source_2[mapping_config.source_2_id_cleaned].tolist(),
        'cleaned_list': df_unique_source_2['aggressively_cleaned_name'].tolist(),
        'indices': df_unique_source_2.index.tolist(), # Index of row in df_unique_source_2
        'index_to_orig': df_unique_source_2[mapping_config.source_2_id_cleaned].to_dict(), # Map df index to orig name
        'has_intermediate_list': df_unique_source_2['has_intermediate'].tolist() # Already computed or set to False
    }
    source_1_length = len(source_1_data_tuples); source_2_length = len(source_2_data['orig_list'])
    print(f"Prepared {source_1_length} Source 1 items and {source_2_length} Source 2 items.")

    # --- Parallel Processing ---
    mappings_candidate = []
    process_func = partial(process_source1_item, source_2_data=source_2_data, mapping_config=mapping_config)

    with multiprocessing.Pool(processes=args.cores) as pool:
            # Use imap_unordered for potentially better performance if task order doesn't matter
            results_iterator = pool.imap_unordered(process_func, source_1_data_tuples, chunksize=args.multiproc_chunksize)
            for candidates in results_iterator:
                if candidates:
                    mappings_candidate.extend(candidates) # Collect candidates from the Combined approach

    # --- End Parallel Processing ---
    df_mappings_candidate = pd.DataFrame(mappings_candidate)
    print(f"Generated {len(mappings_candidate)} potential mappings using the Combined approach.")

    df_chunks = [df_mappings_candidate.iloc[i:i + mapping_config.request_item_size] for i in range(0, len(df_mappings_candidate), mapping_config.request_item_size)]
    dfs_json = [chunk.to_dict(orient='records') for chunk in df_chunks]
    
    prev_batch_inc = None
    for i in range(0, len(dfs_json)):
        df_chunk = dfs_json[i]
        current_batch_inc = ((max((i - 1), 0) * mapping_config.request_item_size) + len(df_chunk)) // mapping_config.batch_size
        if prev_batch_inc is None or current_batch_inc != prev_batch_inc:
            prev_batch_inc = current_batch_inc
            print(f"Prepping batch call {current_batch_inc + 1} / {len(dfs_json) * mapping_config.request_item_size // mapping_config.batch_size}") 
            
        messages = [
            {
                "role": "system",
                "content": f"""
{mapping_config.system_prompt}
Here are the mappings (in JSON format):
{json.dumps(df_chunk, indent=4).replace('\n', ' ')}
"""
            }
        ]

        completion = {
            "custom_id": f'mappings-{uuid.uuid4()}',
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": "gpt-4o",
                "temperature": 0,
                "messages": messages,
                "response_format": type_to_response_format_param(mapping_config.response_format),
            }
        }

        Batch_Gen_Util.add_to_batch(
            data=json.dumps(completion, indent=4).replace('\n', ' '),
            increment=len(df_chunk)
        )

    batches = Batch_Gen_Util.conclude_session()
    batch_recap = {
        "batches": batches
    }

    with open(f"{batch_folder}/batch_recap.json", 'w') as f:
        json.dump(batch_recap, f, indent=4)

def merge_with_mapping_and_save_results(df_1, df_2, df_mappings, merge_strategy, mapping_config: config.MappingConfig):
    intermediate_df = pd.merge(
        df_1,
        df_mappings,
        left_on=f"{mapping_config.source_1_prefix}_{mapping_config.source_1_id_cleaned}",
        right_on=mapping_config.source_1_mapping_column,
        how=merge_strategy
    )
    final_merged_df = pd.merge(
        intermediate_df,
        df_2,
        left_on=mapping_config.source_2_mapping_column,
        right_on=f"{mapping_config.source_2_prefix}_{mapping_config.source_2_id_cleaned}",
        how=merge_strategy
    )

    df_1_columns = df_1.columns.tolist()
    df_2_columns = df_2.columns.tolist()

    final_columns_to_keep = df_1_columns + [col for col in df_2_columns if col not in df_1_columns] + ['have_same_base', 'have_same_form', 'is_diluted']

    final_df = final_merged_df[final_columns_to_keep]
    print(f"Final DataFrame created with {len(final_df)} rows and {len(final_df.columns)} columns.")

    # --- Display Results & Save ---
    print("\nFinal DataFrame Head:")
    print(final_df.head())

    output_filename = f"outputs/{mapping_config.merge_output_filename_prefix}{'' if merge_strategy == 'inner' else '_all'}.csv"

    # Save the final result to a CSV
    try:
        final_df.to_csv(output_filename, index=False)
        print(f"\nSuccessfully saved final data to {output_filename}")
    except Exception as e:
        print(f"\nError saving final data to {output_filename}: {e}")


def process_batch(df_source_1, df_source_2, mapping_config: config.MappingConfig):
    batch_folder = f'{mapping_config.mapping_name}/batches'
    Batch_Ret_Util = batch_ret_util.BatchRetUtil(os.path.join(os.getcwd(), batch_folder))
    contents = Batch_Ret_Util.get_contents()

    print(f"Loaded {len(contents)} batches from {batch_folder}.")
    final_mappings = []
    for content in contents:
        for mapping in content['mappings']:
            if mapping_config.mapping_type == config.MappingType.SUBSTANCE:
                if mapping['have_same_base'] is True or mapping['have_same_form'] is True or mapping['is_diluted'] is True:
                    final_mapping = {}
                    final_mapping[mapping_config.source_1_mapping_column] = mapping['active_substance_1']
                    final_mapping[mapping_config.source_1_mapping_column_cleaned] = str_processing.cleaning_id(mapping['active_substance_1'])
                    final_mapping[mapping_config.source_2_mapping_column] = mapping['active_substance_2']
                    final_mapping[mapping_config.source_2_mapping_column_cleaned] = str_processing.cleaning_id(mapping['active_substance_2'])
                    final_mapping['have_same_base'] = mapping['have_same_base']
                    final_mapping['have_same_form'] = mapping['have_same_form']
                    final_mapping['is_diluted'] = mapping['is_diluted']
                    final_mappings.append(final_mapping)
            elif mapping_config.mapping_type == config.MappingType.SUPPLIER:
                if mapping['confidence_score_match_site_level'] >= 0.7 or mapping['confidence_score_are_part_of_same_company'] >= 0.7:
                    final_mapping = {}
                    final_mapping[mapping_config.source_1_mapping_column] = mapping['item_1']
                    final_mapping[mapping_config.source_1_mapping_column_cleaned] = str_processing.cleaning_id(mapping['item_1'])
                    final_mapping[mapping_config.source_1_is_supplier_site_column] = mapping['is_item_1_supplier_site']
                    final_mapping[mapping_config.source_2_mapping_column] = mapping['item_2']
                    final_mapping[mapping_config.source_2_mapping_column_cleaned] = str_processing.cleaning_id(mapping['item_2'])
                    final_mapping[mapping_config.source_1_is_supplier_site_column] = mapping['is_item_2_supplier_site']
                    final_mapping['confidence_score_match_site_level'] = mapping['confidence_score_match_site_level']
                    final_mapping['confidence_score_are_part_of_same_company'] = mapping['confidence_score_are_part_of_same_company']
                    final_mappings.append(final_mapping)

    # # prefix all source_1 keys with the mapping name
    # df_source_1 = df_source_1.rename(columns={col: f"{mapping_config.source_1_prefix}_{col}" for col in df_source_1.columns})
    # df_source_2 = df_source_2.rename(columns={col: f"{mapping_config.source_2_prefix}_{col}" for col in df_source_2.columns})
     

    with open(f'outputs/{mapping_config.mapping_output_filename}', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = list(final_mappings[0].keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(final_mappings)

        # df_final_mappings = pd.DataFrame(final_mappings)

        # # merge_strategies = ["left", "inner"]
        # merge_strategies = ["left"]
        # for merge_strategy in merge_strategies:
        #     merge_with_mapping_and_save_results(df_source_1, df_source_2, df_final_mappings, merge_strategy, mapping_config)
    pass

def merge_direct_and_save_results(df_1, df_2, merge_strategy, mapping_config: config.MappingConfig):
    final_merged_df = pd.merge(
        df_1,
        df_2,
        left_on=mapping_config.source_1_id,
        right_on=mapping_config.source_2_id,
        how=merge_strategy
    )

    df_1_columns = df_1.columns.tolist()
    df_2_columns = df_2.columns.tolist()

    final_columns_to_keep = df_1_columns + [col for col in df_2_columns if col not in df_1_columns]

    final_df = final_merged_df[final_columns_to_keep]
    print(f"Final DataFrame created with {len(final_df)} rows and {len(final_df.columns)} columns.")

    # --- Display Results & Save ---
    print("\nFinal DataFrame Head:")
    print(final_df.head())

    output_filename = f"outputs/{mapping_config.merge_output_filename_prefix}{'' if merge_strategy == 'inner' else '_all'}.csv"

    # Save the final result to a CSV
    try:
        final_df.to_csv(output_filename, index=False)
        print(f"\nSuccessfully saved final data to {output_filename}")
    except Exception as e:
        print(f"\nError saving final data to {output_filename}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='A mapping tool in the pharmaceutical industry.',
    )

    parser.add_argument('mapping_name', type=str, help='The name of the mapping to execute.')

    parser.add_argument('--cores', type=int, default=multiprocessing.cpu_count(), help='Number of CPU cores to use.')
    parser.add_argument('-g', '--generate', action='store_true', help='Start the mapping generation process.')
    parser.add_argument('-p', '--process', action='store_true', help='Process the batch files needed for the mapping.')
    parser.add_argument('-d', '--dry-run', action='store_true', help='Perform a dry run without executing the mapping.')
    parser.add_argument('--multiproc-chunksize', type=int, default=50, help='Chunk size for multiprocessing.')

    try:
        args = parser.parse_args()
    except SystemExit as e:
        # Catch SystemExit to prevent script termination on --help or error in some environments
        print(f"Argparse exited with code {e.code}")
        sys.exit(e.code)
        
    mapping_config = config.get_mapping_config(args.mapping_name)

    if args.generate:
        print(f"Starting mapping generation for: {args.mapping_name}")
        if args.dry_run:
            print("Dry run mode: No API calls will be made.")
        

        if mapping_config.mapping_type in [config.MappingType.SUPPLIER, config.MappingType.SUBSTANCE]:

            # check if the file exist in the inputs folder
            print(f"Loading files for mapping: {args.mapping_name} 1")
            if os.path.exists(f'inputs/{mapping_config.source_1_filename}'):
                df_source_1 = pd.read_csv(f'inputs/{mapping_config.source_1_filename}')
            elif os.path.exists(f'outputs/{mapping_config.source_1_filename}'):
                df_source_1 = pd.read_csv(f'outputs/{mapping_config.source_1_filename}')
            df_source_1[mapping_config.source_1_id_cleaned] = df_source_1[mapping_config.source_1_id].apply(str_processing.cleaning_id)
            df_source_1 = df_source_1[df_source_1[mapping_config.source_1_id_cleaned].notna() & df_source_1[mapping_config.source_1_id_cleaned] != '']

            print(f"Loading files for mapping: {args.mapping_name} 2")
            # check if the file exist in the inputs folder
            if os.path.exists(f'inputs/{mapping_config.source_2_filename}'):
                df_source_2 = pd.read_csv(f'inputs/{mapping_config.source_2_filename}')
            elif os.path.exists(f'outputs/{mapping_config.source_2_filename}'):
                df_source_2 = pd.read_csv(f'outputs/{mapping_config.source_2_filename}')
            
            df_source_2[mapping_config.source_2_id_cleaned] = df_source_2[mapping_config.source_2_id].apply(str_processing.cleaning_id)
            df_source_2 = df_source_2[df_source_2[mapping_config.source_2_id_cleaned].notna() & df_source_2[mapping_config.source_2_id_cleaned] != '']



            generate_batch()
        elif mapping_config.mapping_type == config.MappingType.MERGE:
            # merge_direct_and_save_results(df_source_1, df_source_2, 'left', mapping_config)
            pass
    elif args.process:
        print(f"Processing batch files for: {args.mapping_name}")
        if mapping_config.mapping_type in [config.MappingType.SUPPLIER, config.MappingType.SUBSTANCE]:
            # process_batch(df_source_1, df_source_2, mapping_config)
            process_batch(None, None, mapping_config)
        elif mapping_config.mapping_type == config.MappingType.MERGE:
            print("No batch files to process for merge type.")
        pass
    else:
        print("No action specified. Use -g to generate or -p to process.")

