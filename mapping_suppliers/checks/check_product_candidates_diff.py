import argparse
import sys
import pandas as pd
import os
import time
import logging
import multiprocessing
from functools import partial
import queue
from logging.handlers import QueueHandler, QueueListener

# Assuming utils structure exists
try:
    from utils import str_processing
    from utils import config
    from utils import fuzzy_matching
except ImportError as e:
    print(f"CRITICAL: Failed to import utils. Ensure utils package is accessible. Error: {e}", file=sys.stderr)
    sys.exit(1)


import rapidfuzz.fuzz as fuzz
import rapidfuzz.process as process

# --- Constants ---
FUZZY_MATCH_THRESHOLD_ORIG = 80       # Threshold for original string matching (and post-filter)
FUZZY_MATCH_THRESHOLD_CLEANED = 90    # Threshold for cleaned string matching (initial step)
SCORERS_ORIG = [fuzz.token_set_ratio, fuzz.ratio, fuzz.partial_ratio, fuzz.token_sort_ratio]
SCORERS_CLEANED = [fuzz.token_sort_ratio, fuzz.token_set_ratio] # Scorers for initial cleaned match
PROGRESS_UPDATE_INTERVAL = 100

# --- Worker Process Logging Initializer ---
def worker_log_init(log_queue):
    qh = QueueHandler(log_queue)
    root = logging.getLogger()
    if root.handlers:
        for h in root.handlers[:]: root.removeHandler(h)
    root.addHandler(qh)
    root.setLevel(logging.INFO) # Or set to DEBUG for more verbose worker logs

# --- Function to process a single source_1 item (for parallelization) ---
# Implements comparison: "Original Only" vs "Cleaned + Original Post-Filter"
def process_source1_item(source_1_tuple, source_2_data, mapping_type, thresholds):
    """
    Processes a source_1 item against source_2.
    Compares two approaches:
    1. Matching Original strings directly (Original Only).
    2. Matching Cleaned strings, then post-filtering with Original strings (Combined).
    Filters source_2 based on 'intermediate' status for SUBSTANCE mapping BEFORE matching.
    Returns discrepancy details between the two approaches & candidates from the Combined approach.
    """
    original_index_1, name_1_orig, name_1_cleaned, source1_has_intermediate = source_1_tuple
    threshold_orig = thresholds['orig']
    threshold_cleaned = thresholds['cleaned']

    # Unpack source_2 data
    s2_orig_list_full = source_2_data['orig_list']
    s2_cleaned_list_full = source_2_data['cleaned_list']
    s2_indices_full = source_2_data['indices']
    s2_has_intermediate_list = source_2_data['has_intermediate_list']
    s2_index_to_orig_full = source_2_data['index_to_orig']

    matches_original_only_indices = set()
    matches_combined_approach_indices = set() # Stores results of Cleaned + Original Post-Filter
    discrepancy_details_for_return = None
    item_candidates = [] # Candidates from the Combined approach

    # --- Pre-filter Source 2 based on 'intermediate' status ---
    if mapping_type == config.MappingType.SUBSTANCE:
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
        # --- Approach 1: Matching WITHOUT Cleaning (Original Only) ---
        # Use filtered list for matching
        cutoff_for_extract_orig = max(1, threshold_orig - 15)
        results_orig = process.extract(
            name_1_orig,
            filtered_s2_orig_list,
            scorer=fuzz.token_set_ratio, # Example primary scorer for extraction
            limit=None,
            score_cutoff=cutoff_for_extract_orig
        )
        for choice_orig, score, idx_in_filtered_list in results_orig:
            name_2_orig_filtered = filtered_s2_orig_list[idx_in_filtered_list]
            # Check *all* original scorers against the threshold
            all_scores_orig = [scorer(name_1_orig, name_2_orig_filtered) for scorer in SCORERS_ORIG]
            if any(s >= threshold_orig for s in all_scores_orig):
                original_s2_df_index = filtered_idx_to_orig_s2_df_idx[idx_in_filtered_list]
                matches_original_only_indices.add(original_s2_df_index)


        # --- Approach 2: Matching WITH Cleaning + Original Post-Filter (Combined) ---
        if name_1_cleaned and name_1_cleaned != "" and filtered_s2_cleaned_list:
            cutoff_for_extract_cleaned = max(1, threshold_cleaned - 15)
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

                if any(s >= threshold_cleaned for s in all_scores_cleaned):
                    # This item passed the CLEANED match check. Now apply post-filter.
                    original_s2_df_index = filtered_idx_to_orig_s2_df_idx[idx_in_filtered_list]
                    name_2_orig = s2_index_to_orig_full[original_s2_df_index] # Get original name 2

                    # Step 2.2: Post-filter using ORIGINAL names and ORIGINAL threshold
                    all_scores_orig_postfilter = [scorer(name_1_orig, name_2_orig) for scorer in SCORERS_ORIG]

                    if any(s >= threshold_cleaned - 10 for s in all_scores_orig_postfilter):
                        # Passed BOTH cleaned match and original post-filter
                        matches_combined_approach_indices.add(original_s2_df_index)


        # --- Discrepancy Check: Compare "Original Only" vs "Combined" ---
        diff_indices = matches_combined_approach_indices.symmetric_difference(matches_original_only_indices)
        if diff_indices:
            # logging.warning(f"--- Discrepancy Found for source_1: '{name_1_orig}' (Idx {original_index_1}) ---")
            diff_rows_data = {idx: s2_index_to_orig_full[idx] for idx in diff_indices} # Use full map
            indices_only_combined = matches_combined_approach_indices.difference(matches_original_only_indices)
            indices_only_original = matches_original_only_indices.difference(matches_combined_approach_indices)
            discrepancy_details_for_return = {
                'source_1_name': name_1_orig, 'source_1_index': original_index_1,
                'diff_items_indices': list(diff_indices), # Store indices for reference
                'diff_items_names': list(diff_rows_data.values()),
                'matched_only_combined': [s2_index_to_orig_full[idx] for idx in indices_only_combined], # Items found ONLY by Combined approach
                'matched_only_original': [s2_index_to_orig_full[idx] for idx in indices_only_original] # Items found ONLY by Original Only approach
            }

        # --- Collect Candidates based on the "Combined" approach results ---
        for original_s2_df_index in matches_combined_approach_indices:
            mapping = { "item_1": name_1_orig,
                        "item_2": s2_index_to_orig_full[original_s2_df_index],
                        "item_1_orig_index": original_index_1,
                        "item_2_orig_index": original_s2_df_index }
            item_candidates.append(mapping)

    except Exception as e:
        logging.error(f"ERROR processing source_1 item {original_index_1} ('{name_1_orig}'): {e}", exc_info=True)

    return discrepancy_details_for_return, item_candidates

# --- Main Execution ---
if __name__ == "__main__":
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description='Compare fuzzy matching: Original Only vs Combined (Cleaned + Original Post-Filter).')
    parser.add_argument('mapping_name', type=str, help='The name of the mapping config to use.')
    parser.add_argument('--cores', type=int, default=multiprocessing.cpu_count(), help='Number of CPU cores to use.')
    parser.add_argument('--chunksize', type=int, default=50, help='Chunk size for multiprocessing.')
    try: args = parser.parse_args()
    except SystemExit as e: print(f"Argparse exited: {e}", file=sys.stderr); sys.exit(e.code)

    # --- Logging Setup (Queue Listener) ---
    log_filename = f'check_product_candidates_diff.log' # Include mapping name
    log_filepath = os.path.abspath(log_filename)
    print(f"INFO: Log file target: {log_filepath}")
    if os.path.exists(log_filepath):
        try: os.remove(log_filepath)
        except OSError as e: print(f"WARN: Could not remove old log file {log_filepath}: {e}", file=sys.stderr)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO) # Show INFO level logs on console
    stream_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(stream_formatter)

    file_handler = None
    try:
        file_handler = logging.FileHandler(log_filepath, mode='w')
        file_handler.setLevel(logging.DEBUG) # Log everything to file
        file_formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
    except Exception as e:
        print(f"CRITICAL: Failed to create FileHandler for {log_filepath}: {e}\nFile logging will be DISABLED.\n", file=sys.stderr)

    handlers_for_listener_and_main = [stream_handler]
    if file_handler:
        handlers_for_listener_and_main.append(file_handler)
    else:
        print("WARN: File logging is disabled.", file=sys.stderr)

    listener = None; log_queue = None # Initialize
    try:
        log_queue = multiprocessing.Queue(-1)
        # Pass only handlers that should be used by the listener (workers will log via queue)
        listener_handlers = [h for h in handlers_for_listener_and_main] # Copy list
        listener = QueueListener(log_queue, *listener_handlers, respect_handler_level=True)
        listener.start()
        print("INFO: QueueListener started.")
    except Exception as e:
        print(f"CRITICAL: Failed Queue/Listener setup: {e}", file=sys.stderr)
        sys.exit(1) # Exit if listener fails

    # Setup logging for the main process (can also use handlers directly)
    logging.basicConfig(level=logging.DEBUG, # Set main process level
                        format='%(asctime)s - MAIN - %(levelname)s - %(message)s',
                        handlers=handlers_for_listener_and_main)

    logging.info(f"CWD: {os.getcwd()}")
    # --- End Logging Setup ---

    logging.info(f"Starting Comparison: {args.mapping_name}, Cores: {args.cores}, Chunk: {args.chunksize}")
    logging.info(f"Thresholds -> Original: {FUZZY_MATCH_THRESHOLD_ORIG}, Cleaned (Initial): {FUZZY_MATCH_THRESHOLD_CLEANED}")
    logging.info(f"Scorers Original: {[s.__name__ for s in SCORERS_ORIG]}")
    logging.info(f"Scorers Cleaned: {[s.__name__ for s in SCORERS_CLEANED]}")

    all_candidates_combined = []; all_discrepancies_found = []
    listener_obj = listener; queue_obj = log_queue # For finally block

    try:
        # --- Config & Data ---
        logging.info("Load config...")
        mapping_config = config.get_mapping_config(args.mapping_name)
        mapping_type = mapping_config.mapping_type
        logging.info(f"Mapping Type: {mapping_type}")

        logging.info("Load data...")
        s1_fp = f'inputs/{mapping_config.source_1_filename}'
        if not os.path.exists(s1_fp): raise FileNotFoundError(f"Source 1 file not found: {s1_fp}")
        df_source_1 = pd.read_csv(s1_fp)

        s2_fp = f'inputs/{mapping_config.source_2_filename}'
        if not os.path.exists(s2_fp): raise FileNotFoundError(f"Source 2 file not found: {s2_fp}")
        df_source_2 = pd.read_csv(s2_fp)

        logging.info("Get unique items...")
        unique_items_1 = str_processing.get_unique_items(df_source_1, mapping_config.source_1_id, mapping_config.source_1_separator)
        df_unique_source_1 = pd.DataFrame(unique_items_1, columns=['original_name']).dropna(subset=['original_name'])
        unique_items_2 = str_processing.get_unique_items(df_source_2, mapping_config.source_2_id, mapping_config.source_2_separator)
        df_unique_source_2 = pd.DataFrame(unique_items_2, columns=['original_name']).dropna(subset=['original_name'])
        logging.info(f"Unique Source 1: {len(df_unique_source_1)}, Unique Source 2: {len(df_unique_source_2)}")
        if len(df_unique_source_1) == 0 or len(df_unique_source_2) == 0:
             logging.warning("One or both unique item lists are empty. Exiting.")
             sys.exit(0)
        logging.info(f"Potential comparisons: {len(df_unique_source_1) * len(df_unique_source_2):,}")


        # --- Pre-compute Intermediate Status ---
        # Only relevant if mapping_type is SUBSTANCE
        if mapping_type == config.MappingType.SUBSTANCE:
            logging.info("Pre-computing 'intermediate' status for SUBSTANCE mapping...")
            df_unique_source_1['has_intermediate'] = df_unique_source_1['original_name'].str.lower().str.contains('intermediate', na=False, regex=False)
            df_unique_source_2['has_intermediate'] = df_unique_source_2['original_name'].str.lower().str.contains('intermediate', na=False, regex=False)
            logging.info(f"Intermediate counts: Source 1: {df_unique_source_1['has_intermediate'].sum()}, Source 2: {df_unique_source_2['has_intermediate'].sum()}")
        else:
            logging.info("Skipping 'intermediate' status computation (not SUBSTANCE type).")
            # Add columns with default False to avoid errors later if accessed
            df_unique_source_1['has_intermediate'] = False
            df_unique_source_2['has_intermediate'] = False


        logging.info("Cleaning names...")
        start_clean = time.time()
        # Determine cleaning function based on mapping type
        if mapping_type == config.MappingType.SUBSTANCE:
            clean_func = fuzzy_matching.clean_product_name_aggressively_pharma
            logging.info("Using SUBSTANCE cleaning function.")
        elif mapping_type == config.MappingType.SUPPLIER:
            clean_func = fuzzy_matching.clean_supplier_name_aggressively_pharma
            logging.info("Using SUPPLIER cleaning function.")
        else:
            # Default or handle unknown types
            clean_func = fuzzy_matching.clean_product_name_aggressively_pharma
            logging.warning(f"Unknown mapping type '{mapping_type}'. Defaulting to PRODUCT cleaning function.")

        df_unique_source_1['cleaned_name'] = df_unique_source_1['original_name'].apply(clean_func)
        df_unique_source_2['cleaned_name'] = df_unique_source_2['original_name'].apply(clean_func)
        logging.info(f"Cleaning done ({time.time() - start_clean:.2f} sec).")


        logging.info("Preparing data for parallel processing...")
        # Ensure index is standard range index for easy mapping if reset_index wasn't used
        df_unique_source_1 = df_unique_source_1.reset_index(drop=True)
        df_unique_source_2 = df_unique_source_2.reset_index(drop=True)

        source_1_data_tuples = list(zip(
            df_unique_source_1.index,
            df_unique_source_1['original_name'],
            df_unique_source_1['cleaned_name'],
            df_unique_source_1['has_intermediate'] # Already computed or set to False
        ))
        source_2_data = {
            'orig_list': df_unique_source_2['original_name'].tolist(),
            'cleaned_list': df_unique_source_2['cleaned_name'].tolist(),
            'indices': df_unique_source_2.index.tolist(), # Index of row in df_unique_source_2
            'index_to_orig': df_unique_source_2['original_name'].to_dict(), # Map df index to orig name
            'has_intermediate_list': df_unique_source_2['has_intermediate'].tolist() # Already computed or set to False
        }
        source_1_length = len(source_1_data_tuples); source_2_length = len(source_2_data['orig_list'])
        logging.info(f"Prepared {source_1_length} Source 1 items and {source_2_length} Source 2 items.")


        # --- Parallel Processing ---
        start_proc = time.time(); processed_count = 0; start_prog = time.time()
        thresholds = {'orig': FUZZY_MATCH_THRESHOLD_ORIG, 'cleaned': FUZZY_MATCH_THRESHOLD_CLEANED}

        logging.info("Creating partial function for worker process...")
        process_func = partial(process_source1_item, source_2_data=source_2_data, mapping_type=mapping_type, thresholds=thresholds)

        logging.info(f"Creating multiprocessing Pool with {args.cores} workers...")
        # Use try-with-resources for the pool
        with multiprocessing.Pool(processes=args.cores, initializer=worker_log_init, initargs=(log_queue,)) as pool:
            logging.info(f"Pool created. Submitting tasks (Chunksize: {args.chunksize})...")
            # Use imap_unordered for potentially better performance if task order doesn't matter
            results_iterator = pool.imap_unordered(process_func, source_1_data_tuples, chunksize=args.chunksize)
            logging.info("Iterating through results as they complete...")
            for discrepancy_info, candidates in results_iterator:
                if discrepancy_info:
                    all_discrepancies_found.append(discrepancy_info)
                    # Log discrepancy details immediately if needed (might be verbose)
                    # logging.debug(f"Discrepancy found for Src1 Idx {discrepancy_info.get('source_1_index', 'N/A')}")
                if candidates:
                    all_candidates_combined.extend(candidates) # Collect candidates from the Combined approach

                # --- Progress Update ---
                processed_count += 1
                current_time = time.time()
                if processed_count == 1 or current_time - start_prog >= 5.0 or processed_count % PROGRESS_UPDATE_INTERVAL == 0 or processed_count == source_1_length:
                     elapsed_total = current_time - start_proc
                     items_per_second = processed_count / elapsed_total if elapsed_total > 0 else 0
                     eta_seconds = (source_1_length - processed_count) / items_per_second if items_per_second > 0 else 0
                     eta_seconds = max(0, min(eta_seconds, 86400*99)) # Cap ETA
                     eta_str = time.strftime("%H:%M:%S", time.gmtime(eta_seconds)) if items_per_second > 0 and processed_count < source_1_length else "N/A"
                     logging.info(f"Progress: {processed_count}/{source_1_length} ({processed_count/source_1_length*100:.1f}%) | Rate: {items_per_second:.2f} items/sec | ETA: {eta_str}")
                     start_prog = current_time # Reset timer for next interval update check

            logging.info("Finished iterating through all results.")
        logging.info("Multiprocessing Pool closed.")
        # --- End Parallel ---

        proc_time = time.time() - start_proc
        logging.info(f"Total processing time: {proc_time / 60:.2f} minutes.")
        logging.info(f"Generated {len(all_candidates_combined)} potential mappings using the Combined approach.")
        logging.info(f"Found {len(all_discrepancies_found)} discrepancy cases between 'Original Only' and 'Combined' approaches.")


        # --- Discrepancy Recap ---
        if all_discrepancies_found:
            logging.info("\n" + "="*20 + " DISCREPANCY RECAP ('Original Only' vs 'Combined') " + "="*20)
            logging.info(f"Found {len(all_discrepancies_found)} source_1 items where the two matching approaches yielded different results.")
            # Sort discrepancies by source_1_index for easier reading
            all_discrepancies_found.sort(key=lambda x: x.get('source_1_name', 1))
            for i, disc in enumerate(all_discrepancies_found):
                logging.info(f"\n--- Case {i+1}/{len(all_discrepancies_found)} ---")
                logging.info(f"Src1 (Idx {disc.get('source_1_index', 'N/A')}): '{disc.get('source_1_name', 'N/A')}'")
                matched_only_combined = disc.get('matched_only_combined', [])
                matched_only_original = disc.get('matched_only_original', [])
                if matched_only_combined:
                    logging.info(f"  -> Matched Src2 ONLY via Combined (Cleaned + Orig Filter):")
                    for item in matched_only_combined: logging.info(f"       - {item}")
                if matched_only_original:
                    logging.info(f"  -> Matched Src2 ONLY via Original Only:")
                    for item in matched_only_original: logging.info(f"       - {item}")
            logging.info("="*78 + "\n") # Adjust length to match header
        else:
            logging.info("No discrepancies found between the 'Original Only' and 'Combined' matching approaches.")
        # --- End Recap ---


        # --- Save Final Candidates (from Combined approach) ---
        if all_candidates_combined:
            try:
                df_candidates = pd.DataFrame(all_candidates_combined)
                # Define output filename based on mapping name and approach
                output_dir = "outputs"
                os.makedirs(output_dir, exist_ok=True)
                candidates_filename = os.path.join(output_dir, f"{args.mapping_name}_candidates_combined.csv")
                df_candidates.to_csv(candidates_filename, index=False)
                logging.info(f"Saved {len(df_candidates)} candidates from the Combined approach to: {candidates_filename}")
            except Exception as e:
                logging.error(f"Failed to save candidates CSV: {e}", exc_info=True)
        else:
            logging.info("No candidates generated by the Combined approach to save.")

    # --- Error Handling ---
    except FileNotFoundError as e:
        logging.error(f"Input File Error: {e}")
        sys.exit(1)
    except (KeyError, AttributeError) as e:
        logging.error(f"Configuration or Data Access Error: Check mapping config keys (e.g., source_1_id) or DataFrame columns. Error: {e}", exc_info=True)
        sys.exit(1)
    except ImportError as e:
         logging.error(f"Import Error during execution (might indicate missing dependencies or utils): {e}", exc_info=True)
         sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred during main execution: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # --- Shutdown ---
        logging.info("Shutdown sequence initiated: Entering finally block...")
        if 'listener_obj' in locals() and listener_obj is not None and listener_obj.is_alive():
             if 'queue_obj' in locals() and queue_obj is not None:
                 try:
                     logging.info("Signalling workers to stop logging and closing log queue...")
                     # Give workers a moment to potentially finish logging
                     time.sleep(0.5)
                     queue_obj.put_nowait(None) # Signal listener to stop
                     queue_obj.close()
                     queue_obj.join_thread()
                     logging.info("Log queue closed.")
                 except Exception as e:
                     logging.error(f"Error during log queue closing: {e}", exc_info=True)
                     print(f"ERROR: Log queue close error: {e}", file=sys.stderr) # Also print directly

             try:
                 logging.info("Stopping QueueListener...")
                 listener_obj.stop()
                 # Join should happen automatically if started as daemon, but explicit stop is good.
                 logging.info("QueueListener stopped.")
             except Exception as e:
                 logging.error(f"Error stopping QueueListener: {e}", exc_info=True)
                 print(f"ERROR: Listener stop error: {e}", file=sys.stderr) # Also print directly
        elif 'listener_obj' in locals() and listener_obj is not None:
             logging.warning("Listener object exists but is not alive.")
        else:
             logging.info("QueueListener object not found or not started.")
             print("INFO: Listener not referenced or started.", file=sys.stderr)

        # Ensure all handlers are closed, especially the file handler
        # This part runs in the main process
        main_handlers = logging.getLogger().handlers[:]
        for handler in main_handlers:
            try:
                handler.close()
                logging.getLogger().removeHandler(handler)
                # print(f"DEBUG: Closed and removed handler {handler}", file=sys.stderr) # Debug print
            except Exception as e:
                 # This might happen if handler was already closed or doesn't support close
                 print(f"WARN: Error closing handler {handler}: {e}", file=sys.stderr)

        print("INFO: Main process logging handlers closed.", file=sys.stderr)

    logging.info("Script finished.")
    print("INFO: Script finished.", file=sys.stderr) # Final confirmation print