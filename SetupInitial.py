#!/usr/bin/env python3
import os
import subprocess
import shutil
import gzip
import glob
import duckdb
import logging
import math
import json
import re
import time
import zstandard as zstd
import concurrent.futures
import threading
import gc
import random
import queue
from collections import defaultdict, deque

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("processing.log"), logging.StreamHandler()])
logger = logging.getLogger()

# Script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Configurable parameters
FIELD_PRESENCE_THRESHOLD = 0.50  # Field must be present in at least 50% of records (changed from 50%)
AAC_FIELD_PRESENCE_THRESHOLD = 0.50  # Same threshold for AAC records, adjust if needed

# Patterns to identify different datasets
DATASET_PATTERNS = {
    'records': re.compile(r'aarecords_(?!metadata|journals|digital_lending)..json.gz'),
    'metadata': re.compile(r'aarecords_metadata.*json.gz'),
    'journals': re.compile(r'aarecords_journals.*json.gz'),
    'digital_lending': re.compile(r'aarecords_digital_lending.*json.gz')
}

# Pattern for AAC datasets
AAC_PATTERN = re.compile(r'annas_archive_meta__aacid__(.+?)__.*\.jsonl(?:\.seekable)?\.zst')

# Thread-safe dictionaries for tracking field counts and record counts
DATASET_FIELDS_LOCK = threading.Lock()
DATASET_RECORD_COUNTS_LOCK = threading.Lock()
DATASET_FIELDS = defaultdict(lambda: defaultdict(int))
DATASET_RECORD_COUNTS = defaultdict(int)


#VALUES FOR 30GB OF RAM
# OPTIMIZED SETTINGS
MAX_WORKERS_ANALYSIS = 50  # Increased to 50 for analysis
MAX_WORKERS_CONVERSION = 4
MAX_WORKERS_MERGE = 4      # For merging parquet files
mega_batch_size = 500      # The last merge
FILE_SAMPLE_RATE = 0.20    # Analyze only 20% of files
RAM_MERGE="30GB"
CPU = 16


# Target size for split files (in MB)
TARGET_SPLIT_SIZE_MB = 10  # 10MB parts for better parallelization

def create_directories():
    """Creates necessary directories if they don't exist."""
    dirs = [
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearch'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearch_json'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux_json'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearch_parts'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux_parts'),
        os.path.join(SCRIPT_DIR, 'data', 'aac'),
        os.path.join(SCRIPT_DIR, 'data', 'aac_json'),
        os.path.join(SCRIPT_DIR, 'data', 'aac_parts'),
        os.path.join(SCRIPT_DIR, 'data', 'torrent'),
        os.path.join(SCRIPT_DIR, 'data', 'error'),
        os.path.join(SCRIPT_DIR, 'reports'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearchF'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearchauxF'),
        os.path.join(SCRIPT_DIR, 'data', 'aacF')
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)
        logging.info(f"Created directory: {d}")

def determine_dataset(filename):
    """Determines which dataset a file belongs to based on its name."""
    # First check for AAC files
    aac_match = AAC_PATTERN.match(filename)
    if aac_match:
        return f"aac_{aac_match.group(1)}"
        
    # Then check for regular datasets
    for dataset, pattern in DATASET_PATTERNS.items():
        if pattern.match(filename):
            return dataset
    return 'other'  # Default category if no pattern matches

def find_and_move_files():
    """
    Searches for files in the structure:
    data/torrent/aa*/elasticsearch/.gz and data/torrent/aa/elasticsearchaux/*.gz
    Then moves files larger than 2 MB to data/elasticsearch and data/elasticsearchaux respectively.
    """
    min_size = 2 * 1024 * 1024  # 2MB in bytes
    moved_files = 0

    torrent_dir = os.path.join(SCRIPT_DIR, 'data', 'torrent')
    aa_dirs = glob.glob(os.path.join(torrent_dir, 'aa*'))

    if not aa_dirs:
        logging.warning("No directories starting with 'aa' found in data/torrent")
        return

    for aa_dir in aa_dirs:
        for folder in ['elasticsearch', 'elasticsearchaux', 'aac']:
            source_dir = os.path.join(aa_dir, folder)
            if os.path.exists(source_dir) and os.path.isdir(source_dir):
                target_dir = os.path.join(SCRIPT_DIR, 'data', folder)
                
                # Different patterns based on the folder
                if folder == 'aac':
                    files = glob.glob(os.path.join(source_dir, '*.zst'))
                else:
                    files = glob.glob(os.path.join(source_dir, '*.gz'))
                    
                for file_path in files:
                    if os.path.getsize(file_path) >= min_size:
                        file_name = os.path.basename(file_path)
                        target_path = os.path.join(target_dir, file_name)
                        shutil.move(file_path, target_path)
                        moved_files += 1
                        logging.info(f"Moved {file_name} to {target_dir}")

    if moved_files == 0:
        logging.warning("No files were moved. Verify that structure and sizes are correct.")
    else:
        logging.info(f"Total files moved: {moved_files}")

def count_lines_and_avg_size(file_path):
    """Efficiently counts the number of lines in a file and calculates average line size."""
    logging.info(f"Counting lines in {os.path.basename(file_path)}...")
    
    total_size = os.path.getsize(file_path)
    
    # Count total lines
    result = subprocess.run(["wc", "-l", file_path], capture_output=True, text=True)
    line_count = int(result.stdout.strip().split()[0])
    
    # Calculate average bytes per line
    avg_bytes_per_line = total_size / line_count if line_count > 0 else 0
    
    logging.info(f"Total lines: {line_count}, avg bytes/line: {avg_bytes_per_line:.2f}")
    return line_count, avg_bytes_per_line

def extract_nested_fields(data, prefix=""):
    """
    Recursively extracts all fields from a nested JSON using dot notation.
    Returns a set of field paths.
    """
    fields = set()
    queue = deque([(data, prefix)])

    while queue:
        current_data, current_prefix = queue.popleft()
        if isinstance(current_data, dict):
            for key, value in current_data.items():
                field_path = f"{current_prefix}.{key}" if current_prefix else key
                fields.add(field_path)
                if isinstance(value, (dict, list)):
                    queue.append((value, field_path))
        elif isinstance(current_data, list):
            if current_data and isinstance(current_data[0], (dict, list)):
                array_path = f"{current_prefix}[0]" if current_prefix else "[0]"
                queue.append((current_data[0], array_path))

    return fields

def analyze_json_structure(file_path, dataset_name):
    """Analyzes entire JSON file and counts field frequencies."""
    logging.info(f"Analyzing ALL records in {os.path.basename(file_path)} for dataset: {dataset_name}")
    record_count = 0
    local_fields = defaultdict(int)
    
    with open(file_path, 'r') as f:
        for line in f:
            try:
                record = json.loads(line.strip())
                record_count += 1
                all_fields = extract_nested_fields(record)
                for field in all_fields:
                    local_fields[field] += 1
            except json.JSONDecodeError:
                logging.warning(f"Invalid JSON record in {os.path.basename(file_path)}")
                continue
    
    # Thread-safe update of global counters - APPLYING 10X SCALING FACTOR
    with DATASET_FIELDS_LOCK:
        for field, count in local_fields.items():
            # Apply 10x scaling because we only analyzed 10% of files
            DATASET_FIELDS[dataset_name][field] += count * 10
    
    with DATASET_RECORD_COUNTS_LOCK:
        # Apply 10x scaling to record count too
        DATASET_RECORD_COUNTS[dataset_name] += record_count * 10
    
    logging.info(f"Processed ALL {record_count} records from {os.path.basename(file_path)}")
    logging.info(f"Found {len(local_fields)} unique fields")
    return record_count

def split_for_target_size(json_path, parts_dir, file_name, recursive=True):
    """
    Split a file into parts of approximately 10MB (based on line count).
    Returns a list of all generated part files.
    """
    logging.info(f"Splitting {os.path.basename(json_path)} for target size of {TARGET_SPLIT_SIZE_MB}MB...")
    
    # Get total lines and average line size
    total_lines, avg_bytes_per_line = count_lines_and_avg_size(json_path)
    
    # Calculate how many lines to achieve target size
    target_size_bytes = TARGET_SPLIT_SIZE_MB * 1024 * 1024
    lines_per_file = math.ceil(target_size_bytes / avg_bytes_per_line)
    
    # Calculate total number of parts
    num_parts = math.ceil(total_lines / lines_per_file)
    
    logging.info(f"Splitting into approximately {num_parts} parts of {lines_per_file} lines each")
    
    # Run the split command
    split_prefix = os.path.join(parts_dir, f"{file_name}_part_")
    
    # Determine correct extension
    file_ext = ".json"
    if json_path.endswith(".jsonl"):
        file_ext = ".jsonl"
    
    subprocess.run([
        "split", 
        "-d", 
        f"--additional-suffix={file_ext}", 
        f"--lines={lines_per_file}", 
        json_path, 
        split_prefix
    ], check=True)
    
    # Get list of created part files
    part_files = sorted(glob.glob(f"{split_prefix}*"))
    logging.info(f"Created {len(part_files)} part files")
    
    # Check if any parts are still very large and need further splitting
    all_part_files = []
    
    for part_file in part_files:
        file_size_mb = os.path.getsize(part_file) / (1024 * 1024)
        
        if file_size_mb > TARGET_SPLIT_SIZE_MB * 2 and recursive:  # If more than double our target
            logging.info(f"Part file {os.path.basename(part_file)} is still {file_size_mb:.2f}MB, splitting further")
            
            # Define subpart naming
            sub_file_name = os.path.splitext(os.path.basename(part_file))[0]
            
            # Recursively split the large part
            sub_part_files = split_for_target_size(part_file, parts_dir, sub_file_name, recursive=True)
            
            # Add subparts to the result list
            all_part_files.extend(sub_part_files)
            
            # Remove the original large part file
            os.remove(part_file)
            logging.info(f"Removed large part file after further splitting: {os.path.basename(part_file)}")
        else:
            all_part_files.append(part_file)
    
    return all_part_files

def decompress_zst_file(zst_file, output_file):
    """Decompress a Zstandard compressed file."""
    logging.info(f"Decompressing {os.path.basename(zst_file)}...")
    
    with open(zst_file, 'rb') as f_in:
        dctx = zstd.ZstdDecompressor()
        with open(output_file, 'wb') as f_out:
            dctx.copy_stream(f_in, f_out)
    
    logging.info(f"Decompressed to {os.path.basename(output_file)}")

def decompress_file(compressed_file, json_dir, compression):
    """Decompress a single compressed file."""
    base_name = os.path.basename(compressed_file)
    
    if compression == 'gzip':
        file_pattern = r'(.+)\.json\.gz'
    else:  # zstd
        file_pattern = r'(.+)\.jsonl\.seekable\.zst'
    
    file_name = re.match(file_pattern, base_name).group(1)
    
    if compression == 'gzip':
        json_name = f"{file_name}.json"
    else:
        json_name = f"{file_name}.jsonl"
        
    json_path = os.path.join(json_dir, json_name)
    
    logging.info(f"Decompressing {base_name}...")
    
    try:
        # Decompress the file
        if compression == 'gzip':
            with gzip.open(compressed_file, 'rb') as f_in:
                with open(json_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:  # zstd
            decompress_zst_file(compressed_file, json_path)
        
        logging.info(f"Successfully decompressed {base_name}")
        return json_path
        
    except Exception as e:
        logging.error(f"Error decompressing {base_name}: {e}")
        return None

def process_directory(config):
    """
    Process an entire directory with pipeline parallelism:
    - Files begin splitting as soon as they're decompressed
    - Analysis starts as soon as split parts are available
    - No waiting for all files to complete a stage before starting the next
    """
    source_dir = config['source_dir']
    json_dir = config['json_dir']
    parts_dir = config['parts_dir']
    target_dir = config['target_dir']
    compression = config['compression']
    
    # Clear out old field data for this processing batch
    global DATASET_FIELDS, DATASET_RECORD_COUNTS
    DATASET_FIELDS.clear()
    DATASET_RECORD_COUNTS.clear()
    
    # 1. Find all compressed files in this directory
    if compression == 'gzip':
        compressed_files = glob.glob(os.path.join(source_dir, '*.json.gz'))
    else:  # zstd
        compressed_files = glob.glob(os.path.join(source_dir, '*.zst'))
    
    if not compressed_files:
        logging.info(f"No compressed files found in {source_dir}, skipping directory")
        return
    
    logging.info(f"Processing directory {source_dir} with {len(compressed_files)} compressed files")
    
    # Thread-safe queues for pipeline stages
    decompress_queue = queue.Queue()  # Files to decompress
    split_queue = queue.Queue()       # Files ready to split
    analysis_parts = []               # Tracks all split parts for analysis
    
    # Fill decompress queue with all files
    for file in compressed_files:
        decompress_queue.put(file)
    
    # Track all split parts for each dataset
    all_split_parts = defaultdict(list)
    split_parts_lock = threading.Lock()
    
    # Track completed files for progress reporting
    completed_decompress = 0
    completed_split = 0
    completed_decompress_lock = threading.Lock()
    completed_split_lock = threading.Lock()
    
    def decompress_worker():
        """Worker thread to decompress files and add to split queue"""
        nonlocal completed_decompress
        while True:
            try:
                # Get next file to decompress (non-blocking)
                try:
                    compressed_file = decompress_queue.get_nowait()
                except queue.Empty:
                    break
                
                # Decompress the file
                json_file = decompress_file(compressed_file, json_dir, compression)
                
                # If successful, add to split queue
                if json_file and os.path.exists(json_file):
                    split_queue.put(json_file)
                
                # Update progress
                with completed_decompress_lock:
                    completed_decompress += 1
                    logging.info(f"Decompression progress: {completed_decompress}/{len(compressed_files)} files")
                
                # Mark task as done
                decompress_queue.task_done()
                
            except Exception as e:
                logging.error(f"Error in decompress worker: {str(e)}")
    
    def split_worker():
        """Worker thread to split decompressed files"""
        nonlocal completed_split
        while True:
            try:
                # Check if we're done and no more files to decompress
                if decompress_queue.empty() and split_queue.empty() and completed_decompress == len(compressed_files):
                    break
                
                # Try to get a file to split (non-blocking)
                try:
                    json_file = split_queue.get_nowait()
                except queue.Empty:
                    # If no files ready yet, sleep briefly and check again
                    time.sleep(0.1)
                    continue
                
                # Process this file
                if not json_file or not os.path.exists(json_file):
                    split_queue.task_done()
                    continue
                
                # Split the file
                file_name = os.path.splitext(os.path.basename(json_file))[0]
                try:
                    # Split the file into parts
                    parts = split_for_target_size(json_file, parts_dir, file_name)
                    
                    # Determine dataset for these parts
                    if compression == 'gzip':
                        full_name = f"{file_name}.json.gz"
                    else:  # zstd
                        full_name = f"{file_name}.jsonl.seekable.zst"
                    
                    dataset_name = determine_dataset(full_name)
                    
                    # Add parts to the appropriate dataset group
                    with split_parts_lock:
                        all_split_parts[dataset_name].extend(parts)
                    
                    # Add to analysis list
                    analysis_parts.extend(parts)
                    
                    # Remove original JSON file
                    os.remove(json_file)
                    logging.info(f"Removed {os.path.basename(json_file)} after splitting")
                    
                except Exception as e:
                    logging.error(f"Error splitting {os.path.basename(json_file)}: {str(e)}")
                
                # Update progress
                with completed_split_lock:
                    completed_split += 1
                    logging.info(f"Split progress: {completed_split}/{len(compressed_files)} files")
                
                # Mark task as done
                split_queue.task_done()
                
            except Exception as e:
                logging.error(f"Error in split worker: {str(e)}")
    
    # Start pipeline workers
    decompress_threads = []
    for i in range(min(MAX_WORKERS_ANALYSIS, len(compressed_files))):
        t = threading.Thread(target=decompress_worker)
        t.daemon = True
        t.start()
        decompress_threads.append(t)
    
    split_threads = []
    for i in range(min(MAX_WORKERS_ANALYSIS // 2, len(compressed_files))):
        t = threading.Thread(target=split_worker)
        t.daemon = True
        t.start()
        split_threads.append(t)
    
    # Wait for all decompression to complete
    for t in decompress_threads:
        t.join()
    
    # Wait for all splitting to complete
    for t in split_threads:
        t.join()
    
    logging.info(f"All files decompressed and split. Found {len(all_split_parts)} datasets.")
    
    # Now that all files are split and grouped by dataset, sample and analyze each dataset
    for dataset_name, parts in all_split_parts.items():
        logging.info(f"Dataset {dataset_name} has {len(parts)} split files")
        
        # Sample 10% of files randomly for this dataset
        sample_size = max(1, int(len(parts) * FILE_SAMPLE_RATE))
        sampled_files = random.sample(parts, sample_size)
        
        logging.info(f"Analyzing {sample_size} files ({FILE_SAMPLE_RATE*100:.1f}%) for dataset {dataset_name}")
        
        # Analyze all sampled files in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_ANALYSIS) as executor:
            # Submit analysis tasks
            futures = [
                executor.submit(analyze_json_structure, file, dataset_name)
                for file in sampled_files
            ]
            
            # Collect all results
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error during JSON analysis: {e}")
    
    # Write field reports for this directory
    write_field_reports_for_directory(source_dir)
    
    # Now convert all files to Parquet
    convert_to_parquet(all_split_parts, target_dir)
    
    # Merge Parquet files for this directory
    merge_parquet_files_for_directory(config)
    
    # Cleanup temporary files for this directory
    cleanup_temp_files(json_dir, parts_dir)
    
    logging.info(f"Completed processing directory {source_dir}")


def get_common_fields(dataset_name, threshold=None):
    """
    Calculates fields that appear in at least a percentage (threshold) of records.
    Returns a list of tuples (field_path, sql_column_name).
    """
    if threshold is None:
        # Use different thresholds based on dataset type
        if dataset_name.startswith('aac_'):
            threshold = AAC_FIELD_PRESENCE_THRESHOLD
        else:
            threshold = FIELD_PRESENCE_THRESHOLD
    
    if dataset_name not in DATASET_RECORD_COUNTS:
        return []

    total_records = DATASET_RECORD_COUNTS[dataset_name]
    if total_records == 0:
        return []
        
    threshold_count = total_records * threshold
    common_fields = []

    for field, count in DATASET_FIELDS[dataset_name].items():
        if count >= threshold_count:
            # Create SQL-friendly column name
            column_name = field.replace('.', '_').replace('[', '_').replace(']', '_')
            common_fields.append((field, column_name))

    return sorted(common_fields)

def get_combined_fields_for_dataset_group(dataset_group):
    """
    Creates a combined set of fields from all datasets in a group.
    Returns a list of field info dicts to use for Parquet generation.
    """
    # Identify datasets in the group
    if dataset_group == 'elasticsearchaux':
        datasets = ['metadata', 'journals', 'digital_lending']
    elif dataset_group == 'aac':
        datasets = [d for d in DATASET_RECORD_COUNTS.keys() if d.startswith('aac_')]
    else:
        return []
    
    # Get the common fields for each dataset in the group
    all_fields = {}
    
    for dataset in datasets:
        if dataset not in DATASET_RECORD_COUNTS or DATASET_RECORD_COUNTS[dataset] == 0:
            continue
        
        # Use threshold based on dataset type
        if dataset.startswith('aac_'):
            threshold = AAC_FIELD_PRESENCE_THRESHOLD
        else:
            threshold = FIELD_PRESENCE_THRESHOLD
            
        for field_path, column_name in get_common_fields(dataset, threshold):
            if field_path not in all_fields:
                all_fields[field_path] = {
                    'field': field_path,
                    'column': column_name,
                    'datasets': [dataset]
                }
            else:
                all_fields[field_path]['datasets'].append(dataset)
    
    return list(all_fields.values())

def find_common_fields_across_datasets(dataset_group):
    """
    Finds fields that are common across all datasets in a group.
    Returns a sorted list of field names.
    """
    # Identify datasets in the group
    if dataset_group == 'elasticsearchaux':
        datasets = ['metadata', 'journals', 'digital_lending']
    elif dataset_group == 'aac':
        datasets = [d for d in DATASET_RECORD_COUNTS.keys() if d.startswith('aac_')]
    else:
        return []
    
    # Only consider datasets that have records
    active_datasets = [d for d in datasets if d in DATASET_RECORD_COUNTS and DATASET_RECORD_COUNTS[d] > 0]
    
    if len(active_datasets) <= 1:
        return []
    
    # Get common fields for each dataset using appropriate threshold
    dataset_common_fields = {}
    for dataset in active_datasets:
        # Use threshold based on dataset type
        if dataset.startswith('aac_'):
            threshold = AAC_FIELD_PRESENCE_THRESHOLD
        else:
            threshold = FIELD_PRESENCE_THRESHOLD
            
        fields = set(field for field, _ in get_common_fields(dataset, threshold))
        dataset_common_fields[dataset] = fields
    
    # Find intersection of all sets
    common_fields = set.intersection(*dataset_common_fields.values()) if dataset_common_fields else set()
    return sorted(common_fields)

def write_field_reports_for_directory(source_dir):
    """Writes reports about field frequencies and common fields for this directory."""
    reports_dir = os.path.join(SCRIPT_DIR, 'reports')
    dir_name = os.path.basename(source_dir)
    
    # Determine dataset group based on directory
    if 'elasticsearchaux' in dir_name:
        dataset_group = 'elasticsearchaux'
    elif 'elasticsearch' in dir_name:
        dataset_group = 'elasticsearch'
    elif 'aac' in dir_name:
        dataset_group = 'aac'
    else:
        dataset_group = dir_name
    
    # Get combined fields for dataset groups
    combined_fields = []
    if dataset_group == 'elasticsearchaux':
        combined_fields = get_combined_fields_for_dataset_group('elasticsearchaux')
    elif dataset_group == 'aac':
        combined_fields = get_combined_fields_for_dataset_group('aac')
    
    # Find fields common across datasets in each group
    common_across_datasets = []
    if dataset_group == 'elasticsearchaux':
        common_across_datasets = find_common_fields_across_datasets('elasticsearchaux')
    elif dataset_group == 'aac':
        common_across_datasets = find_common_fields_across_datasets('aac')

    # Write individual dataset reports
    for dataset_name in DATASET_FIELDS.keys():
        report_path = os.path.join(reports_dir, f"{dataset_group}_{dataset_name}_fields_report.txt")
        with open(report_path, 'w') as f:
            f.write(f"==== Field Analysis Report for Dataset: {dataset_name} in {dataset_group} ====\n\n")
            f.write(f"NOTICE: Analysis used 10% file sampling with 10x scaling for estimation\n\n")
            total_records = DATASET_RECORD_COUNTS[dataset_name]
            f.write(f"Estimated Total Records: {total_records}\n")
            f.write(f"Total Unique Fields Found: {len(DATASET_FIELDS[dataset_name])}\n\n")
            
            # Use threshold based on dataset type
            threshold = AAC_FIELD_PRESENCE_THRESHOLD if dataset_name.startswith('aac_') else FIELD_PRESENCE_THRESHOLD
            threshold_percent = threshold * 100
            
            common_fields = get_common_fields(dataset_name, threshold)
            f.write(f"=== Fields present in {threshold_percent}%+ of records ({len(common_fields)} fields) ===\n")
            for field, column_name in common_fields:
                presence = DATASET_FIELDS[dataset_name][field] / total_records * 100
                f.write(f"- {field} → {column_name}: {presence:.2f}%\n")
            f.write("\n")
            
            f.write(f"=== All Fields by Presence Percentage ===\n")
            sorted_fields = sorted(DATASET_FIELDS[dataset_name].items(), key=lambda x: x[1], reverse=True)
            for field, count in sorted_fields:
                percentage = (count / total_records) * 100
                f.write(f"- {field}: {percentage:.2f}% ({count}/{total_records} records)\n")
        
        logging.info(f"Wrote field analysis report for {dataset_name} to {report_path}")
    
    # Write report of fields common across each dataset group
    if common_across_datasets:
        common_fields_report = os.path.join(reports_dir, f"common_fields_across_{dataset_group}.txt")
        with open(common_fields_report, 'w') as f:
            f.write(f"==== Fields Common Across All {dataset_group} Datasets ====\n\n")
            f.write(f"NOTICE: Analysis used 10% file sampling with 10x scaling for estimation\n\n")
            f.write(f"Found {len(common_across_datasets)} fields that appear in a majority of records in all datasets:\n\n")
            for field in common_across_datasets:
                f.write(f"- {field}\n")
        
        logging.info(f"Wrote common fields report to {common_fields_report}")
    
    # Write report of combined fields for each dataset group
    if combined_fields:
        combined_fields_report = os.path.join(reports_dir, f"combined_{dataset_group}_fields.txt")
        with open(combined_fields_report, 'w') as f:
            f.write(f"==== Combined Fields for {dataset_group} Datasets ====\n\n")
            f.write(f"NOTICE: Analysis used 10% file sampling with 10x scaling for estimation\n\n")
            f.write(f"Total combined fields: {len(combined_fields)}\n\n")
            
            # Group fields by number of datasets
            by_dataset_count = defaultdict(list)
            for field_info in combined_fields:
                by_dataset_count[len(field_info['datasets'])].append(field_info)
            
            for count in sorted(by_dataset_count.keys(), reverse=True):
                f.write(f"\n=== Fields present in {count} dataset(s) ===\n")
                for field_info in sorted(by_dataset_count[count], key=lambda x: x['field']):
                    datasets_str = ', '.join(field_info['datasets'])
                    f.write(f"- {field_info['field']} → {field_info['column']} (in: {datasets_str})\n")
        
        logging.info(f"Wrote combined {dataset_group} fields report to {combined_fields_report}")
    
    # Save metadata for later processing
    metadata_path = os.path.join(reports_dir, f"{dataset_group}_fields_metadata.json")
    with open(metadata_path, 'w') as f:
        metadata = {
            'common_fields': {
                k: [{"field": f_field, "column": c} for f_field, c in get_common_fields(k)] 
                for k in DATASET_FIELDS.keys()
            },
            'record_counts': dict(DATASET_RECORD_COUNTS),
            'analysis_method': f"10% file sampling by dataset with 10x scaling",
            'field_presence_threshold': {
                'elasticsearch': FIELD_PRESENCE_THRESHOLD,
                'aac': AAC_FIELD_PRESENCE_THRESHOLD
            }
        }
        
        if dataset_group == 'elasticsearchaux':
            metadata['combined_elasticsearchaux_fields'] = combined_fields
            metadata['elasticsearchaux_common_fields'] = [{"field": f} for f in common_across_datasets]
        elif dataset_group == 'aac':
            metadata['combined_aac_fields'] = combined_fields
            metadata['aac_common_fields'] = [{"field": f} for f in common_across_datasets]
            
        json.dump(metadata, f, indent=2)

    logging.info(f"Wrote dataset fields metadata to {metadata_path}")

def convert_json_to_parquet(split_file, dataset_name, parquet_path, fields_to_use):
    """
    Convert a single JSON split file to Parquet format.
    This function runs in a worker thread.
    """
    logging.info(f"Converting {os.path.basename(split_file)} to {os.path.basename(parquet_path)}")
    
    con = duckdb.connect()
    try:
        # Try extraction based on dataset type
        if dataset_name.startswith('aac_'):
            # Para AAC: extraer aacid como campo principal y luego campos comunes de metadata
            field_extractions = ["\"aacid\" AS \"aacid\""]
            
            # Extract fields from metadata (just like we do with _source in elasticsearch)
            for field_info in fields_to_use:
                field_path = field_info["field"]
                column_name = field_info["column"]
                
                # Skip the main field
                if field_path == 'aacid':
                    continue
                
                # For fields inside metadata
                if '.' in field_path:
                    parts = field_path.split('.')
                    if parts[0] == 'metadata':
                        parts = parts[1:]  # Skip 'metadata' prefix
                    
                    # Build path for JSON extraction
                    json_path = '$'
                    for part in parts:
                        json_path += f'.{part}'
                    
                    field_extractions.append(f"json_extract_string(\"metadata\", '{json_path}') AS \"{column_name}\"")
            
            # Create SQL statement
            sql_fields = ", ".join(field_extractions)
            
            # Add ignore_errors=true to handle duplicate fields
            conversion_sql = f"""
            COPY (
                SELECT {sql_fields}
                FROM read_json('{split_file}', ignore_errors=true)
            ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
            """
            
        else:
            # Para Elasticsearch: extraer _index, _id, _score y campos comunes de _source
            field_extractions = ["_index AS \"_index\"", "_id AS \"_id\"", "_score AS \"_score\""]
            
            # Extract fields from _source
            for field_info in fields_to_use:
                field_path = field_info["field"]
                column_name = field_info["column"]
                
                # Skip main fields
                if field_path in ['_index', '_id', '_score']:
                    continue
                
                # For fields inside _source
                if '.' in field_path:
                    parts = field_path.split('.')
                    if parts[0] == '_source':
                        parts = parts[1:]  # Skip '_source' prefix
                    
                    # Build path for JSON extraction
                    json_path = '$'
                    for part in parts:
                        json_path += f'.{part}'
                    
                    field_extractions.append(f"json_extract_string(_source, '{json_path}') AS \"{column_name}\"")
            
            # Create SQL statement
            sql_fields = ", ".join(field_extractions)
            
            # Add ignore_errors=true consistently
            conversion_sql = f"""
            COPY (
                SELECT {sql_fields}
                FROM read_json('{split_file}', ignore_errors=true)
            ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
            """
        
        con.execute(conversion_sql)
        rows_count = con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
        logging.info(f"Saved: {os.path.basename(parquet_path)} ({rows_count} rows)")
        con.close()
        return True
        
    except Exception as e:
        logging.error(f"Error converting {os.path.basename(split_file)}: {e}")
        try:
            # Si falla, intentar con read_json_auto como último recurso
            auto_sql = f"""
            COPY (
                SELECT *
                FROM read_json_auto('{split_file}', format='auto', ignore_errors=true)
            ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
            """
            con.execute(auto_sql)
            rows_count = con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
            logging.info(f"Saved with auto-detection: {os.path.basename(parquet_path)} ({rows_count} rows)")
            con.close()
            return True
            
        except Exception as final_e:
            logging.error(f"Final attempt failed for {os.path.basename(split_file)}: {final_e}")
            try:
                con.close()
            except:
                pass
            return False


def convert_to_parquet(dataset_groups, target_dir):
    """Convert all split files to Parquet for all datasets."""
    error_dir = os.path.join(SCRIPT_DIR, 'data', 'error')
    reports_dir = os.path.join(SCRIPT_DIR, 'reports')
    
    # Determine directory type
    dir_name = os.path.basename(target_dir)
    if 'elasticsearchaux' in dir_name:
        dataset_group = 'elasticsearchaux'
    elif 'elasticsearch' in dir_name:
        dataset_group = 'elasticsearch'
    elif 'aac' in dir_name:
        dataset_group = 'aac'
    else:
        dataset_group = dir_name
    
    # Get combined fields for dataset group
    combined_elasticsearchaux_fields = []
    combined_aac_fields = []
    
    if dataset_group == 'elasticsearchaux':
        combined_elasticsearchaux_fields = get_combined_fields_for_dataset_group('elasticsearchaux')
    elif dataset_group == 'aac':
        combined_aac_fields = get_combined_fields_for_dataset_group('aac')
    
    logging.info(f"Converting all files to Parquet for {dataset_group}...")
    os.makedirs(error_dir, exist_ok=True)
    
    # Process each dataset
    for dataset_name, split_files in dataset_groups.items():
        logging.info(f"Converting {len(split_files)} split files for dataset {dataset_name}")
        
        # Determine which fields to use
        if dataset_group == 'elasticsearchaux' and dataset_name in ['metadata', 'journals', 'digital_lending']:
            # For elasticsearchaux, use the combined fields from all datasets
            fields_to_use = combined_elasticsearchaux_fields
            logging.info(f"Using {len(fields_to_use)} combined fields for elasticsearchaux")
        elif dataset_group == 'elasticsearch' and dataset_name == 'records':
            # For main elasticsearch (which only has 'records' dataset), use dataset-specific fields
            fields_to_use = [{"field": field, "column": column} for field, column in get_common_fields(dataset_name, FIELD_PRESENCE_THRESHOLD)]
            logging.info(f"Using {len(fields_to_use)} individual common fields for main elasticsearch records")
        elif dataset_group == 'aac' and dataset_name.startswith('aac_'):
            # For AAC, use only dataset-specific fields, not combined fields
            fields_to_use = [{"field": field, "column": column} for field, column in get_common_fields(dataset_name, AAC_FIELD_PRESENCE_THRESHOLD)]
            logging.info(f"Using {len(fields_to_use)} individual common fields for {dataset_name} (NOT using combined fields)")
        else:
            # Use threshold based on dataset type
            threshold = AAC_FIELD_PRESENCE_THRESHOLD if dataset_name.startswith('aac_') else FIELD_PRESENCE_THRESHOLD
            fields_to_use = [{"field": field, "column": column} for field, column in get_common_fields(dataset_name, threshold)]
            logging.info(f"Using {len(fields_to_use)} common fields for {dataset_name}")
        
        # Group split files by original file
        split_groups = defaultdict(list)
        for split_file in split_files:
            base_name = os.path.basename(split_file)
            # Handle nested recursive parts
            if '_part__part_' in base_name:
                # This is a recursively split part, get the original root filename
                original_file = base_name.split('_part_')[0]
            else:
                original_file = base_name.split('_part_')[0]
            
            split_groups[original_file].append(split_file)
        
        # Process each group of split files
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_CONVERSION) as executor:
            for original_file, group_splits in split_groups.items():
                if not group_splits:
                    continue
                
                # Convert each split file to Parquet
                futures = []
                created_parquet_files = []
                
                for i, split_file in enumerate(sorted(group_splits), 1):
                    parquet_name = f"{dataset_name}_{original_file}_{i}.parquet"
                    parquet_path = os.path.join(target_dir, parquet_name)
                    
                    # Submit conversion task
                    future = executor.submit(
                        convert_json_to_parquet, 
                        split_file, 
                        dataset_name, 
                        parquet_path, 
                        fields_to_use
                    )
                    futures.append((future, split_file, parquet_path))
                
                # Process results as they complete
                all_successful = True
                for future, split_file, parquet_path in futures:
                    try:
                        success = future.result()
                        if success:
                            created_parquet_files.append(parquet_path)
                        else:
                            all_successful = False
                    except Exception as e:
                        logging.error(f"Error in conversion task for {os.path.basename(split_file)}: {e}")
                        all_successful = False
                
                # Check if all conversions were successful
                if not all_successful:
                    logging.error(f"Some conversions failed for {original_file}, cleaning up")
                    # Delete all created Parquet files
                    for parquet_path in created_parquet_files:
                        try:
                            if os.path.exists(parquet_path):
                                os.remove(parquet_path)
                                logging.info(f"Deleted parquet file due to errors: {os.path.basename(parquet_path)}")
                        except Exception as e:
                            logging.warning(f"Could not delete parquet file: {e}")
                    
                    # Move JSON parts to error directory
                    for _, split_file, _ in futures:
                        if os.path.exists(split_file):
                            error_path = os.path.join(error_dir, os.path.basename(split_file))
                            try:
                                shutil.move(split_file, error_path)
                                logging.info(f"Moved JSON part to error directory: {os.path.basename(split_file)}")
                            except Exception as e:
                                logging.warning(f"Could not move JSON part to error directory: {e}")
                else:
                    # Clean up split files after successful conversion
                    for _, split_file, _ in futures:
                        try:
                            if os.path.exists(split_file):
                                os.remove(split_file)
                                logging.info(f"Deleted split file: {os.path.basename(split_file)}")
                        except Exception as e:
                            logging.warning(f"Could not delete split file: {e}")


def process_mini_batch(batch_files, prefix, mega_batch_idx, mini_batch_idx, temp_dir):
    """Process a mini batch of files and save to temporary Parquet file."""
    try:
        start_time = time.time()
        
        # Temporary file for this mini-batch
        temp_output = f"{temp_dir}/temp_{mini_batch_idx}.parquet"
        
        # Connect to DuckDB
        conn = duckdb.connect(database=':memory:')
        
        # Performance settings for DuckDB
        conn.execute("PRAGMA memory_limit='{RAM_MERGE}'")
        conn.execute("PRAGMA threads={CPU}")
        
        # Process the files in this mini-batch more efficiently
        # Use a single SQL operation with UNION ALL
        sql_parts = []
        for file in batch_files:
            sql_parts.append(f"SELECT * FROM '{file}'")  # No need to exclude _source since it's already excluded in conversion
        
        sql_query = " UNION ALL ".join(sql_parts)
        
        # Write directly without creating intermediate table
        conn.execute(f"""
        COPY ({sql_query}) TO '{temp_output}' (FORMAT PARQUET, COMPRESSION 'zstd')
        """)
        
        # Close connection to free memory
        conn.close()
        
        # Force garbage collection
        gc.collect()
        
        duration = time.time() - start_time
        logger.info(f"Mini-batch {mini_batch_idx} for prefix {prefix} processed in {duration:.2f} seconds")
        
        return temp_output
    except Exception as e:
        logger.error(f"Error processing mini-batch {mini_batch_idx}: {str(e)}")
        return None

def get_base_filename(file_path):
    """
    Extract base filename for grouping when merging, preserving the original filename format.
    For example: 'metadata_journals_1.parquet' -> 'metadata_journals'
    """
    filename = os.path.basename(file_path)
    # Remove the extension
    base_name = os.path.splitext(filename)[0]
    # Remove the last part after the last underscore (which is the part number)
    if '_' in base_name:
        # Split by underscore and get all parts except the last one
        parts = base_name.rsplit('_', 1)
        if parts[1].isdigit():  # If last part is a number, remove it
            return parts[0]
    return base_name  # Return as is if no trailing number

def merge_parquet_files_for_directory(config):
    """
    Merge individual Parquet files into consolidated files by prefix group.
    This improves query performance by reducing the number of files.
    """
    source_dir = config['target_dir']  # Use target_dir as source for merge
    
    # Determine the final output directory
    if 'elasticsearchaux' in source_dir:
        output_dir = os.path.join(SCRIPT_DIR, 'data', 'elasticsearchauxF')
        file_pattern = "*.parquet"
    elif 'elasticsearch' in source_dir and 'aux' not in source_dir:
        output_dir = os.path.join(SCRIPT_DIR, 'data', 'elasticsearchF')
        file_pattern = "other_aarecords__*_*.parquet"
    elif 'aac' in source_dir:
        output_dir = os.path.join(SCRIPT_DIR, 'data', 'aacF')
        file_pattern = "aac_*_*.parquet"
    else:
        logging.info(f"Don't know how to merge files for {source_dir}, skipping merge")
        return
    
    logger.info(f"Starting Parquet file merging process for {source_dir} to {output_dir}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Get all Parquet files matching the pattern
    parquet_files = glob.glob(os.path.join(source_dir, file_pattern))
    if not parquet_files:
        # Try with a more general pattern if the specific pattern doesn't match
        parquet_files = glob.glob(os.path.join(source_dir, "*.parquet"))
    
    logger.info(f"Found {len(parquet_files)} Parquet files in {source_dir} to merge")
    
    if not parquet_files:
        logger.info(f"No files to process in {source_dir}, skipping merge")
        return

    # Group files by their base filename (excluding part numbers)
    file_groups = defaultdict(list)
    
    for file in parquet_files:
        base_name = get_base_filename(file)
        file_groups[base_name].append(file)
    
    logger.info(f"Grouped files into {len(file_groups)} base name groups for {source_dir}")
    
    # Calculate the total number of files to process
    total_files = sum(len(files) for files in file_groups.values())
    processed_files = 0
    
    # Number of workers for parallelization
    num_workers = MAX_WORKERS_MERGE  # Process mini-batches in parallel
    
    # Process each group
    for base_name, files in sorted(file_groups.items()):
        logger.info(f"Processing file group {base_name} with {len(files)} files from {source_dir}")
        
        total_mega_batches = (len(files) + mega_batch_size - 1) // mega_batch_size
        
        for mega_batch_idx in range(total_mega_batches):
            mega_start_idx = mega_batch_idx * mega_batch_size
            mega_end_idx = min((mega_batch_idx + 1) * mega_batch_size, len(files))
            mega_batch_files = files[mega_start_idx:mega_end_idx]
            
            # Directory for temporary files
            temp_dir = f"{output_dir}/temp_{base_name}_{mega_batch_idx}"
            os.makedirs(temp_dir, exist_ok=True)
            
            # Final output file for this mega-batch - with batch number
            batch_num = mega_batch_idx + 1
            
            # Keep original name format, just add the batch number
            final_output = f"{output_dir}/{base_name}_{batch_num}.parquet"
            
            logger.info(f"Processing mega-batch {batch_num}/{total_mega_batches} for {base_name} ({mega_end_idx-mega_start_idx} files)")
            
            # Process in mini-batches of 100 files
            mini_batch_size = 100
            
            # Prepare mini-batches
            mini_batches = []
            for i in range(0, len(mega_batch_files), mini_batch_size):
                end_idx = min(i + mini_batch_size, len(mega_batch_files))
                mini_batches.append(mega_batch_files[i:end_idx])
            
            # Create a counter for completed mini-batches
            completed_mini_batches = 0
            total_mini_batches = len(mini_batches)
            
            # Process mini-batches in parallel
            temp_outputs = []
            with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
                futures = {}
                for i, mini_batch in enumerate(mini_batches):
                    future = executor.submit(
                        process_mini_batch, 
                        mini_batch, 
                        base_name, 
                        mega_batch_idx, 
                        i, 
                        temp_dir
                    )
                    futures[future] = len(mini_batch)
                
                # Collect results as they complete
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    if result:
                        temp_outputs.append(result)
                    
                    # Update processed count
                    batch_size = futures[future]
                    processed_files += batch_size
                    completed_mini_batches += 1
                    
                    # Show progress percentage
                    completion = processed_files / total_files * 100
                    mega_completion = completed_mini_batches / total_mini_batches * 100
                    logger.info(f"Progress: {processed_files}/{total_files} files ({completion:.2f}%), "
                              f"Mega-batch: {completed_mini_batches}/{total_mini_batches} mini-batches ({mega_completion:.2f}%)")
            
            # Combine all temporary files into one (sequentially)
            if temp_outputs:
                logger.info(f"Combining {len(temp_outputs)} temporary files into final output: {final_output}")
                
                conn = duckdb.connect(database=':memory:')
                conn.execute("PRAGMA memory_limit='{RAM_MERGE}'")
                conn.execute("PRAGMA threads={CPU}")
                
                # Create a file list string for SQL
                file_list = ", ".join([f"'{f}'" for f in temp_outputs])
                
                # Combine and write to final file
                conn.execute(f"""
                COPY (
                    SELECT * FROM read_parquet([{file_list}])
                ) TO '{final_output}' (FORMAT PARQUET, COMPRESSION 'zstd')
                """)
                
                conn.close()
                
                # Clean up temporary files if successful
                for temp_file in temp_outputs:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                
                # Clean up original Parquet files that have been merged
                for orig_file in mega_batch_files:
                    try:
                        if os.path.exists(orig_file):
                            os.remove(orig_file)
                            logger.info(f"Deleted original Parquet file: {os.path.basename(orig_file)}")
                    except Exception as e:
                        logger.warning(f"Could not delete original Parquet file: {e}")
                
                # Remove temporary directory if empty
                try:
                    if not os.listdir(temp_dir):
                        os.rmdir(temp_dir)
                except Exception as e:
                    logger.warning(f"Could not remove temp directory: {str(e)}")
                
                logger.info(f"Successfully merged mega-batch {batch_num} to {final_output}")
            else:
                logger.error(f"No temporary files were created for mega-batch {batch_num}")
        
        logger.info(f"Completed processing all batches for {base_name} in {source_dir}")
    
    logger.info(f"Parquet file merging process completed for {source_dir}")

def cleanup_temp_files(json_dir, parts_dir):
    """Cleanup temporary JSON and split files"""
    logging.info(f"Cleaning up temporary files in {json_dir} and {parts_dir}")
    
    # Clean up JSON files
    json_files = glob.glob(os.path.join(json_dir, "*.json")) + glob.glob(os.path.join(json_dir, "*.jsonl"))
    for f in json_files:
        try:
            os.remove(f)
        except Exception as e:
            logging.warning(f"Failed to delete {f}: {e}")
    
    # Clean up split files
    split_files = glob.glob(os.path.join(parts_dir, "*_part_*"))
    for f in split_files:
        try:
            os.remove(f)
        except Exception as e:
            logging.warning(f"Failed to delete {f}: {e}")
    
    logging.info("Temporary files cleanup completed")

if __name__ == "__main__":
    logging.info("Starting data processing pipeline...")
    create_directories()
    find_and_move_files()
    
    # Process each main directory completely before moving to the next
    configs = [
        {
            'source_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux'),
            'json_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux_json'),
            'parts_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux_parts'),
            'target_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux'),
            'compression': 'gzip'
        },
        {
            'source_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearch'),
            'json_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearch_json'),
            'parts_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearch_parts'),
            'target_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearch'),
            'compression': 'gzip'
        },
        {
            'source_dir': os.path.join(SCRIPT_DIR, 'data', 'aac'),
            'json_dir': os.path.join(SCRIPT_DIR, 'data', 'aac_json'),
            'parts_dir': os.path.join(SCRIPT_DIR, 'data', 'aac_parts'),
            'target_dir': os.path.join(SCRIPT_DIR, 'data', 'aac'),
            'compression': 'zstd'
        }
    ]
    
    # Process each directory sequentially - full pipeline
    for config in configs:
        logging.info(f"===== Starting full processing pipeline for {config['source_dir']} =====")
        process_directory(config)
        logging.info(f"===== Completed processing for {config['source_dir']} =====\n")
    
    logging.info("Data processing complete! Reports on common fields are available in the reports directory.")
