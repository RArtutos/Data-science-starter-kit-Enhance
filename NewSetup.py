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
from collections import defaultdict, deque

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("processing.log"), logging.StreamHandler()])
logger = logging.getLogger()

# Script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

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

# Maximum number of concurrent thread, values for 30GB RAM
MAX_WORKERS_ANALYSIS = 10  # For analysis
MAX_WORKERS_CONVERSION = 4
MAX_WORKERS_MERGE = 4      # For merging parquet files
mega_batch_size = 500      #The last merge

# Target size for split files (in MB) - REDUCED
TARGET_SPLIT_SIZE_MB = 10  # Reduced from 100MB to 10MB


def get_prefix_number(filename):
    """Extract the prefix number from the filename."""
    match = re.search(r'other_aarecords__(\d+)_', filename)
    if match:
        return int(match.group(1))
    return None

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
    """Analyzes JSON structure and counts field frequencies."""
    logging.info(f"Analyzing JSON structure in {os.path.basename(file_path)} for dataset: {dataset_name}")
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
    
    # Thread-safe update of global counters
    with DATASET_FIELDS_LOCK:
        for field, count in local_fields.items():
            DATASET_FIELDS[dataset_name][field] += count
    
    with DATASET_RECORD_COUNTS_LOCK:
        DATASET_RECORD_COUNTS[dataset_name] += record_count
    
    logging.info(f"Processed {record_count} records from {os.path.basename(file_path)}")
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

def process_compressed_file(config, compressed_file):
    """
    Process a single compressed file: decompress, split, analyze.
    This is designed to run as a worker in a thread pool.
    """
    source_dir = config['source_dir']
    json_dir = config['json_dir']
    parts_dir = config['parts_dir']
    compression = config['compression']
    
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
    dataset_name = determine_dataset(base_name)
    
    logging.info(f"Processing {base_name} (Dataset: {dataset_name})")
    
    try:
        # Step 1: Decompress the file
        logging.info(f"Decompressing {base_name} to {json_dir}...")
        if compression == 'gzip':
            with gzip.open(compressed_file, 'rb') as f_in:
                with open(json_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:  # zstd
            decompress_zst_file(compressed_file, json_path)
        
        # Step 2: Split the file into parts targeting 10MB per part
        logging.info(f"Splitting {json_name} into ~{TARGET_SPLIT_SIZE_MB}MB parts...")
        split_files = split_for_target_size(json_path, parts_dir, file_name)
        
        # Step 3: Remove the large JSON file to free up space
        os.remove(json_path)
        logging.info(f"Removed {json_name} after splitting")
        
        # Step 4: Analyze each part to gather field statistics (this will be done in parallel)
        return {
            'dataset_name': dataset_name,
            'split_files': split_files
        }
        
    except Exception as e:
        logging.error(f"Error processing {base_name}: {e}")
        return None

def decompress_split_and_analyze():
    """
    First pass to analyze all files and determine field frequencies.
    This uses a split-first approach for large files to avoid memory issues.
    Uses thread pool to process multiple files concurrently.
    """
    # Check if metadata already exists - if so, we can skip this phase
    reports_dir = os.path.join(SCRIPT_DIR, 'reports')
    metadata_path = os.path.join(reports_dir, "dataset_fields_metadata.json")
    if os.path.exists(metadata_path):
        logging.info("Field analysis metadata already exists. Skipping decompress/analysis phase.")
        # Load existing metadata
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
            global DATASET_FIELDS, DATASET_RECORD_COUNTS
            # Restore the field and record counts
            for dataset, fields in metadata.get('common_fields', {}).items():
                for field_info in fields:
                    field_name = field_info.get('field')
                    # We don't have exact counts, but we set a high value to indicate these are common
                    DATASET_FIELDS[dataset][field_name] = 100
            for dataset, count in metadata.get('record_counts', {}).items():
                DATASET_RECORD_COUNTS[dataset] = count
        return

    logging.info(f"Starting analysis using up to {MAX_WORKERS_ANALYSIS} concurrent threads...")

    configs = [
        {
            'source_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearch'),
            'json_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearch_json'),
            'parts_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearch_parts'),
            'compression': 'gzip'
        },
        {
            'source_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux'),
            'json_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux_json'),
            'parts_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux_parts'),
            'compression': 'gzip'
        },
        {
            'source_dir': os.path.join(SCRIPT_DIR, 'data', 'aac'),
            'json_dir': os.path.join(SCRIPT_DIR, 'data', 'aac_json'),
            'parts_dir': os.path.join(SCRIPT_DIR, 'data', 'aac_parts'),
            'compression': 'zstd'
        }
    ]

    # Gather all files to process
    all_tasks = []
    
    for config in configs:
        source_dir = config['source_dir']
        compression = config['compression']
        
        if compression == 'gzip':
            compressed_files = glob.glob(os.path.join(source_dir, '*.json.gz'))
        else:  # zstd
            compressed_files = glob.glob(os.path.join(source_dir, '*.zst'))
            
        for compressed_file in compressed_files:
            all_tasks.append((config, compressed_file))
    
    # Phase 1: Decompress and split files (limited concurrency)
    split_results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_ANALYSIS) as executor:
        # Submit tasks for decompression and splitting
        future_to_file = {
            executor.submit(process_compressed_file, config, compressed_file): (config, compressed_file)
            for config, compressed_file in all_tasks
        }
        
        for future in concurrent.futures.as_completed(future_to_file):
            config, compressed_file = future_to_file[future]
            try:
                result = future.result()
                if result:
                    split_results.append(result)
            except Exception as e:
                logging.error(f"Error processing {os.path.basename(compressed_file)}: {e}")
    
    # Phase 2: Analyze JSON structure of split parts (parallel)
    all_analysis_tasks = []
    
    for result in split_results:
        dataset_name = result['dataset_name']
        split_files = result['split_files']
        
        for split_file in split_files:
            all_analysis_tasks.append((split_file, dataset_name))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_ANALYSIS) as executor:
        # Submit tasks for JSON analysis
        futures = [
            executor.submit(analyze_json_structure, split_file, dataset_name)
            for split_file, dataset_name in all_analysis_tasks
        ]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error during JSON analysis: {e}")
    
    # Write reports after analyzing all datasets
    write_field_reports()

def get_common_fields(dataset_name, threshold=0.5):  # Changed threshold from 0 to 0.5 (50%)
    """
    Calculates fields that appear in at least a percentage (threshold) of records.
    Returns a list of tuples (field_path, sql_column_name).
    """
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
    
    # Get the common fields (threshold 50%) for each dataset in the group
    all_fields = {}
    
    for dataset in datasets:
        if dataset not in DATASET_RECORD_COUNTS or DATASET_RECORD_COUNTS[dataset] == 0:
            continue
        
        for field_path, column_name in get_common_fields(dataset, 0.5):
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
    
    # Get common fields (50%+) for each dataset
    dataset_common_fields = {}
    for dataset in active_datasets:
        fields = set(field for field, _ in get_common_fields(dataset, 0.5))
        dataset_common_fields[dataset] = fields
    
    # Find intersection of all sets
    common_fields = set.intersection(*dataset_common_fields.values()) if dataset_common_fields else set()
    return sorted(common_fields)

def write_field_reports():
    """Writes reports about field frequencies and common fields."""
    reports_dir = os.path.join(SCRIPT_DIR, 'reports')
    
    # Get combined fields for dataset groups
    elasticsearchaux_fields = get_combined_fields_for_dataset_group('elasticsearchaux')
    aac_fields = get_combined_fields_for_dataset_group('aac')
    
    # Find fields common across datasets in each group
    elasticsearchaux_common_fields = find_common_fields_across_datasets('elasticsearchaux')
    aac_common_fields = find_common_fields_across_datasets('aac')

    # Write individual dataset reports
    for dataset_name in DATASET_FIELDS.keys():
        report_path = os.path.join(reports_dir, f"{dataset_name}_fields_report.txt")
        with open(report_path, 'w') as f:
            f.write(f"==== Field Analysis Report for Dataset: {dataset_name} ====\n\n")
            total_records = DATASET_RECORD_COUNTS[dataset_name]
            f.write(f"Total Records Analyzed: {total_records}\n")
            f.write(f"Total Unique Fields Found: {len(DATASET_FIELDS[dataset_name])}\n\n")
            
            common_fields = get_common_fields(dataset_name, 0.5)  # Changed threshold to 50%
            f.write(f"=== Fields present in 50%+ of records ({len(common_fields)} fields) ===\n")
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
    for group_name, common_fields in [
        ('elasticsearchaux', elasticsearchaux_common_fields),
        ('aac', aac_common_fields)
    ]:
        common_fields_report = os.path.join(reports_dir, f"common_fields_across_{group_name}.txt")
        with open(common_fields_report, 'w') as f:
            f.write(f"==== Fields Common Across All {group_name} Datasets ====\n\n")
            f.write(f"Found {len(common_fields)} fields that appear in a majority of records in all datasets:\n\n")
            for field in common_fields:
                f.write(f"- {field}\n")
        
        logging.info(f"Wrote common fields report to {common_fields_report}")
    
    # Write report of combined fields for each dataset group
    for group_name, combined_fields in [
        ('elasticsearchaux', elasticsearchaux_fields),
        ('aac', aac_fields)
    ]:
        combined_fields_report = os.path.join(reports_dir, f"combined_{group_name}_fields.txt")
        with open(combined_fields_report, 'w') as f:
            f.write(f"==== Combined Fields for {group_name} Datasets ====\n\n")
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
        
        logging.info(f"Wrote combined {group_name} fields report to {combined_fields_report}")
    
    # Save metadata for later processing
    metadata_path = os.path.join(reports_dir, "dataset_fields_metadata.json")
    with open(metadata_path, 'w') as f:
        metadata = {
            'common_fields': {
                k: [{"field": f_field, "column": c} for f_field, c in get_common_fields(k, 0.5)] 
                for k in DATASET_FIELDS.keys()
            },
            'combined_elasticsearchaux_fields': elasticsearchaux_fields,
            'combined_aac_fields': aac_fields,
            'elasticsearchaux_common_fields': [{"field": f} for f in elasticsearchaux_common_fields],
            'aac_common_fields': [{"field": f} for f in aac_common_fields],
            'record_counts': dict(DATASET_RECORD_COUNTS)
        }
        json.dump(metadata, f, indent=2)

    logging.info(f"Wrote dataset fields metadata to {metadata_path}")

def find_processed_parquet_files():
    """
    Find all existing parquet files to skip processing files that were already converted.
    Returns a set of base filenames (without the path or extension).
    """
    processed_files = set()
    
    # Check all target directories
    directories = [
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearch'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux'),
        os.path.join(SCRIPT_DIR, 'data', 'aac')
    ]
    
    for directory in directories:
        parquet_files = glob.glob(os.path.join(directory, '*.parquet'))
        for parquet_file in parquet_files:
            base_name = os.path.basename(parquet_file).replace('.parquet', '')
            processed_files.add(base_name)
            
    logging.info(f"Found {len(processed_files)} already processed parquet files")
    return processed_files

def convert_json_to_parquet(split_file, dataset_name, parquet_path, fields_to_use):
    """
    Convert a single JSON split file to Parquet format.
    This function runs in a worker thread.
    """
    logging.info(f"Converting {os.path.basename(split_file)} to {os.path.basename(parquet_path)}")
    
    con = duckdb.connect()
    try:
        # Try simplified extraction first
        try:
            logging.info("Using simplified extraction...")
            field_extractions = []
            
            # For AAC files, field structure may be different
            if dataset_name.startswith('aac_'):
                # Adapt field extraction based on the actual AAC file structure
                for field_info in fields_to_use:
                    field_path = field_info["field"]
                    column_name = field_info["column"]
                    
                    # Build path for JSON extraction
                    if '.' in field_path:
                        parts = field_path.split('.')
                        json_path = '$'
                        for part in parts:
                            json_path += f'.{part}'
                        
                        field_extractions.append(f"json_extract_string(json, '{json_path}') AS \"{column_name}\"")
                    else:
                        # Direct field access
                        field_extractions.append(f"json->>'{field_path}' AS \"{column_name}\"")
                
                # If no fields were defined, extract entire JSON
                if not field_extractions:
                    field_extractions.append("json")
                    
            else:
                # For elasticsearch and elasticsearchaux files
                # Extract main Elasticsearch fields but exclude _source
                field_extractions.append("_index AS \"_index\"")
                field_extractions.append("_id AS \"_id\"")
                field_extractions.append("_score AS \"_score\"")
                # Do not include _source column in final result
                
                # Extract common fields from _source
                for field_info in fields_to_use:
                    field_path = field_info["field"]
                    column_name = field_info["column"]
                    
                    # Skip if already a main field
                    if field_path in ['_index', '_id', '_score']:
                        continue
                    
                    # For fields with dot notation, extract from _source
                    if '.' in field_path:
                        parts = field_path.split('.')
                        if parts[0] == '_source':
                            parts = parts[1:]
                        
                        # Build path for JSON extraction
                        json_path = '$'
                        for part in parts:
                            json_path += f'.{part}'
                        
                        field_extractions.append(f"json_extract_string(_source, '{json_path}') AS \"{column_name}\"")
                    else:
                        # Direct field access
                        if field_path != '_source':
                            field_extractions.append(f"_source->>'{field_path}' AS \"{column_name}\"")
            
            simplified_sql = ", ".join(field_extractions)
            
            if dataset_name.startswith('aac_'):
                # For AAC files, we need a different approach as they're parsed differently
                simplified_conversion = f"""
                COPY (
                    SELECT {simplified_sql}
                    FROM read_json_auto('{split_file}', format='auto', ignore_errors=true) AS t(json)
                ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
                """
            else:
                simplified_conversion = f"""
                COPY (
                    SELECT {simplified_sql}
                    FROM read_json('{split_file}')
                ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
                """
            
            con.execute(simplified_conversion)
            rows_count = con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
            logging.info(f"Saved with simplified approach: {os.path.basename(parquet_path)} ({rows_count} rows)")
            con.close()
            return True
        except Exception as e2:
            logging.warning(f"Simplified extraction failed for {os.path.basename(split_file)}: {e2}")
        
        # Fallback to basic extraction method - but exclude _source for elasticsearch files
        logging.info("Using basic extraction method...")
        
        if dataset_name.startswith('aac_'):
            raw_sql = f"""
            COPY (
                SELECT *
                FROM read_json_auto('{split_file}', format='auto', ignore_errors=true)
            ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
            """
        else:
            # For elasticsearch files, exclude _source
            raw_sql = f"""
            COPY (
                SELECT * EXCLUDE (_source)
                FROM read_json('{split_file}')
            ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
            """
            
        con.execute(raw_sql)
        rows_count = con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
        logging.info(f"Saved with basic extraction: {os.path.basename(parquet_path)} ({rows_count} rows)")
        con.close()
        return True
        
    except Exception as e:
        logging.error(f"Basic extraction methods failed for {os.path.basename(split_file)}: {e}")
        try:
            # Final fallback using auto-detection - still exclude _source for elasticsearch
            logging.info("Trying auto-detection as last resort...")
            
            if dataset_name.startswith('aac_'):
                auto_sql = f"""
                COPY (
                    SELECT * 
                    FROM read_json_auto('{split_file}', format='auto', ignore_errors=true)
                ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
                """
            else:
                # For elasticsearch files, exclude _source
                auto_sql = f"""
                COPY (
                    SELECT * EXCLUDE (_source)
                    FROM read_json_auto('{split_file}', format='auto', ignore_errors=true)
                ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
                """
                
            con.execute(auto_sql)
            rows_count = con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
            logging.info(f"Saved with auto-detection: {os.path.basename(parquet_path)} ({rows_count} rows)")
            con.close()
            return True
        except Exception as final_e:
            logging.error(f"Final auto-detection attempt failed for {os.path.basename(split_file)}: {final_e}")
            try:
                con.close()
            except:
                pass
            return False

def process_files():
    """
    For files that have been split during analysis:
    1. Convert each part to Parquet extracting the appropriate fields 
    2. If conversion fails, delete generated Parquets and move JSON parts to error directory
    Uses a single thread to minimize memory usage.
    """
    logging.info(f"Starting process to convert split files to Parquet using {MAX_WORKERS_CONVERSION} worker...")
    error_dir = os.path.join(SCRIPT_DIR, 'data', 'error')

    # Find already processed files to avoid duplicates
    processed_files = find_processed_parquet_files()

    # Load common fields from metadata
    reports_dir = os.path.join(SCRIPT_DIR, 'reports')
    metadata_path = os.path.join(reports_dir, "dataset_fields_metadata.json")
    
    # If metadata file doesn't exist, we can't proceed
    if not os.path.exists(metadata_path):
        logging.error("Metadata file not found. Run the analysis phase first.")
        return
        
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
        common_fields_dict = metadata['common_fields']
        combined_elasticsearchaux_fields = metadata.get('combined_elasticsearchaux_fields', [])
        combined_aac_fields = metadata.get('combined_aac_fields', [])

    configs = [
        {
            'source_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearch'),
            'parts_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearch_parts'),
            'target_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearch')
        },
        {
            'source_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux'),
            'parts_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux_parts'),
            'target_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux')
        },
        {
            'source_dir': os.path.join(SCRIPT_DIR, 'data', 'aac'),
            'parts_dir': os.path.join(SCRIPT_DIR, 'data', 'aac_parts'),
            'target_dir': os.path.join(SCRIPT_DIR, 'data', 'aac')
        }
    ]

    for config in configs:
        source_dir = config['source_dir']
        parts_dir = config['parts_dir']
        target_dir = config['target_dir']
        
        # Group split files by original file
        split_files = glob.glob(os.path.join(parts_dir, '*_part_*.json'))
        if not split_files:
            # Also check for JSONL files (for AAC)
            split_files.extend(glob.glob(os.path.join(parts_dir, '*_part_*.jsonl')))
            
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
        for original_file, group_splits in split_groups.items():
            if not group_splits:
                continue
            
            # Determine dataset from original file name
            extended_original_file = original_file
            if source_dir.endswith('aac'):
                extended_original_file = f"{original_file}.jsonl.seekable.zst"
            else:
                extended_original_file = f"{original_file}.json.gz"
                
            dataset_name = determine_dataset(extended_original_file)
            
            # Determine which fields to use
            if source_dir.endswith('elasticsearchaux') and dataset_name in ['metadata', 'journals', 'digital_lending']:
                # For elasticsearchaux, use the combined fields from all datasets
                fields_to_use = combined_elasticsearchaux_fields
                logging.info(f"Using {len(fields_to_use)} combined fields for elasticsearchaux")
            elif source_dir.endswith('aac') and dataset_name.startswith('aac_'):
                # For AAC, use the combined fields from all AAC datasets
                fields_to_use = combined_aac_fields
                logging.info(f"Using {len(fields_to_use)} combined fields for AAC")
            else:
                fields_to_use = common_fields_dict.get(dataset_name, [])
                logging.info(f"Using {len(fields_to_use)} common fields for {dataset_name}")
            
            # Start converting each split file
            logging.info(f"Processing {len(group_splits)} split files for {original_file}")
            
            # Prepare conversion tasks
            conversion_tasks = []
            for i, split_file in enumerate(sorted(group_splits), 1):
                parquet_name = f"{dataset_name}_{original_file}_{i}.parquet"
                parquet_path = os.path.join(target_dir, parquet_name)
                
                # Check if this parquet file already exists
                parquet_base = os.path.splitext(os.path.basename(parquet_path))[0]
                if parquet_base in processed_files:
                    logging.info(f"Skipping already processed file: {parquet_base}")
                    continue
                
                conversion_tasks.append((split_file, dataset_name, parquet_path, fields_to_use))
            
            # If all conversions were already done, skip
            if not conversion_tasks:
                logging.info(f"All split files for {original_file} already processed, skipping")
                
                # Clean up split files
                for split_file in group_splits:
                    try:
                        if os.path.exists(split_file):
                            os.remove(split_file)
                            logging.info(f"Deleted already processed split file: {os.path.basename(split_file)}")
                    except Exception as e:
                        logging.warning(f"Could not delete split file: {e}")
                continue
            
            # Run conversions using limited workers
            results = []
            created_parquet_files = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_CONVERSION) as executor:
                # Submit tasks for conversion
                future_to_task = {}
                for task in conversion_tasks:
                    split_file, dataset_name, parquet_path, fields = task
                    future = executor.submit(convert_json_to_parquet, split_file, dataset_name, parquet_path, fields)
                    future_to_task[future] = (split_file, parquet_path)
                
                # Process results as they complete
                for future in concurrent.futures.as_completed(future_to_task):
                    split_file, parquet_path = future_to_task[future]
                    try:
                        success = future.result()
                        results.append((split_file, parquet_path, success))
                        if success:
                            created_parquet_files.append(parquet_path)
                    except Exception as e:
                        logging.error(f"Error in conversion task for {os.path.basename(split_file)}: {e}")
                        results.append((split_file, parquet_path, False))
            
            # Check if all conversions were successful
            all_successful = all(success for _, _, success in results)
            
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
                for split_file, _, _ in results:
                    if os.path.exists(split_file):
                        error_path = os.path.join(error_dir, os.path.basename(split_file))
                        try:
                            shutil.move(split_file, error_path)
                            logging.info(f"Moved JSON part to error directory: {os.path.basename(split_file)}")
                        except Exception as e:
                            logging.warning(f"Could not move JSON part to error directory: {e}")
            else:
                # Clean up split files after successful conversion
                for split_file, _, _ in results:
                    try:
                        if os.path.exists(split_file):
                            os.remove(split_file)
                            logging.info(f"Deleted split file: {os.path.basename(split_file)}")
                    except Exception as e:
                        logging.warning(f"Could not delete split file: {e}")
                
                # Remove original compressed file if it still exists
                if source_dir.endswith('aac'):
                    compressed_file = os.path.join(source_dir, f"{original_file}.jsonl.seekable.zst")
                else:
                    compressed_file = os.path.join(source_dir, f"{original_file}.json.gz")
                    
                if os.path.exists(compressed_file):
                    try:
                        os.remove(compressed_file)
                        logging.info(f"Deleted original compressed file: {os.path.basename(compressed_file)}")
                    except Exception as e:
                        logging.warning(f"Could not delete original file: {e}")

def process_mini_batch(batch_files, prefix, mega_batch_idx, mini_batch_idx, temp_dir):
    """Process a mini batch of files and save to temporary Parquet file."""
    try:
        start_time = time.time()
        
        # Temporary file for this mini-batch
        temp_output = f"{temp_dir}/temp_{mini_batch_idx}.parquet"
        
        # Connect to DuckDB
        conn = duckdb.connect(database=':memory:')
        
        # Performance settings for DuckDB
        conn.execute("PRAGMA memory_limit='30GB'")
        conn.execute("PRAGMA threads=16")
        
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
    For example: 'metadata_journals_1.parquet' -> 'metadata_journals_1'
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

def merge_parquet_files():
    """
    Merge individual Parquet files into consolidated files by prefix group.
    This improves query performance by reducing the number of files.
    Maintains original file names.
    """
    logger.info("Starting Parquet file merging process")
    
    # Configure dirs to process and their output dirs
    dir_configs = [
        {
            'source_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearch'),
            'output_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearchF'),
            'file_pattern': "other_aarecords__*_*.parquet"
        },
        {
            'source_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux'),
            'output_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearchauxF'),
            'file_pattern': "metadata_*_*.parquet"
        },
        {
            'source_dir': os.path.join(SCRIPT_DIR, 'data', 'aac'),
            'output_dir': os.path.join(SCRIPT_DIR, 'data', 'aacF'),
            'file_pattern': "aac_*_*.parquet"
        }
    ]
    
    # Process each directory configuration
    for config in dir_configs:
        source_dir = config['source_dir']
        output_dir = config['output_dir']
        file_pattern = config['file_pattern']
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Get all Parquet files matching the pattern
        parquet_files = glob.glob(os.path.join(source_dir, file_pattern))
        logger.info(f"Found {len(parquet_files)} Parquet files in {source_dir} matching pattern {file_pattern}")
        
        if not parquet_files:
            logger.info(f"No files to process in {source_dir}, skipping")
            continue

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
                    conn.execute("PRAGMA memory_limit='30GB'")
                    conn.execute("PRAGMA threads=16")
                    
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
    
    logger.info("Parquet file merging process completed for all directories")

def cleanup_intermediate_files():
    """Cleanup intermediate directories and files after processing"""
    logger.info("Starting cleanup of intermediate files and directories")
    
    # Directories to clean up
    cleanup_dirs = [
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearch_json'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux_json'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearch_parts'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux_parts'),
        os.path.join(SCRIPT_DIR, 'data', 'aac_json'),
        os.path.join(SCRIPT_DIR, 'data', 'aac_parts'),
        # Temp directories from merge process
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearchF', 'temp_*'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearchauxF', 'temp_*'),
        os.path.join(SCRIPT_DIR, 'data', 'aacF', 'temp_*')
    ]
    
    # Delete all files in cleanup directories
    for dir_pattern in cleanup_dirs:
        matching_dirs = glob.glob(dir_pattern)
        for cleanup_dir in matching_dirs:
            if os.path.exists(cleanup_dir) and os.path.isdir(cleanup_dir):
                try:
                    logger.info(f"Cleaning up directory: {cleanup_dir}")
                    # Delete all files in the directory
                    files = glob.glob(os.path.join(cleanup_dir, '*'))
                    for f in files:
                        try:
                            os.remove(f)
                        except Exception as e:
                            logger.warning(f"Failed to delete file {f}: {e}")
                    
                    # Try to remove the directory itself
                    shutil.rmtree(cleanup_dir)
                    logger.info(f"Successfully removed directory: {cleanup_dir}")
                except Exception as e:
                    logger.warning(f"Failed to completely clean up directory {cleanup_dir}: {e}")
    
    logger.info("Cleanup of intermediate files completed")


if __name__ == "__main__":
    logging.info("Starting data processing pipeline...")
    create_directories()
    
    # Check if we need to run the first part (analysis) or resume from conversion
    reports_dir = os.path.join(SCRIPT_DIR, 'reports')
    metadata_path = os.path.join(reports_dir, "dataset_fields_metadata.json")
    
    # Phase 1: Convert compressed files to Parquet
    if not os.path.exists(metadata_path):
        # Run the full pipeline
        find_and_move_files()
        decompress_split_and_analyze()  # Analysis with parallelization
    
    # Phase 2: Convert JSON to Parquet
    process_files()
    
    # Phase 3: Merge Parquet files
    merge_parquet_files()
    
    # Phase 4: Cleanup
    cleanup_intermediate_files()
    
    logging.info("Data processing complete! Reports on common fields are available in the reports directory.")
