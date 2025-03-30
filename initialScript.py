import os
import subprocess
import shutil
import gzip
import glob
import duckdb
import logging
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get the directory where the script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def create_directories():
    """Create necessary directories (data/elasticsearch, data/json, data/parquet) if they don't exist."""
    dirs = [
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearch'),
        os.path.join(SCRIPT_DIR, 'data', 'json'),
        os.path.join(SCRIPT_DIR, 'data', 'parquet')
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)

def download_and_extract():
    """
    Download the torrent file and use aria2c to download only the Elasticsearch files.
    It lists the files in the torrent, filters for files in the /elasticsearch/ folder that are larger than 1MB,
    then downloads only those files.
    After the download, if the files are inside a subdirectory, they are moved to the elasticsearch folder.
    Finally, it removes files smaller than 1GB.
    """
    torrent_url = "https://annas-archive.org/dyn/small_file/torrents/other_aa/aa_derived_mirror_metadata/aa_derived_mirror_metadata_20250223.torrent"
    # Torrent file will be stored in data/elasticsearch
    torrent_path = os.path.join(SCRIPT_DIR, 'data', 'elasticsearch', 'metadata.torrent')
    output_dir = os.path.join(SCRIPT_DIR, 'data', 'elasticsearch')

    # Download torrent file if it doesn't exist
    if os.path.exists(torrent_path):
        logging.info("Torrent file already exists, skipping download.")
    else:
        logging.info("Downloading torrent file...")
        subprocess.run(['curl', '-L', '-o', torrent_path, torrent_url], check=True)

    logging.info("Fetching file list from the torrent using aria2c --show-files...")
    try:
        result = subprocess.run(
            ['aria2c', '--show-files', torrent_path],
            capture_output=True, text=True, check=True
        )
        file_list = result.stdout.split("\n")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to list files in the torrent: {e}")
        return

    # Parse the file list to extract indexes of files in the elasticsearch folder
    file_indexes = []
    # Process the file list in pairs: one line with index and path, the next with the size.
    for i in range(len(file_list) - 1):
        line = file_list[i].strip()
        size_line = file_list[i + 1].strip() if i + 1 < len(file_list) else ""
        match = re.match(r"^(\d+)\|(.+)", line)
        if match:
            index, file_path = match.groups()
            index = index.strip()
            file_path = file_path.strip()
            # Process only files in the elasticsearch folder
            if "/elasticsearch/" in file_path:
                size_match = re.search(r"\(([\d,]+)\)", size_line)
                if size_match:
                    file_size = int(size_match.group(1).replace(",", ""))
                    # Only download files larger than 1MB (1,000,000 bytes)
                    if file_size >= 1_000_000:
                        file_indexes.append(index)
                        logging.info(f"Selected file: {file_path} (Index: {index}, Size: {file_size} bytes)")

    if not file_indexes:
        logging.error("No large Elasticsearch files found in the torrent. Check the torrent structure.")
        return

    index_string = ",".join(file_indexes)
    logging.info(f"Downloading Elasticsearch files using aria2c (Indexes: {index_string})...")

    try:
        subprocess.run([
            'aria2c', '--seed-time=0', '--max-connection-per-server=16',
            '--dir=' + output_dir, f'--select-file={index_string}', '--bt-save-metadata=true',
            torrent_path
        ], check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"aria2c download failed: {e}")
        return

    # Move gz files from subdirectories to the output directory
    for item in os.listdir(output_dir):
        sub_path = os.path.join(output_dir, item)
        if os.path.isdir(sub_path):
            for file in os.listdir(sub_path):
                if file.endswith('.gz'):
                    src = os.path.join(sub_path, file)
                    dst = os.path.join(output_dir, file)
                    shutil.move(src, dst)
                    logging.info(f"Moved {file} from subdirectory to {output_dir}")
            try:
                os.rmdir(sub_path)
            except Exception as e:
                logging.warning(f"Could not remove directory {sub_path}: {e}")

    logging.info("Download complete. Removing small files (<1GB) from the elasticsearch folder...")

    # Remove any files smaller than 1GB in the output directory
    for file in os.listdir(output_dir):
        file_path = os.path.join(output_dir, file)
        if os.path.isfile(file_path) and os.path.getsize(file_path) < 1_000_000_000:
            os.remove(file_path)
            logging.info(f"Deleted small file: {file}")

    # Remove the torrent file after download
    if os.path.exists(torrent_path):
        os.remove(torrent_path)

    logging.info("Download and cleanup complete.")

def decompress_gz_files():
    """
    Decompress all .json.gz files from data/elasticsearch and save them as .json in data/json.
    (Using the provided decompression code.)
    """
    gz_files = glob.glob(os.path.join(SCRIPT_DIR, 'data', 'elasticsearch', '*.json.gz'))
    
    for gz_file in gz_files:
        base_name = os.path.basename(gz_file)
        json_name = base_name[:-3]  # Remove .gz extension
        json_path = os.path.join(SCRIPT_DIR, 'data', 'json', json_name)
        
        print(f"ðŸ”„ Decompressing {base_name}...")
        
        with gzip.open(gz_file, 'rb') as f_in:
            with open(json_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                
        print(f"âœ… Created {json_name}")

def convert_to_parquet(json_file):
    """
    Convert a given JSON file to Parquet in 10 chunks using DuckDB.
    (Using the provided conversion code.)
    """
    print(f"\nðŸ”„ Processing {os.path.basename(json_file)}")
    
    # Connect to DuckDB
    con = duckdb.connect()
    
    # Count total rows
    total_rows = con.execute(f"SELECT COUNT(*) FROM read_ndjson_objects('{json_file}');").fetchone()[0]
    chunk_size = total_rows // 10 if total_rows >= 10 else total_rows
    print(f"ðŸ“Š Total rows: {total_rows} | Chunk size: {chunk_size}")
    
    # Process in 10 chunks
    offset = 0
    base_name = os.path.basename(json_file)
    file_name = os.path.splitext(base_name)[0]
    
    for i in range(10):
        print(f"ðŸ”„ Processing chunk {i+1}/10 (Offset: {offset})...")
        
        query = f"""
        CREATE OR REPLACE TABLE batch_data AS
        SELECT * FROM read_ndjson_objects('{json_file}')
        LIMIT {chunk_size} OFFSET {offset};
        """
        con.execute(query)
        
        count = con.execute("SELECT COUNT(*) FROM batch_data;").fetchone()[0]
        if count == 0:
            print("âœ… No more rows to process.")
            break
        
        # Save chunk as Parquet
        parquet_file = os.path.join(SCRIPT_DIR, 'data', 'parquet', f"{file_name}_{i+1}.parquet")
        con.execute(f"COPY batch_data TO '{parquet_file}' (FORMAT 'parquet', COMPRESSION 'zstd');")
        print(f"âœ… Saved: {os.path.basename(parquet_file)} ({count} rows)")
        
        offset += count
    
    con.close()
    print(f"âœ… Completed processing {base_name}")

def process_all_json_files():
    """Process all JSON files in data/json by converting them to Parquet."""
    json_files = glob.glob(os.path.join(SCRIPT_DIR, 'data', 'json', '*.json'))
    for json_file in json_files:
        convert_to_parquet(json_file)

def main():
    """Main execution function."""
    try:
        logging.info("Starting data processing pipeline...")
        create_directories()
        download_and_extract()
        decompress_gz_files() 
        process_all_json_files()
        logging.info("Data processing complete! You can now start analyzing the data.")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
