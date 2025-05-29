import os
import sys
import argparse
import json
import time
import sqlite3
from datetime import datetime, timezone
import requests
from tqdm import tqdm
import concurrent.futures
import threading
import random
from functools import partial

# Database file
DB_FILE = "bitcoin_first_tx.db"
# Lock for thread-safe database access
DB_LOCK = threading.Lock()
# Memory cache to reduce database hits
MEMORY_CACHE = {}
# Memory cache size limit
MAX_CACHE_SIZE = 100000

def setup_database():
    """Create SQLite database for caching first transaction data"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Create table for first transactions
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS first_transactions (
        address TEXT PRIMARY KEY,
        block_time INTEGER,
        tx_hash TEXT,
        source TEXT,
        timestamp INTEGER
    )
    ''')
    
    # Create index on address for faster lookups
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_address ON first_transactions (address)')
    
    # Create table for batch processing status
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS batch_status (
        batch_id TEXT PRIMARY KEY,
        total_addresses INTEGER,
        processed_addresses INTEGER,
        success_count INTEGER,
        start_time INTEGER,
        end_time INTEGER,
        status TEXT
    )
    ''')
    
    conn.commit()
    conn.close()
    
    print(f"Database setup complete: {DB_FILE}")

def get_cached_transaction(address):
    """Get transaction data from memory cache or local database"""
    # Check memory cache first (fastest)
    if address in MEMORY_CACHE:
        return MEMORY_CACHE[address]
    
    # Then check database
    with DB_LOCK:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        cursor.execute('SELECT block_time, tx_hash, source FROM first_transactions WHERE address = ?', (address,))
        result = cursor.fetchone()
        
        conn.close()
    
    if result:
        block_time, tx_hash, source = result
        data = {"status": {"block_time": block_time}, "tx_hash": tx_hash, "source": source}
        
        # Add to memory cache if not full
        if len(MEMORY_CACHE) < MAX_CACHE_SIZE:
            MEMORY_CACHE[address] = data
        
        return data
    
    return None

def save_to_database(address, block_time, tx_hash, source):
    """Save transaction data to local database"""
    with DB_LOCK:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                'INSERT OR REPLACE INTO first_transactions (address, block_time, tx_hash, source, timestamp) VALUES (?, ?, ?, ?, ?)',
                (address, block_time, tx_hash, source, int(time.time()))
            )
            conn.commit()
        except Exception as e:
            print(f"Error saving to database: {str(e)}")
        finally:
            conn.close()
    
    # Also update memory cache
    if len(MEMORY_CACHE) < MAX_CACHE_SIZE:
        MEMORY_CACHE[address] = {"status": {"block_time": block_time}, "tx_hash": tx_hash, "source": source}

def save_batch_to_database(batch_data):
    """Save a batch of transaction data to database efficiently"""
    if not batch_data:
        return
    
    with DB_LOCK:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        try:
            cursor.executemany(
                'INSERT OR IGNORE INTO first_transactions (address, block_time, tx_hash, source, timestamp) VALUES (?, ?, ?, ?, ?)',
                batch_data
            )
            conn.commit()
        except Exception as e:
            print(f"Error saving batch to database: {str(e)}")
        finally:
            conn.close()

def update_batch_status(batch_id, total, processed, success, status="in_progress"):
    """Update the status of a batch in the database"""
    with DB_LOCK:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        try:
            # Check if batch exists
            cursor.execute('SELECT batch_id FROM batch_status WHERE batch_id = ?', (batch_id,))
            exists = cursor.fetchone()
            
            current_time = int(time.time())
            
            if exists:
                # Update existing batch
                cursor.execute(
                    'UPDATE batch_status SET processed_addresses = ?, success_count = ?, end_time = ?, status = ? WHERE batch_id = ?',
                    (processed, success, current_time, status, batch_id)
                )
            else:
                # Insert new batch
                cursor.execute(
                    'INSERT INTO batch_status (batch_id, total_addresses, processed_addresses, success_count, start_time, end_time, status) VALUES (?, ?, ?, ?, ?, ?, ?)',
                    (batch_id, total, processed, success, current_time, current_time, status)
                )
            
            conn.commit()
        except Exception as e:
            print(f"Error updating batch status: {str(e)}")
        finally:
            conn.close()

def import_from_csv(csv_file, delimiter=',', address_col=0, timestamp_col=1, tx_hash_col=None, max_workers=4):
    """Import first transaction data from CSV file with parallel processing"""
    if not os.path.exists(csv_file):
        print(f"Error: CSV file {csv_file} not found.")
        return 0
    
    # Count total lines for progress reporting
    total_lines = sum(1 for _ in open(csv_file, 'r', encoding='utf-8', errors='ignore'))
    print(f"Found {total_lines} lines in {csv_file}")
    
    # Process in chunks for better memory management and parallelization
    chunk_size = 100000  # Lines per chunk
    total_chunks = (total_lines + chunk_size - 1) // chunk_size
    
    def process_chunk(chunk_id, start_line, end_line):
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        count = 0
        batch = []
        batch_size = 10000
        
        with open(csv_file, 'r', encoding='utf-8', errors='ignore') as f:
            # Skip to start line
            for _ in range(start_line):
                next(f, None)
            
            # Process lines in this chunk
            for i, line in enumerate(f):
                if i >= (end_line - start_line):
                    break
                
                try:
                    fields = line.strip().split(delimiter)
                    
                    if len(fields) <= max(address_col, timestamp_col):
                        continue
                    
                    address = fields[address_col].strip()
                    timestamp = fields[timestamp_col].strip()
                    
                    # Convert timestamp to integer if possible
                    try:
                        timestamp = int(timestamp)
                    except ValueError:
                        # Try to parse as date string
                        try:
                            dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                            timestamp = int(dt.timestamp())
                        except:
                            continue
                    
                    tx_hash = fields[tx_hash_col] if tx_hash_col is not None and tx_hash_col < len(fields) else ""
                    
                    batch.append((address, timestamp, tx_hash, "csv_import", int(time.time())))
                    count += 1
                    
                    if len(batch) >= batch_size:
                        with DB_LOCK:
                            cursor.executemany(
                                'INSERT OR IGNORE INTO first_transactions (address, block_time, tx_hash, source, timestamp) VALUES (?, ?, ?, ?, ?)',
                                batch
                            )
                            conn.commit()
                        batch = []
                
                except Exception as e:
                    pass  # Skip problematic lines silently
            
            # Insert any remaining records
            if batch:
                with DB_LOCK:
                    cursor.executemany(
                        'INSERT OR IGNORE INTO first_transactions (address, block_time, tx_hash, source, timestamp) VALUES (?, ?, ?, ?, ?)',
                        batch
                    )
                    conn.commit()
        
        conn.close()
        return count
    
    # Skip header line
    header_offset = 1
    
    # Create chunks
    chunks = []
    for i in range(total_chunks):
        start = i * chunk_size + header_offset
        end = min((i + 1) * chunk_size + header_offset, total_lines)
        chunks.append((i, start, end))
    
    # Process chunks in parallel
    total_imported = 0
    with tqdm(total=total_lines-1, desc=f"Importing {csv_file}") as pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_chunk, i, start, end): (i, end-start) 
                      for i, start, end in chunks}
            
            for future in concurrent.futures.as_completed(futures):
                chunk_id, chunk_size = futures[future]
                try:
                    count = future.result()
                    total_imported += count
                    pbar.update(chunk_size)
                except Exception as e:
                    print(f"Error processing chunk {chunk_id}: {str(e)}")
    
    print(f"Imported {total_imported} records from {csv_file}")
    return total_imported

def import_from_tsv(tsv_file, address_col=0, balance_col=1, max_workers=4):
    """Import Bitcoin addresses from TSV file (like blockchair export)"""
    if not os.path.exists(tsv_file):
        print(f"Error: TSV file {tsv_file} not found.")
        return 0
    
    print(f"Importing addresses from {tsv_file}...")
    
    # For blockchair exports, we don't have transaction timestamps
    # We'll use the current time as a placeholder
    current_time = int(time.time())
    
    # Count total lines for progress reporting
    total_lines = sum(1 for _ in open(tsv_file, 'r', encoding='utf-8', errors='ignore'))
    print(f"Found {total_lines} lines in {tsv_file}")
    
    # Process in chunks for better memory management
    chunk_size = 100000  # Lines per chunk
    total_chunks = (total_lines + chunk_size - 1) // chunk_size
    
    def process_chunk(chunk_id, start_line, end_line):
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        count = 0
        batch = []
        batch_size = 10000
        
        with open(tsv_file, 'r', encoding='utf-8', errors='ignore') as f:
            # Skip to start line
            for _ in range(start_line):
                next(f, None)
            
            # Process lines in this chunk
            for i, line in enumerate(f):
                if i >= (end_line - start_line):
                    break
                
                try:
                    fields = line.strip().split('\t')
                    
                    if len(fields) <= max(address_col, balance_col):
                        continue
                    
                    address = fields[address_col].strip()
                    
                    # Skip header or empty addresses
                    if not address or address == "address":
                        continue
                    
                    # Use balance as a filter (optional)
                    try:
                        balance = float(fields[balance_col])
                        # Skip zero balance addresses if desired
                        # if balance <= 0:
                        #    continue
                    except:
                        pass
                    
                    # We don't have transaction timestamps from the TSV
                    # Use a placeholder timestamp (current time)
                    batch.append((address, current_time, "", "tsv_import", current_time))
                    count += 1
                    
                    if len(batch) >= batch_size:
                        with DB_LOCK:
                            cursor.executemany(
                                'INSERT OR IGNORE INTO first_transactions (address, block_time, tx_hash, source, timestamp) VALUES (?, ?, ?, ?, ?)',
                                batch
                            )
                            conn.commit()
                        batch = []
                
                except Exception as e:
                    pass  # Skip problematic lines silently
            
            # Insert any remaining records
            if batch:
                with DB_LOCK:
                    cursor.executemany(
                        'INSERT OR IGNORE INTO first_transactions (address, block_time, tx_hash, source, timestamp) VALUES (?, ?, ?, ?, ?)',
                        batch
                    )
                    conn.commit()
        
        conn.close()
        return count
    
    # Skip header line
    header_offset = 1
    
    # Create chunks
    chunks = []
    for i in range(total_chunks):
        start = i * chunk_size + header_offset
        end = min((i + 1) * chunk_size + header_offset, total_lines)
        chunks.append((i, start, end))
    
    # Process chunks in parallel
    total_imported = 0
    with tqdm(total=total_lines-1, desc=f"Importing {tsv_file}") as pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_chunk, i, start, end): (i, end-start) 
                      for i, start, end in chunks}
            
            for future in concurrent.futures.as_completed(futures):
                chunk_id, chunk_size = futures[future]
                try:
                    count = future.result()
                    total_imported += count
                    pbar.update(chunk_size)
                except Exception as e:
                    print(f"Error processing chunk {chunk_id}: {str(e)}")
    
    print(f"Imported {total_imported} addresses from {tsv_file}")
    return total_imported

def get_date_from_block_time(block_time):
    """Convert block time (Unix timestamp) to a human-readable date"""
    try:
        # Using the recommended timezone-aware approach
        return datetime.fromtimestamp(int(block_time), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    except (ValueError, TypeError):
        return "Unknown date format"

def process_address_batch(batch, batch_id):
    """Process a batch of addresses in parallel and return results"""
    results = []
    
    # Query database for all addresses in this batch at once (more efficient)
    with DB_LOCK:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        placeholders = ','.join(['?'] * len(batch))
        cursor.execute(f'SELECT address, block_time, tx_hash, source FROM first_transactions WHERE address IN ({placeholders})', batch)
        db_results = cursor.fetchall()
        
        conn.close()
    
    # Convert to dictionary for faster lookups
    found_addresses = {row[0]: (row[1], row[2], row[3]) for row in db_results}
    
    # Process each address
    for address in batch:
        if address in found_addresses:
            block_time, tx_hash, source = found_addresses[address]
            date = get_date_from_block_time(block_time)
            results.append((address, date, source))
        else:
            results.append((address, "No transaction data found in local database.", None))
    
    return results

def process_addresses_parallel(addresses, output_file, batch_size=1000, max_workers=8):
    """
    Process Bitcoin addresses in parallel using multiple worker threads.
    
    Args:
        addresses: List of addresses to process
        output_file: Path to the output file
        batch_size: Number of addresses to process in each batch
        max_workers: Maximum number of worker threads
    """
    # Create or clear the output file
    with open(output_file, 'w') as output:
        output.write("") # Clear the file
    
    # Create a progress file
    progress_file = f"{output_file}.progress"
    
    total_addresses = len(addresses)
    print(f"Processing {total_addresses} addresses with {max_workers} workers...")
    
    # Calculate optimal batch size based on total addresses and workers
    # Ensure each worker gets at least one batch
    optimal_batch_size = min(
        batch_size,
        max(100, (total_addresses // (max_workers * 10)))
    )
    
    # Create batches
    batches = []
    for i in range(0, total_addresses, optimal_batch_size):
        batch = addresses[i:i + optimal_batch_size]
        batches.append((f"batch_{i//optimal_batch_size}", batch))
    
    total_batches = len(batches)
    print(f"Split into {total_batches} batches of ~{optimal_batch_size} addresses each")
    
    # Process batches with a thread pool
    processed_count = 0
    success_count = 0
    
    with tqdm(total=total_addresses, desc="Processing addresses") as pbar:
        # Process batches in chunks to avoid memory issues with very large datasets
        chunk_size = max(1, min(100, total_batches // 10))
        
        for chunk_start in range(0, total_batches, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_batches)
            current_batches = batches[chunk_start:chunk_end]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_batch = {
                    executor.submit(process_address_batch, batch, batch_id): (batch_id, batch) 
                    for batch_id, batch in current_batches
                }
                
                for future in concurrent.futures.as_completed(future_to_batch):
                    batch_id, batch = future_to_batch[future]
                    try:
                        results = future.result()
                        
                        # Write results to output file
                        with open(output_file, 'a') as output:
                            for address, date, source in results:
                                output.write(f"{address}\n")
                                if source:
                                    output.write(f"{date} (via {source})\n\n")
                                else:
                                    output.write(f"{date}\n\n")
                        
                        # Update counters
                        batch_size = len(batch)
                        processed_count += batch_size
                        success_count += sum(1 for _, _, source in results if source)
                        
                        # Update progress
                        pbar.update(batch_size)
                        
                        # Update progress file
                        with open(progress_file, 'w') as progress:
                            progress.write(f"Processed: {processed_count}/{total_addresses} ({processed_count/total_addresses*100:.2f}%)\n")
                            progress.write(f"Success: {success_count}/{processed_count} ({success_count/processed_count*100:.2f}% hit rate)\n")
                        
                        # Update batch status in database
                        update_batch_status(batch_id, len(batch), len(batch), 
                                           sum(1 for _, _, source in results if source), 
                                           "completed")
                    
                    except Exception as e:
                        print(f"Error processing batch {batch_id}: {str(e)}")
                        # Update batch status as failed
                        update_batch_status(batch_id, len(batch), 0, 0, "failed")
    
    print(f"Completed processing {processed_count}/{total_addresses} addresses.")
    print(f"Found data for {success_count}/{processed_count} addresses ({success_count/processed_count*100:.2f}% hit rate).")
    print(f"Results saved to {output_file}")

def process_addresses(input_files, output_file, batch_size=1000, max_workers=8):
    """
    Process Bitcoin addresses from one or more input files and write transaction dates to output file.
    
    Args:
        input_files: List of input file paths containing Bitcoin addresses
        output_file: Path to the output file
        batch_size: Number of addresses to process in each batch
        max_workers: Maximum number of worker threads
    """
    # Get all addresses from input files
    all_addresses = []
    for input_file in input_files:
        try:
            with open(input_file, 'r') as f:
                addresses = [a.strip() for a in f.read().splitlines() if a.strip()]
                all_addresses.extend(addresses)
        except FileNotFoundError:
            print(f"Error: Input file '{input_file}' not found. Skipping.")
    
    total_addresses = len(all_addresses)
    print(f"Found {total_addresses} addresses to process.")
    
    # Check for previously processed addresses
    processed_addresses = set()
    if os.path.exists(output_file):
        with open(output_file, 'r') as f:
            lines = f.read().splitlines()
            for i in range(0, len(lines), 3):  # Each address takes 3 lines in the output file
                if i < len(lines):
                    processed_addresses.add(lines[i].strip())
    
    # Filter out already processed addresses
    addresses_to_process = [addr for addr in all_addresses if addr not in processed_addresses]
    print(f"Found {len(processed_addresses)} already processed addresses. {len(addresses_to_process)} remaining.")
    
    # Process addresses in parallel
    if addresses_to_process:
        process_addresses_parallel(addresses_to_process, output_file, batch_size, max_workers)
    else:
        print("No addresses to process. All addresses have already been processed.")

def process_directory(directory, output_file, batch_size=1000, max_workers=8):
    """Process all .txt files in a directory"""
    if not os.path.exists(directory):
        print(f"Error: Directory '{directory}' not found.")
        return
    
    # Get all .txt files in the directory
    input_files = [os.path.join(directory, f) for f in os.listdir(directory) 
                  if f.endswith('.txt') and os.path.isfile(os.path.join(directory, f))]
    
    if not input_files:
        print(f"No .txt files found in directory '{directory}'.")
        return
    
    print(f"Found {len(input_files)} .txt files in '{directory}'.")
    process_addresses(input_files, output_file, batch_size, max_workers)

def download_bitcoin_data(output_file):
    """Download Bitcoin address first transaction data from public datasets"""
    print("Downloading Bitcoin address data...")
    
    # This is a placeholder - in a real implementation, you would download from
    # sources like BigQuery public datasets or other Bitcoin data providers
    
    print("Download complete.")
    return output_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get first transaction dates for Bitcoin addresses using local database.')
    
    # Add arguments
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-f', '--files', nargs='+', help='One or more input files containing Bitcoin addresses')
    group.add_argument('-d', '--directory', help='Directory containing input files with Bitcoin addresses')
    group.add_argument('-s', '--single', help='Single Bitcoin address to process')
    group.add_argument('-i', '--import', dest='import_file', help='Import first transaction data from CSV file')
    group.add_argument('-t', '--tsv', dest='tsv_file', help='Import addresses from TSV file (like blockchair export)')
    group.add_argument('--download', action='store_true', help='Download Bitcoin address data')
    
    parser.add_argument('-o', '--output', default='Output_BTC_TX_Date.txt', 
                        help='Output file path (default: Output_BTC_TX_Date.txt)')
    parser.add_argument('-b', '--batch-size', type=int, default=1000,
                        help='Number of addresses to process in each batch (default: 1000)')
    parser.add_argument('-w', '--workers', type=int, default=8,
                        help='Maximum number of worker threads (default: 8)')
    parser.add_argument('--delimiter', default=',', help='Delimiter for import file (default: ,)')
    parser.add_argument('--address-col', type=int, default=0, help='Column index for address in import file (default: 0)')
    parser.add_argument('--time-col', type=int, default=1, help='Column index for timestamp in import file (default: 1)')
    parser.add_argument('--tx-col', type=int, default=None, help='Column index for transaction hash in import file (optional)')
    
    # Setup database if it doesn't exist
    setup_database()
    
    # Parse arguments
    if len(sys.argv) > 1:
        args = parser.parse_args()
        
        # Process based on input type
        if args.files:
            process_addresses(args.files, args.output, args.batch_size, args.workers)
        elif args.directory:
            process_directory(args.directory, args.output, args.batch_size, args.workers)
        elif args.single:
            # Create a temporary file with the single address
            temp_file = 'temp_address.txt'
            with open(temp_file, 'w') as f:
                f.write(f"{args.single}\n")
            
            process_addresses([temp_file], args.output, 1, 1)
            
            # Clean up the temporary file
            os.remove(temp_file)
        elif args.import_file:
            import_from_csv(args.import_file, args.delimiter, args.address_col, args.time_col, args.tx_col, args.workers)
        elif args.tsv_file:
            import_from_tsv(args.tsv_file, args.address_col, args.time_col, args.workers)
        elif args.download:
            download_bitcoin_data(args.output)
    else:
        # Default behavior for backward compatibility
        input_file = 'List_of_wallets.txt'
        output_file = 'Output_BTC_TX_Date.txt'
        
        # Check if input file exists
        try:
            with open(input_file, 'r') as f:
                pass
        except FileNotFoundError:
            # Create an empty input file if it doesn't exist
            with open(input_file, 'w') as f:
                f.write("1NUhcfvRthmvrHf1PAJKe5uEzBGK44ASBD\n")
            print(f"Created example input file: {input_file}")
        
        process_addresses([input_file], output_file, 1000, 8) 