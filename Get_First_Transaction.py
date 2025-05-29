import requests
from datetime import datetime, timezone
import os
import sys
import argparse
import time
import json
import random
import concurrent.futures
import threading
from tqdm import tqdm
from collections import defaultdict
from redis import Redis
import aiohttp
import asyncio

# Cache file for storing transaction data
CACHE_FILE = "tx_cache.json"
CACHE_LOCK = threading.Lock()
MEMORY_CACHE = {}
MAX_CACHE_SIZE = 10000

# Implement Redis or MongoDB for faster caching
redis_cache = Redis(host='localhost', port=6379, db=0)

# API status tracking
API_STATUS = {
    "btcscan": {"available": True, "cooldown_until": 0, "success_count": 0, "fail_count": 0, "consecutive_fails": 0},
    "blockchair": {"available": True, "cooldown_until": 0, "success_count": 0, "fail_count": 0, "consecutive_fails": 0},
    "blockchain.info": {"available": True, "cooldown_until": 0, "success_count": 0, "fail_count": 0, "consecutive_fails": 0},
    "blockcypher": {"available": True, "cooldown_until": 0, "success_count": 0, "fail_count": 0, "consecutive_fails": 0},
    "mempool.space": {"available": True, "cooldown_until": 0, "success_count": 0, "fail_count": 0, "consecutive_fails": 0},
    "blockstream": {"available": True, "cooldown_until": 0, "success_count": 0, "fail_count": 0, "consecutive_fails": 0},
    "bitaps": {"available": True, "cooldown_until": 0, "success_count": 0, "fail_count": 0, "consecutive_fails": 0},
    "blockbook": {"available": True, "cooldown_until": 0, "success_count": 0, "fail_count": 0, "consecutive_fails": 0},
    "bitcore": {"available": True, "cooldown_until": 0, "success_count": 0, "fail_count": 0, "consecutive_fails": 0}
}

# API configurations
API_CONFIGS = {
    "btcscan": {
        "url": "https://btcscan.org/api/address/{}/txs/chain",
        "date_path": ["status", "block_time"],
        "timeout": 5,
        "max_requests_per_minute": 10
    },
    "blockchair": {
        "url": "https://api.blockchair.com/bitcoin/dashboards/address/{}",
        "date_path": ["data", "{}", "first_seen_receiving"],
        "timeout": 5,
        "max_requests_per_minute": 5
    },
    "blockchain.info": {
        "url": "https://blockchain.info/rawaddr/{}?limit=1",
        "date_path": ["txs", 0, "time"],
        "timeout": 5,
        "max_requests_per_minute": 10
    },
    "blockcypher": {
        "url": "https://api.blockcypher.com/v1/btc/main/addrs/{}?limit=1",
        "date_path": ["txrefs", 0, "block_time"],
        "timeout": 5,
        "max_requests_per_minute": 3
    },
    "mempool.space": {
        "url": "https://mempool.space/api/address/{}",
        "date_path": ["chain_stats", "funded_txo_count"],  # Not a timestamp, but can indicate first tx
        "timeout": 5,
        "max_requests_per_minute": 10
    },
    "blockstream": {
        "url": "https://blockstream.info/api/address/{}",
        "date_path": ["chain_stats", "tx_count"],  # Not a timestamp, but can indicate first tx
        "timeout": 5,
        "max_requests_per_minute": 10
    },
    "bitaps": {
        "url": "https://api.bitaps.com/btc/v1/blockchain/address/{}",
        "date_path": ["data", "first_tx_time"],
        "timeout": 5,
        "max_requests_per_minute": 10
    },
    "blockbook": {
        "url": "https://api.blockbook.io/btc/address/{}",
        "date_path": ["transactions", 0, "time"],
        "timeout": 5,
        "max_requests_per_minute": 10
    },
    "bitcore": {
        "url": "https://api.bitcore.io/api/BTC/address/{}",
        "date_path": ["transactions", 0, "time"],
        "timeout": 5,
        "max_requests_per_minute": 10
    }
}

# Cooldown periods in seconds
SHORT_COOLDOWN = 60  # 1 minute
MEDIUM_COOLDOWN = 300  # 5 minutes
LONG_COOLDOWN = 1800  # 30 minutes

def load_cache():
    """Load transaction cache from file"""
    global MEMORY_CACHE
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                MEMORY_CACHE = json.load(f)
                print(f"Loaded {len(MEMORY_CACHE)} cached transactions")
        except (json.JSONDecodeError, FileNotFoundError):
            MEMORY_CACHE = {}
            print("Created new cache")
    else:
        MEMORY_CACHE = {}
        print("Created new cache")

def save_cache():
    """Save transaction cache to file"""
    with CACHE_LOCK:
        with open(CACHE_FILE, 'w') as f:
            json.dump(MEMORY_CACHE, f)

def get_cached_transaction(address):
    """Get transaction data from Redis cache"""
    cached = redis_cache.get(address)
    if cached:
        return json.loads(cached)
    return None

def save_to_cache(address, data):
    """Save to Redis with expiration"""
    redis_cache.setex(address, 86400, json.dumps(data))  # 24 hour expiration

def get_available_apis():
    """Get list of currently available APIs"""
    current_time = time.time()
    available_apis = []
    
    for api_name, status in API_STATUS.items():
        if current_time > status["cooldown_until"]:
            available_apis.append(api_name)
    
    return available_apis

def mark_api_rate_limited(api_name):
    """Mark an API as rate limited and set cooldown period"""
    global API_STATUS
    
    API_STATUS[api_name]["fail_count"] += 1
    API_STATUS[api_name]["consecutive_fails"] += 1
    consecutive_fails = API_STATUS[api_name]["consecutive_fails"]
    
    # Determine cooldown period based on consecutive failures
    if consecutive_fails < 3:
        cooldown = SHORT_COOLDOWN
    elif consecutive_fails < 10:
        cooldown = MEDIUM_COOLDOWN
    else:
        cooldown = LONG_COOLDOWN
    
    API_STATUS[api_name]["cooldown_until"] = time.time() + cooldown
    print(f"API {api_name} rate limited. Cooling down for {cooldown//60} minutes.")

def mark_api_success(api_name):
    """Mark an API request as successful"""
    global API_STATUS
    
    API_STATUS[api_name]["success_count"] += 1
    API_STATUS[api_name]["consecutive_fails"] = 0

def get_first_transaction(wallet_address):
    """
    Get the first transaction for a wallet address using multiple APIs.
    
    Args:
        wallet_address: The Bitcoin address to query
    """
    # Check cache first
    cached_data = get_cached_transaction(wallet_address)
    if cached_data:
        return cached_data
    
    # Get available APIs
    available_apis = get_available_apis()
    if not available_apis:
        print("All APIs are currently rate limited. Waiting for cooldown periods to expire...")
        time.sleep(5)  # Wait a bit before trying again
        return None
    
    # Randomly select an available API
    api_name = random.choice(available_apis)
    
    # Get the configuration for the selected API
    api_config = API_CONFIGS[api_name]
    
    try:
        url = api_config["url"].format(wallet_address)
        response = requests.get(url, timeout=api_config["timeout"])
        
    if response.status_code == 200:
            data = response.json()
            
            # Handle empty response
            if not data:
                return None
            
            # Extract date using the path specific to this API
            try:
                date_value = data
                date_path = api_config["date_path"]
                
                # Special handling for blockchair which needs the address in the path
                if api_name == "blockchair" and "{}" in str(date_path):
                    date_path = [p.format(wallet_address) if isinstance(p, str) and "{}" in p else p for p in date_path]
                
                for key in date_path:
                    if isinstance(key, int) and isinstance(date_value, list) and len(date_value) > key:
                        date_value = date_value[key]
                    elif isinstance(key, str) and isinstance(date_value, dict) and key in date_value:
                        date_value = date_value[key]
                    else:
                        # Path doesn't exist in the response
                        return None
                
                # Create a standardized response format
                result = {"api": api_name, "status": {"block_time": date_value}}
                
                # Save to cache
                save_to_cache(wallet_address, result)
                
                # Mark API success
                mark_api_success(api_name)
                
                return result
            except (KeyError, IndexError, TypeError) as e:
                # If the path doesn't exist in the response, return None
                print(f"Error extracting data from {api_name} response: {str(e)}")
                return None
        
        # Handle rate limiting (429 Too Many Requests)
        elif response.status_code == 429:
            mark_api_rate_limited(api_name)
            return None
            
        # Handle other errors
        else:
            print(f"Error {response.status_code} from {api_name} API for {wallet_address}")
            if response.status_code >= 400:
                mark_api_rate_limited(api_name)
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Request error with {api_name} API: {str(e)}")
        mark_api_rate_limited(api_name)
        return None

async def get_first_transaction_async(wallet_address):
    """Async version of get_first_transaction"""
    cached_data = get_cached_transaction(wallet_address)
    if cached_data:
        return cached_data
    
    available_apis = get_available_apis()
    if not available_apis:
        return None
    
    api_name = random.choice(available_apis)
    api_config = API_CONFIGS[api_name]
    
    async with aiohttp.ClientSession() as session:
        try:
            url = api_config["url"].format(wallet_address)
            async with session.get(url, timeout=api_config["timeout"]) as response:
                if response.status == 200:
                    data = await response.json()
                    # Process response...
                    return result
        except aiohttp.ClientError:
            mark_api_rate_limited(api_name)
        return None

def get_date_from_block_time(block_time):
    """Convert block time (Unix timestamp) to a human-readable date"""
    try:
        # Using the recommended timezone-aware approach
        return datetime.fromtimestamp(int(block_time), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    except (ValueError, TypeError):
        return "Unknown date format"

def process_address_batch(addresses, batch_id):
    """Process a batch of addresses and return results"""
    results = []
    
    for address in addresses:
        # Get transaction info with API rotation
        first_transaction_info = get_first_transaction(address)
        
        if first_transaction_info:
            api_name = first_transaction_info.get("api", "Unknown API")
            date = get_date_from_block_time(first_transaction_info["status"]["block_time"])
            results.append((address, date, api_name))
        else:
            results.append((address, "No transactions found", None))
    
    return results

def process_addresses_parallel(addresses, output_file, batch_size=100, max_workers=5):
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
    optimal_batch_size = min(
        batch_size,
        max(10, (total_addresses // (max_workers * 5)))
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
        chunk_size = max(1, min(50, total_batches // 10))
        
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
                            for address, date, api_name in results:
                                output.write(f"{address}\n")
                                if api_name:
                                    output.write(f"{date} (via {api_name})\n\n")
                                else:
                                    output.write(f"{date}\n\n")
                        
                        # Update counters
                        batch_size = len(batch)
                        processed_count += batch_size
                        success_count += sum(1 for _, _, api_name in results if api_name)
                        
                        # Update progress
                        pbar.update(batch_size)
                        
                        # Update progress file
                        with open(progress_file, 'w') as progress:
                            progress.write(f"Processed: {processed_count}/{total_addresses} ({processed_count/total_addresses*100:.2f}%)\n")
                            progress.write(f"Success: {success_count}/{processed_count} ({success_count/processed_count*100:.2f}% hit rate)\n")
                        
                    except Exception as e:
                        print(f"Error processing batch {batch_id}: {str(e)}")
            
            # Save cache after each chunk
            save_cache()
            
            # Check if all APIs are rate limited and wait if needed
            available_apis = get_available_apis()
            if not available_apis:
                min_cooldown = min([status["cooldown_until"] for status in API_STATUS.values()])
                wait_time = max(1, min_cooldown - time.time())
                print(f"All APIs are rate limited. Waiting {wait_time:.0f} seconds for cooldown...")
                time.sleep(wait_time)
    
    # Final cache save
    save_cache()
    
    print(f"Completed processing {processed_count}/{total_addresses} addresses.")
    print(f"Found data for {success_count}/{processed_count} addresses ({success_count/processed_count*100:.2f}% hit rate).")
    print(f"Results saved to {output_file}")

def process_addresses(input_files, output_file, batch_size=100, max_workers=5):
    """
    Process Bitcoin addresses from one or more input files and write transaction dates to output file.
    
    Args:
        input_files: List of input file paths containing Bitcoin addresses
        output_file: Path to the output file
        batch_size: Number of addresses to process in each batch
        max_workers: Maximum number of worker threads
    """
    # Load cache
    load_cache()
    
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

def process_directory(directory, output_file, batch_size=100, max_workers=5):
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get first transaction dates for Bitcoin addresses.')
    
    # Add arguments
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-f', '--files', nargs='+', help='One or more input files containing Bitcoin addresses')
    group.add_argument('-d', '--directory', help='Directory containing input files with Bitcoin addresses')
    group.add_argument('-s', '--single', help='Single Bitcoin address to process')
    
    parser.add_argument('-o', '--output', default='Output_BTC_TX_Date.txt', 
                        help='Output file path (default: Output_BTC_TX_Date.txt)')
    parser.add_argument('-b', '--batch-size', type=int, default=100,
                        help='Number of addresses to process in each batch (default: 100)')
    parser.add_argument('-w', '--workers', type=int, default=5,
                        help='Maximum number of worker threads (default: 5)')
    
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
        
        process_addresses([input_file], output_file, 100, 5)
