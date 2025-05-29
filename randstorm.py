import random
import time
from multiprocessing import Pool
import coincurve
import hashlib
import base58
from rich.console import Console
import numpy as np
import cupy as cp  # For GPU operations
from numba import cuda, jit
import concurrent.futures

console = Console()
# =========================================================================================
Randstorm = '''฿RANDSTORM฿
'''
# =========================================================================================
console.print(Randstorm)

class SecureRandom:
    def __init__(self, seed):
        self.rng_state = None
        self.rng_pool = []
        self.rng_pptr = 0
        self.rng_psize = 32
        random.seed(seed)
        for _ in range(self.rng_psize):
            self.rng_pool.append(random.randint(0, 255))
        self.rng_pptr = 0

    def rng_get_byte(self):
        if self.rng_pptr >= len(self.rng_pool):
            self.rng_pptr = 0
            self.rng_pool = [random.randint(0, 255) for _ in range(self.rng_psize)]
        byte = self.rng_pool[self.rng_pptr]
        self.rng_pptr += 1
        return byte

    def rng_get_bytes(self, length):
        result = bytearray(length)
        for i in range(length):
            result[i] = self.rng_get_byte()
        return result

class HybridRandstorm:
    def __init__(self, batch_size=1000000):  # 1M batch size
        self.batch_size = batch_size
        self.threads_per_block = 512
        self.blocks_per_grid = min(
            4096,
            (batch_size + self.threads_per_block - 1) // self.threads_per_block
        )
        self.num_streams = 4
        # Calculate correct buffer size for each stream
        self.stream_batch_size = batch_size // self.num_streams
        self.streams = [cp.cuda.Stream() for _ in range(self.num_streams)]
        # Allocate correctly sized buffers
        self.output_buffers = [
            cp.zeros(self.stream_batch_size * 32, dtype=cp.uint8) 
            for _ in range(self.num_streams)
        ]

    def _gpu_rng(self, seed, size):
        """Optimized random number generation using GPU with correct shapes"""
        try:
            kernel = cp.RawKernel(r'''
            extern "C" __global__
            void rng_kernel(unsigned long long seed, int size, unsigned char* output) {
                __shared__ unsigned char shared_output[8192];
                
                int tid = blockDim.x * blockIdx.x + threadIdx.x;
                int stride = blockDim.x * gridDim.x;
                int local_tid = threadIdx.x;
                
                unsigned long long state = seed + tid;
                
                for (int idx = tid; idx < size; idx += stride) {
                    #pragma unroll 8
                    for (int j = 0; j < 32; j += 4) {
                        state = state * 6364136223846793005ULL + 1;
                        unsigned int rand_val = (unsigned int)(state >> 32);
                        
                        if (local_tid < 2048) {
                            shared_output[local_tid * 4] = rand_val & 0xFF;
                            shared_output[local_tid * 4 + 1] = (rand_val >> 8) & 0xFF;
                            shared_output[local_tid * 4 + 2] = (rand_val >> 16) & 0xFF;
                            shared_output[local_tid * 4 + 3] = (rand_val >> 24) & 0xFF;
                        }
                        __syncthreads();
                        
                        if (idx < size) {
                            output[idx * 32 + j] = shared_output[local_tid * 4];
                            output[idx * 32 + j + 1] = shared_output[local_tid * 4 + 1];
                            output[idx * 32 + j + 2] = shared_output[local_tid * 4 + 2];
                            output[idx * 32 + j + 3] = shared_output[local_tid * 4 + 3];
                        }
                        __syncthreads();
                    }
                }
            }
            ''', 'rng_kernel')

            # Process in streams with correct sizes
            results = []
            stream_size = self.stream_batch_size  # Use pre-calculated stream batch size
            
            for i, stream in enumerate(self.streams):
                with stream:
                    start_idx = i * stream_size
                    # Launch kernel with correct size parameters
                    kernel((self.blocks_per_grid,), (self.threads_per_block,),
                          (seed + start_idx, stream_size, self.output_buffers[i]),
                          shared_mem=8192)
                    # Reshape with correct dimensions
                    results.append(self.output_buffers[i].reshape(stream_size, 32))
            
            # Synchronize streams
            for stream in self.streams:
                stream.synchronize()
            
            # Combine results with verified shapes
            return cp.concatenate(results)
            
        except Exception as e:
            print(f"GPU Error: {str(e)}")
            return None

    def process_batch(self, seed, target_address):
        try:
            print(f"\nProcessing batch starting at seed {seed}")  # Debug print
            
            # Generate random numbers
            print("Generating random numbers on GPU...")  # Debug print
            random_numbers = self._gpu_rng(seed, self.batch_size)
            if random_numbers is None:
                print("Failed to generate random numbers")  # Debug print
                return [], []
            
            # Verify shape before transfer
            expected_shape = (self.batch_size, 32)
            print(f"Random numbers shape: {random_numbers.shape}")  # Debug print
            if random_numbers.shape != expected_shape:
                print(f"Shape mismatch: got {random_numbers.shape}, expected {expected_shape}")
                return [], []
            
            # Transfer to CPU
            print("Transferring to CPU...")  # Debug print
            private_keys = cp.asnumpy(random_numbers)
            print(f"Transferred {len(private_keys)} keys to CPU")  # Debug print
            
            # Process results
            print("Processing keys...")  # Debug print
            with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
                # Convert bytes to hex strings in parallel
                private_keys = list(executor.map(
                    lambda pk: bytes(pk).hex(),
                    private_keys
                ))
                print(f"Converted {len(private_keys)} keys to hex")  # Debug print
                
                # Process addresses in smaller batches
                batch_size = 1000
                addresses = []
                total_batches = len(private_keys) // batch_size + (1 if len(private_keys) % batch_size else 0)
                
                print(f"Processing {total_batches} address batches...")  # Debug print
                for i in range(0, len(private_keys), batch_size):
                    batch = private_keys[i:i + batch_size]
                    batch_addresses = list(executor.map(
                        generate_compressed_P2P_address,
                        batch
                    ))
                    addresses.extend(batch_addresses)
                    # Print progress for address generation
                    batch_num = i // batch_size + 1
                    print(f"\rGenerating addresses: batch {batch_num}/{total_batches} "
                          f"({(batch_num/total_batches)*100:.1f}%)", end='', flush=True)
            
            print("\nChecking for matches...")  # Debug print
            # Check for matches
            matches = [i for i, addr in enumerate(addresses) if addr == target_address]
            if matches:
                print(f"\nFound {len(matches)} matches!")  # Debug print
            else:
                print("\nNo matches found in this batch")  # Debug print
            
            return matches, private_keys
            
        except Exception as e:
            print(f"\nError in process_batch: {str(e)}")
            return [], []

def custom_private_key_generator(rng_simulator=None):
    # If no random number generator simulator is provided, create a new one
    rng = SecureRandom()

    # Generate 32 bytes (256 bits) as the private key
    private_key_bytes = rng.rng_get_bytes(32)

    # Convert the bytes to a hexadecimal string
    private_key_hex = private_key_bytes.hex()

    # Return the generated private key in hexadecimal format
    return private_key_hex

def generate_compressed_P2P_address(private_key):
    # Convert the private key from hexadecimal string to bytes
    private_key_bytes = bytes.fromhex(private_key)

    # Derive the compressed public key from the private key using the coincurve library
    public_key = coincurve.PrivateKey(private_key_bytes).public_key.format(compressed=True)

    # Calculate the RIPEMD160 hash of the SHA256 hash of the compressed public key
    public_key_hash = hashlib.new('ripemd160', hashlib.sha256(public_key).digest()).hexdigest()

    # Prepend '00' to the public key hash to create the extended public key hash
    extended_public_key_hash = '00' + public_key_hash

    # Calculate the checksum using double SHA256 on the extended public key hash
    checksum = hashlib.sha256(hashlib.sha256(bytes.fromhex(extended_public_key_hash)).digest()).hexdigest()[:8]

    # Concatenate the extended public key hash and the checksum, then encode in base58
    p2pkh_address = base58.b58encode(bytes.fromhex(extended_public_key_hash + checksum))

    # Return the compressed P2PKH address as a string
    return p2pkh_address.decode()

def generate_hex(args):
    """Modified to accept args as a tuple containing seed and target_address"""
    seed, target_address = args  # Unpack the arguments
    
    # Set the total number of keys to generate
    hex_keys = 1400000000  # 1.4 billion keys for 1 day
    current_seed = seed
    keys_per_update = 1000  # Update progress more frequently

    # Create a secure random number generator
    secure_rng = SecureRandom(current_seed)
    
    # Initialize timing for this process
    process_start_time = time.time()
    last_update_time = process_start_time
    keys_since_update = 0

    try:
        for i in range(hex_keys):
            # Generate a random private key in hexadecimal format
            random_bytes = secure_rng.rng_get_bytes(32)
            hex_representation = random_bytes.hex()
            private_key = hex_representation

            # Generate the compressed P2PKH address from the private key
            p2pkh_address = generate_compressed_P2P_address(private_key)

            # Check if the generated address matches
            if p2pkh_address == target_address:
                print(f"\nMatch found!\nPrivate Key: {private_key}")
                with open("matched_private_keys.txt", "a") as file:
                    file.write(f"{private_key}\n")

            current_seed += 1
            keys_since_update += 1
            
            # Update progress more frequently
            if keys_since_update >= keys_per_update:
                current_time = time.time()
                elapsed_time = current_time - last_update_time
                total_elapsed = current_time - process_start_time
                
                # Calculate speeds
                recent_speed = keys_since_update / elapsed_time if elapsed_time > 0 else 0
                overall_speed = (i + 1) / total_elapsed if total_elapsed > 0 else 0
                
                # Calculate progress percentage
                progress = (i + 1) / hex_keys * 100
                
                # Estimate time remaining
                keys_remaining = hex_keys - (i + 1)
                time_remaining = keys_remaining / overall_speed if overall_speed > 0 else 0
                
                print(f"\rProcess {seed} | Progress: {progress:.2f}% | "
                      f"Current: {i:,} of {hex_keys:,} | "
                      f"Speed: {recent_speed:.2f} keys/s | "
                      f"Time remaining: {time_remaining/3600:.1f} hours | "
                      f"Seed: {current_seed}", end='', flush=True)
                
                # Reset counters
                keys_since_update = 0
                last_update_time = current_time

    except KeyboardInterrupt:
        print("\nStopping gracefully...")
        return current_seed
    except Exception as e:
        print(f"\nError in generate_hex: {str(e)}")
        return current_seed

    return current_seed

def process_gpu_range(gpu_id, start_seed, end_seed, target_address, batch_size):
    """Process a range of seeds on one GPU with improved monitoring"""
    try:
        print(f"\nInitializing GPU {gpu_id}...")  # Debug print
        with cp.cuda.Device(gpu_id):
            randstorm = HybridRandstorm(batch_size)
            current_seed = start_seed
            total_seeds = end_seed - start_seed
            start_time = time.time()
            last_update_time = start_time
            processed_since_update = 0
            total_processed = 0
            
            print(f"GPU {gpu_id} starting processing from seed {start_seed}")  # Debug print
            
            while current_seed < end_seed:
                batch_start_time = time.time()  # Track batch time
                
                # Process batch
                matches, private_keys = randstorm.process_batch(
                    current_seed, 
                    target_address
                )
                
                batch_time = time.time() - batch_start_time  # Calculate batch time
                
                if matches:
                    print(f"\nMatch found on GPU {gpu_id}!")
                    return private_keys[matches[0]]
                
                # Update counters
                batch_processed = len(private_keys) if private_keys else 0
                current_seed += batch_processed
                processed_since_update += batch_processed
                total_processed += batch_processed
                
                # Always show at least initial progress
                current_time = time.time()
                elapsed_time = current_time - start_time
                progress = (total_processed / total_seeds) * 100
                speed = batch_processed / batch_time if batch_time > 0 else 0
                
                # Format status message
                status = (
                    f"\rGPU {gpu_id} | "
                    f"Progress: {progress:.2f}% | "
                    f"Speed: {speed:,.0f} keys/s | "
                    f"Current seed: {current_seed:,} | "
                    f"Batch time: {batch_time:.2f}s"
                )
                print(status, end='', flush=True)
                
                # Free memory periodically
                if progress % 1 < 0.1:  # Every 1% progress
                    cp.get_default_memory_pool().free_all_blocks()
                    print(f"\nGPU {gpu_id} memory cleared")  # Debug print
                
    except Exception as e:
        print(f"\nError on GPU {gpu_id}: {str(e)}")
        return None

def hybrid_search(start_seed, target_address, num_gpus=1):
    """Main search function using both GPU and CPU"""
    try:
        print("\nInitializing hybrid search...")
        randstorm = HybridRandstorm()
        
        # Split work between GPUs
        seeds_per_gpu = 1400000000 // num_gpus
        gpu_ranges = [(start_seed + i * seeds_per_gpu, 
                      start_seed + (i + 1) * seeds_per_gpu) 
                     for i in range(num_gpus)]
        
        print(f"Distributing work across {num_gpus} GPUs...")
        
        # Process on each GPU in parallel
        with concurrent.futures.ProcessPoolExecutor(max_workers=num_gpus) as executor:
            futures = []
            for gpu_id, (start, end) in enumerate(gpu_ranges):
                future = executor.submit(
                    process_gpu_range,
                    gpu_id, start, end,
                    target_address,
                    randstorm.batch_size
                )
                futures.append(future)
                
            try:
                # Wait for results
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    if result:
                        print(f"\nMatch found! Private key: {result}")
                        return result
                        
            except KeyboardInterrupt:
                print("\nReceived interrupt signal. Stopping search gracefully...")
                for future in futures:
                    future.cancel()
                executor.shutdown(wait=False)
                return None
                
    except Exception as e:
        print(f"\nError in hybrid search: {str(e)}")
        return None

def get_available_gpus():
    """Get number of available CUDA GPUs"""
    try:
        return cp.cuda.runtime.getDeviceCount()
    except:
        return 0

if __name__ == '__main__':
    try:
        # Check for available GPUs
        num_gpus = get_available_gpus()
        
        if num_gpus == 0:
            print("No CUDA GPUs detected. Falling back to CPU-only mode...")
            # Use CPU implementation
            target_address = "1NUhcfvRthmvrHf1PAJKe5uEzBGK44ASBD"
            start_seed = 1393635661000
            start_time = time.time()  # Define start_time here
            
            # Create argument tuples containing both seed and target_address
            num_processes = 6  # Use CPU cores instead
            seeds = range(start_seed, start_seed + num_processes)
            args = [(seed, target_address) for seed in seeds]
            
            print(f"Starting search with {num_processes} CPU processes...")
            
            try:
                # Use multiprocessing.Pool to parallelize the generation of random keys
                with Pool(num_processes) as pool:
                    final_seeds = pool.map(generate_hex, args)
                    
                # Calculate total runtime
                total_elapsed_time = time.time() - start_time
                hex_keys = 1400000000 * num_processes
                total_addresses_per_second = hex_keys / total_elapsed_time if total_elapsed_time > 0 else 0
                
                print(f"\n\nTotal Runtime: {total_elapsed_time:.2f} seconds")
                print(f"Total Speed: {total_addresses_per_second:.2f} addresses/second")
                print(f"Seeds processed: {min(seeds)} to {max(final_seeds)}")
                
            except KeyboardInterrupt:
                print("\nStopping CPU search gracefully...")
                pool.terminate()
                pool.join()
            
        else:
            print(f"Found {num_gpus} CUDA GPUs. Using GPU acceleration...")
            target_address = "1NUhcfvRthmvrHf1PAJKe5uEzBGK44ASBD"
            start_seed = 1393635661000
            
            result = hybrid_search(start_seed, target_address, num_gpus)
            
            if result:
                print(f"Found matching private key: {result}")
            else:
                print("\nSearch completed without finding a match")
                
    except KeyboardInterrupt:
        print("\nScript terminated by user")
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
    finally:
        # Cleanup code here if needed
        print("\nCleanup complete")
