import csv
import os

def extract_addresses(tsv_file, output_dir='extracted_addresses', max_addresses_per_file=1000):
    """
    Extract Bitcoin addresses from a TSV file and save them to multiple text files.
    
    Args:
        tsv_file: Path to the TSV file containing Bitcoin addresses
        output_dir: Directory to save the output files
        max_addresses_per_file: Maximum number of addresses per output file
    """
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")
    
    try:
        with open(tsv_file, 'r', encoding='utf-8') as f:
            # Create a CSV reader with tab delimiter
            reader = csv.reader(f, delimiter='\t')
            
            # Read the header to determine which column contains the addresses
            header = next(reader)
            
            # Find the address column (usually named 'address')
            address_col_index = None
            for i, col_name in enumerate(header):
                if 'address' in col_name.lower():
                    address_col_index = i
                    break
            
            if address_col_index is None:
                print(f"Could not find address column in {tsv_file}")
                print(f"Available columns: {header}")
                return
            
            print(f"Found address column: {header[address_col_index]}")
            
            # Process the addresses
            addresses = []
            file_count = 1
            line_count = 0
            
            for row in reader:
                line_count += 1
                if len(row) > address_col_index:
                    address = row[address_col_index].strip()
                    if address:  # Skip empty addresses
                        addresses.append(address)
                
                # Write to file when we reach the maximum number of addresses per file
                if len(addresses) >= max_addresses_per_file:
                    write_addresses_to_file(addresses, output_dir, file_count)
                    addresses = []
                    file_count += 1
            
            # Write any remaining addresses
            if addresses:
                write_addresses_to_file(addresses, output_dir, file_count)
            
            print(f"Processed {line_count} lines from {tsv_file}")
            print(f"Created {file_count} output files in {output_dir}")
    
    except Exception as e:
        print(f"Error processing file {tsv_file}: {e}")

def write_addresses_to_file(addresses, output_dir, file_number):
    """Write a list of addresses to a text file"""
    output_file = os.path.join(output_dir, f"addresses_{file_number}.txt")
    with open(output_file, 'w', encoding='utf-8') as f:
        for address in addresses:
            f.write(f"{address}\n")
    print(f"Wrote {len(addresses)} addresses to {output_file}")

if __name__ == "__main__":
    tsv_file = "blockchair_bitcoin_addresses_and_balance_March_27_2025.tsv"
    
    # Check if the file exists
    if not os.path.exists(tsv_file):
        print(f"Error: File {tsv_file} not found.")
    else:
        # Extract addresses with 5000 addresses per file
        extract_addresses(tsv_file, max_addresses_per_file=5000) 