"""
Batch Process C: Third transformation

Read from a file, transform, write to a new file.
In this case, convert temperature from Kelvin to Fahrenheit.

Note: 
This is a batch process, but the file objects we use are 
often called 'file-like objects' or 'streams'.
Streaming differs in that the input data is unbounded.

Use logging, very helpful when working with batch and streaming processes. 

"""

# Import from Python Standard Library

import csv
import logging

# Set up basic configuration for logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Declare program constants

INPUT_FILE_NAME = "batchfile_2_kelvin.csv"
OUTPUT_FILE_NAME = "batchfile_3_fahrenheit.csv"

# Define program functions (bits of reusable code)
# Use docstrings - and indentation matters!


def convert_k_to_f(temp_k):
    """
    Convert temperature from Kelvin to Fahrenheit.
    
    :param temp_k: Temperature in Kelvin
    :return: Temperature in Fahrenheit
    """
    return (temp_k - 273.15) * 9/5 + 32


def process_rows(input_file_name, output_file_name):
    """
    Process rows in the input CSV file, convert temperatures from Kelvin to Fahrenheit, and write to the output file.
    
    :param input_file_name: Input CSV file name
    :param output_file_name: Output CSV file name
    """
    try:
        with open(input_file_name, mode='r') as input_file, open(output_file_name, mode='w', newline='') as output_file:
            reader = csv.reader(input_file)
            writer = csv.writer(output_file)
            
            # Write header row
            header = next(reader)
            writer.writerow(header)
            
            for row in reader:
                # Assuming the temperature is in the second column
                temp_k = float(row[1])
                temp_f = convert_k_to_f(temp_k)
                row[1] = temp_f
                writer.writerow(row)

        logging.info(f"Conversion complete. Results written to {output_file_name}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


# ---------------------------------------------------------------------------
# If this is the script we are running, then call some functions and execute code!
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        logging.info("===============================================")
        logging.info("Starting batch process C.")
        process_rows(INPUT_FILE_NAME, OUTPUT_FILE_NAME)
        logging.info("Processing complete! Check for new file.")
        logging.info("===============================================")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

