#!/home/bjorkor/projects/gaia/venv/bin/python
import os
import pymysql
import numpy as np
from astropy.io import fits
from astropy.table import Table
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def connect_to_database():
    """Connect to the MariaDB database."""
    return pymysql.connect(
        host='localhost',
        user='bjorkor',
        password='Eatmyass69*',
        database='gaia',
        autocommit=True
    )

def sanitize_row(row):
    """Convert NaN values to None (for SQL NULL) and handle other potential issues in a row."""
    sanitized = []
    for x in row:
        if isinstance(x, (np.float32, np.float64, float)):
            if np.isnan(x):
                sanitized.append(None)  # Convert NaN to None for SQL NULL
            else:
                sanitized.append(float(x))  # Ensure it's a proper float
        elif isinstance(x, (np.int32, np.int64, int)):
            sanitized.append(int(x))  # Ensure it's a proper int
        elif isinstance(x, np.bool_):
            sanitized.append(int(x))  # Convert numpy boolean to int (0 or 1)
        elif isinstance(x, bytes):
            sanitized.append(x.decode('utf-8'))  # Decode bytes to string
        elif isinstance(x, str):
            sanitized.append(x)  # Keep string as is
        else:
            sanitized.append(None)  # For any other types, insert NULL
    return sanitized

def insert_data(cursor, table_name, data):
    """Insert data into the specified table."""
    columns = ', '.join(f"`{col}`" for col in data.colnames)
    placeholders = ', '.join(['%s'] * len(data.colnames))
    insert_stmt = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    for row in data:
        sanitized_row = sanitize_row(row)
        try:
            cursor.execute(insert_stmt, tuple(sanitized_row))
        except pymysql.err.DataError as e:
            print(f"DataError: {e}")
            print(f"Problematic row: {sanitized_row}")
            raise

def process_fits_file(file_path):
    """Process a single FITS file and insert data into the database."""
    connection = connect_to_database()
    cursor = connection.cursor()
    
    try:
        with fits.open(file_path) as hdul:
            # Check if the file contains at least 2 HDUs and the second one is a table HDU
            if len(hdul) > 1 and isinstance(hdul[1], fits.BinTableHDU):
                table_data = Table(hdul[1].data)
                insert_data(cursor, 'gdr1_gaia_source_fits', table_data)
            else:
                print(f"File {file_path} does not contain the expected binary table HDU.")
    finally:
        cursor.close()
        connection.close()

def process_directory(directory):
    """Step through all FITS files in a directory and populate the database using threading."""
    fits_files = [os.path.join(directory, filename) for filename in os.listdir(directory) if filename.endswith('.fits')]
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(process_fits_file, file_path): file_path for file_path in fits_files}
        
        # Progress bar setup
        with tqdm(total=len(futures)) as pbar:
            for future in as_completed(futures):
                file_path = futures[future]
                try:
                    future.result()  # Get the result of the future
                    pbar.update(1)  # Update progress bar
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")

if __name__ == "__main__":
    fits_directory = "/mnt/nas/gaia_data/gdr1/gaia_source/fits"
    
    # Process the directory and populate the database using threading with progress bar
    process_directory(fits_directory)
