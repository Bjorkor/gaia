#!/home/bjorkor/projects/gaia/venv/bin/python
import os
from astropy.io import fits

def get_sql_type(fits_type):
    """Map FITS data type to SQL data type."""
    if fits_type in ['K', 'J']:
        return 'BIGINT'
    elif fits_type == 'I':
        return 'INT'
    elif fits_type == 'E':
        return 'FLOAT'
    elif fits_type == 'D':
        return 'DOUBLE'
    elif fits_type == 'A':
        return 'VARCHAR(255)'
    elif fits_type == 'L':
        return 'TINYINT(1)'  # Boolean in SQL
    else:
        return 'TEXT'

def generate_create_table_statement(file_path, table_name="fits_data"):
    """Generate a CREATE TABLE statement for MariaDB from FITS file."""
    with fits.open(file_path) as hdul:
        # Assuming the first table HDU contains the data
        hdu = hdul[1]
        columns = hdu.columns
        
        sql_columns = []
        for col in columns:
            sql_type = get_sql_type(col.format[0])
            sql_columns.append(f"`{col.name}` {sql_type}")
        
        create_table_sql = f"CREATE TABLE `gdr1_gaia_source_fits` (\n  " + ",\n  ".join(sql_columns) + "\n);"
        return create_table_sql

def process_directory(directory):
    """Process the first FITS file in a directory and generate SQL statement."""
    for filename in os.listdir(directory):
        if filename.endswith('.fits'):
            file_path = os.path.join(directory, filename)
            print(f"Processing file: {file_path}")
            create_table_sql = generate_create_table_statement(file_path, table_name=filename.split('.')[0])
            print("\nGenerated SQL CREATE TABLE statement:\n")
            print(create_table_sql)
            break  # Process only the first FITS file

if __name__ == "__main__":
    # Set the directory containing the FITS files
    fits_directory = "/mnt/nas/gaia_data/gdr1/gaia_source/fits"
    
    # Process the directory and generate SQL CREATE TABLE statement
    process_directory(fits_directory)
