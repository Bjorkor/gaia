#!/home/bjorkor/projects/gaia/venv/bin/python
import os
from astropy.io import fits
from astropy.table import Table

def summarize_fits_file(file_path):
    """Summarize the contents of a single FITS file."""
    with fits.open(file_path) as hdul:
        print(f"\nProcessing file: {file_path}")
        
        for i, hdu in enumerate(hdul):
            print(f"  HDU {i}:")
            
            if hdu.data is None:
                print("    No data in this HDU.")
                continue
            
            # Check for image data
            if len(hdu.data.shape) == 2:
                print("    Contains image data.")
            
            # Check for table data
            if isinstance(hdu, (fits.BinTableHDU, fits.TableHDU)):
                print("    Contains table data.")
                table_data = Table(hdu.data)
                print("    Columns:", table_data.colnames)
            
            # Check for spectral data
            if hdu.header.get('EXTNAME', '').lower() == 'spectrum':
                print("    Contains spectral data.")
            
            # If data type is not specifically recognized
            if not isinstance(hdu, (fits.ImageHDU, fits.PrimaryHDU, fits.BinTableHDU, fits.TableHDU)):
                print("    Contains unrecognized data type.")

def process_directory(directory):
    """Step through all FITS files in a directory and summarize their contents."""
    files = [f for f in os.listdir(directory) if f.endswith('.fits')]
    files.sort()  # Sort files lexicographically
    
    for filename in files:
        file_path = os.path.join(directory, filename)
        summarize_fits_file(file_path)

if __name__ == "__main__":
    # Set the directory containing the FITS files
    fits_directory = "/mnt/nas/gaia_data/gdr1/gaia_source/fits"
    
    # Process the directory and summarize each file's contents
    process_directory(fits_directory)
