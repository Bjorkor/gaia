#!/home/bjorkor/projects/gaia/venv/bin/python
import os
from astropy.io import fits
from astropy.table import Table
import matplotlib.pyplot as plt

def process_fits_file(file_path):
    """Process a single FITS file."""
    with fits.open(file_path) as hdul:
        print(f"\nProcessing file: {file_path}")
        hdul.info()  # Display the structure of the FITS file
        
        for hdu in hdul:
            # Process image data
            if hdu.data is not None and len(hdu.data.shape) == 2:
                print("This HDU contains image data.")
                plt.imshow(hdu.data, cmap='gray')
                plt.colorbar()
                plt.title(f"Image from {file_path}")
                plt.show()

            # Process table data
            if isinstance(hdu, (fits.BinTableHDU, fits.TableHDU)):
                print("This HDU contains a table.")
                table_data = Table(hdu.data)
                print("Columns:", table_data.colnames)
                print("First 5 rows:\n", table_data[:5])

                # Example operation: filter and plot
                if 'ra' in table_data.colnames and 'dec' in table_data.colnames:
                    plt.scatter(table_data['ra'], table_data['dec'], s=1, c=table_data['phot_g_mean_mag'], cmap='viridis')
                    plt.colorbar(label='G-band Magnitude')
                    plt.xlabel('Right Ascension (degrees)')
                    plt.ylabel('Declination (degrees)')
                    plt.title(f"Sky Distribution of Sources from {file_path}")
                    plt.show()

            # Process spectral data
            if hdu.header.get('EXTNAME', '').lower() == 'spectrum' and hdu.data is not None:
                print("This HDU contains spectral data.")
                plt.plot(hdu.data)
                plt.title(f"Spectrum from {file_path}")
                plt.show()

            # If data is not recognized as image, table, or spectrum
            if hdu.data is None:
                print("This HDU contains no data.")
            else:
                print("This HDU contains data that does not match the expected formats.")

def process_directory(directory):
    """Step through all FITS files in a directory."""
    for filename in os.listdir(directory):
        if filename.endswith('.fits'):
            file_path = os.path.join(directory, filename)
            process_fits_file(file_path)

if __name__ == "__main__":
    # Set the directory containing the FITS files
    fits_directory = "/mnt/nas/gaia_data/gdr1/gaia_source/fits"
    
    # Process the directory
    process_directory(fits_directory)
