#!/home/bjorkor/projects/gaia/venv/bin/python
import os
from astropy.io import fits
from astropy.table import Table, vstack
import matplotlib.pyplot as plt

def process_fits_file(file_path):
    """Process a single FITS file and return filtered data."""
    with fits.open(file_path) as hdul:
        print(f"\nProcessing file: {file_path}")
        
        for hdu in hdul:
            if isinstance(hdu, (fits.BinTableHDU, fits.TableHDU)):
                table_data = Table(hdu.data)

                # Filter for stars within 100 parsecs
                if 'parallax' in table_data.colnames and 'ra' in table_data.colnames and 'dec' in table_data.colnames:
                    nearby_stars = table_data[table_data['parallax'] > 1]
                    return nearby_stars
                else:
                    print(f"Necessary data not found in {file_path}.")
                    return None
    return None

def process_directory(directory, max_files=10):
    """Process a directory and combine data from multiple FITS files."""
    combined_data = None
    file_count = 0
    
    for filename in os.listdir(directory):
        if filename.endswith('.fits') and file_count < max_files:
            file_path = os.path.join(directory, filename)
            data = process_fits_file(file_path)
            
            if data is not None:
                if combined_data is None:
                    combined_data = data
                else:
                    combined_data = vstack([combined_data, data])
                
                file_count += 1

    return combined_data

if __name__ == "__main__":
    # Set the directory containing the FITS files
    fits_directory = "/mnt/nas/gaia_data/gdr1/gaia_source/fits"
    
    # Process the directory and combine data from up to 10 files
    combined_data = process_directory(fits_directory, max_files=10)
    
    if combined_data is not None:
        # Plot the combined data
        plt.figure(figsize=(10, 6))
        plt.scatter(combined_data['ra'], combined_data['dec'], s=1, c=combined_data['phot_g_mean_mag'], cmap='viridis')
        plt.colorbar(label='G-band Magnitude')
        plt.xlabel('Right Ascension (degrees)')
        plt.ylabel('Declination (degrees)')
        plt.title('Nearby Stars within 100 parsecs from Multiple Files')
        plt.show()
    else:
        print("No data to plot.")
