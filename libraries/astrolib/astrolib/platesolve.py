# Astro Azure Python utility library
#
import uuid
import requests
from astropy.io import fits
import numpy as np

def qtable_to_fits_bintable(qtable, width, height, output_filename, overwrite=True):
    """
    Convert an astropy QTable to a FITS BINTABLE file.
    
    Parameters:
    -----------
    qtable : astropy.table.QTable
        The input QTable to convert
    output_filename : str
        Path to the output FITS file
    overwrite : bool, default=True
        Whether to overwrite existing files
    """
    
    # Create a list to store column definitions
    columns = []
    
    # Process each column in the QTable
    for col_name in qtable.colnames:
        col = qtable[col_name]
        
        # Handle units - store them in the TUNIT header keyword
        unit_str = None
        if hasattr(col, 'unit') and col.unit is not None:
            unit_str = str(col.unit)
        
        # Get the data, converting quantities to values if needed
        if hasattr(col, 'value'):
            # This is a Quantity column
            data = col.value
        else:
            # Regular column
            data = col.data
        
        # Determine the appropriate FITS format
        if data.dtype.kind in ['U', 'S']:  # String data
            # For string columns, we need to specify the maximum string length
            max_len = max(len(str(item)) for item in data) if len(data) > 0 else 1
            format_str = f'{max_len}A'
        elif data.dtype.kind == 'f':  # Float
            if data.dtype == np.float32:
                format_str = 'E'
            else:
                format_str = 'D'
        elif data.dtype.kind in ['i', 'u']:  # Integer
            if data.dtype in [np.int16, np.uint16]:
                format_str = 'I'
            elif data.dtype in [np.int32, np.uint32]:
                format_str = 'J'
            elif data.dtype in [np.int64, np.uint64]:
                format_str = 'K'
            else:
                format_str = 'J'  # Default to 32-bit integer
        elif data.dtype.kind == 'b':  # Boolean
            format_str = 'L'
        else:
            # Default to double precision
            format_str = 'D'
        
        # Create the column definition
        col_def = fits.Column(
            name=col_name,
            format=format_str,
            array=data,
            unit=unit_str
        )
        
        columns.append(col_def)
    
    # Create the binary table HDU
    table_hdu = fits.BinTableHDU.from_columns(columns)
    table_hdu.name = 'SOURCES'  # Set a name for the HDU
    table_hdu.header['IMAGEH'] = height
    table_hdu.header['IMAGEW'] = width

    # Copy table metadata to header if present
    if hasattr(qtable, 'meta') and qtable.meta:
        for key, value in qtable.meta.items():
            # FITS header keys must be <= 8 characters
            fits_key = str(key)[:8].upper()
            try:
                table_hdu.header[fits_key] = value
            except (ValueError, TypeError):
                # Skip problematic metadata entries
                print(f"Warning: Could not add metadata key '{key}' to FITS header")
    
    # Create primary HDU (required for FITS files)
    primary_hdu = fits.PrimaryHDU()
    primary_hdu.header['NEXTEND'] = 1  # 1 extention

    # Create HDU list and write to file
    hdul = fits.HDUList([primary_hdu, table_hdu])
    output_filename = f'/tmp/{output_filename}'
    hdul.writeto(output_filename, overwrite=overwrite)
    
    return output_filename

# Function to do a plate solve using my API

def solve_from_source_list_api(url, qtable, image_width, image_height, scale_low, scale_high):

    xyls = f'{uuid.uuid4()}.fit'

    qtable_to_fits_bintable(qtable[:100], image_width, image_height, xyls, overwrite=True)

    # Not sure if the xcentroid/ycentroid need to change to x_centroid/y_centroid, but I will leave them as is for now.  The API should be able to handle it.
    payload = {'xcolumn': 'xcentroid',
        'ycolumn': 'ycentroid',
        'scale_low': scale_low,
        'scale_high': scale_high}
    files=[
        ('file',(xyls,open(f'/tmp/{xyls}','rb'),'application/octet-stream'))
    ]
    headers = {}

    response = requests.request("POST", url + "/solve", headers=headers, data=payload, files=files)
    job_id = response.json()['job_id']
    wcs_file = response.json()['output_files'][0]

    payload = {}
    headers = {}

    response = requests.request("GET", url + f'/results/{job_id}/{wcs_file}', headers=headers, data=payload)

    wcs = response.content
    hdu = fits.PrimaryHDU.fromstring(wcs, ignore_missing_end=True)
    return hdu