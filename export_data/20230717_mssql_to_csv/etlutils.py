import os

creds = {
    'username': '',
    'password': '',
    'server': '',
    'database': '',
}

import subprocess
import gzip
import shutil
import subprocess
import os
from zipfile import ZipFile

def bcp_to_file(query, connection_name, filepath, delimiter='|', linebreak=os.linesep, header=False):
    subprocess.run([
        "/opt/mssql-tools/bin/bcp", 
        query, 
        "queryout", 
        filepath, 
        "-c", 
        "-r" + linebreak, 
        "-t" + delimiter, 
        "-U", creds['username'], 
        "-P", creds['password'], 
        "-S", creds['server'], 
        "-d", creds['database']
    ], stderr=subprocess.PIPE)
    file_size = os.path.getsize(filepath)
    print(f"File Size: {file_size} bytes")
    if os.path.isfile(filepath) is False:
        raise Exception(f'File not found: {filepath}')
    # print(f'Successfully exported file {filepath} from {connection_name}')

import gzip
import shutil
import subprocess
import os
from zipfile import ZipFile

# This function compresses a file using GZIP compression.
# It takes the filepath of the file as input.
def gzip_file(filepath):
    with open(filepath, 'rb') as f_in: # Open the file in binary read mode
        with gzip.open(filepath + '.gz', 'wb') as f_out: # Open a GZIP file in binary write mode with the same filepath plus '.gz' extension
            shutil.copyfileobj(f_in, f_out) # Copy the contents of the input file to the GZIP file
    os.remove(filepath) # Remove the original file
    print(f'Successfully zipped {filepath}.') 

# This function unzips a file compressed with GZIP.
# It takes the filepath of the compressed file and an optional flag to remove the original file.
def unzip_gzip_file(filepath, remove_file=True):
    with gzip.open(filepath, 'rb') as f_in: # Open the GZIP file in binary read mode
        with open(filepath[:-3], 'wb') as f_out: # Open a new file in binary write mode with the original filename (without '.gz' extension)
            shutil.copyfileobj(f_in, f_out) # Copy the contents of the GZIP file to the new file
    if remove_file: # Remove the original file if specified
        os.remove(filepath)
    print(f'Successfully unzipped {filepath}.')    

# This function compresses a file using the external 'pigz' command-line tool.
# It takes the filepath of the file as input.
def pigz_file(filepath):
    # Execute the 'pigz' command with the filepath as an argument, capturing the output
    proc = subprocess.Popen(f'pigz {filepath}'.split(maxsplit=1), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if err != b'': # If there is any error output, raise a ValueError with the error message
        raise ValueError(f'The following error occurred: {err}')
    print(f'Successfully zipped {filepath}.') 

# This function extracts files from a ZIP file.
# It takes the directory path of the ZIP file and the filename to extract as input.
def unzip_zip_file(filepath, filename):
    file_list = [] # Create an empty list to store the extracted file names
    with ZipFile(f'{filepath}/{filename}') as zf: # Open the ZIP file
        for zip_info in zf.infolist(): # Iterate over the files in the ZIP file
            if zip_info.filename[-1] == '/': # Skip extracting subdirectories, only extract files
                continue
            zip_info.filename = os.path.basename(zip_info.filename) # Set the filename to the basename (remove any leading directories)
            zf.extract(zip_info, filepath) # Extract the file to the specified directory
            file_list.append(zip_info.filename) # Add the extracted filename to the list
    return file_list # Return the list of extracted file names








# def gzip_file(filepath, pgp_key=None):
#     with open(filepath, 'rb') as f_in:
#         with gzip.open(filepath + '.gz', 'wb') as f_out:
#             shutil.copyfileobj(f_in, f_out)
#     os.remove(filepath)
#     print(f'Successfully zipped {filepath}.')

# def unzip_gzip_file(filepath, remove_file=True):
#     with gzip.open(filepath, 'rb') as f_in:
#         with open(filepath[:-3], 'wb') as f_out:
#             shutil.copyfileobj(f_in, f_out)
#     if remove_file:
#         os.remove(filepath)
#     print(f'Successfully zipped {filepath}.')    

# def pigz_file(filepath):
#     proc = subprocess.Popen(f'pigz {filepath}'.split(maxsplit=1), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#     out, err = proc.communicate()
#     if err != b'':
#         raise ValueError(f'The following error occured {err}')
#     logging.info(f'Successfully zipped {filepath}.') 

# def unzip_zip_file(filepath, filename):
#     file_list = []
#     with ZipFile(f'{filepath}/{filename}') as zf:
#         for zip_info in zf.infolist():
#             #The below if statement handles subdirectories. If there is a subdirectory identified
#             #this skips extracting that folder. The script will work its way into the
#             #subdirectory and extract any files inside.
#             if zip_info.filename[-1] == '/':
#                 continue
#             zip_info.filename = os.path.basename(zip_info.filename)
#             zf.extract(zip_info, filepath)
#             file_list.append(zip_info.filename)
#     return file_list
