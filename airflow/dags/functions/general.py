import os
import zipfile

def zip_files(base_folder, file_paths, output_filename):
    with zipfile.ZipFile(output_filename, 'w') as f:
        for file_path in file_paths:
            f.write(file_path, os.path.relpath(file_path, base_folder))

