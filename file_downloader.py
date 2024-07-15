import re
import requests

def download_file(url, filename):
    """
    Downloads a file from the given URL to the specified filename.

    Args:
        url (str): The URL of the file to download.
        filename (str): The path where the file will be saved.

    Returns:
        bool: True if the download was successful, False otherwise.

    Raises:
        requests.RequestException: If there's an error during the download process.
    """
    try:
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()
        
        # Open the file in binary write mode
        with open(filename, 'wb') as file:
            # Iterate over the response data in chunks
            for chunk in response.iter_content(chunk_size=8192):
                # Write each chunk to the file
                file.write(chunk)
        
        print(f"File downloaded successfully: {filename}")
        return True
    except requests.RequestException as e:
        print(f"Error downloading file: {e}")
        return False

def validate_pdf_filename(filename):
    """
    Validates if the given filename is correctly formatted for a PDF file.

    Args:
        filename (str): The filename to validate.

    Returns:
        bool: True if the filename is valid, False otherwise.
    """
    pattern = r'^[a-zA-Z0-9_-]+\.pdf$'
    if re.match(pattern, filename):
        return True
    else:
        return False

def download_all_files(file_url_arr, dest):
    """
    Downloads all files from the given array of URLs to the specified destination.

    Args:
        file_url_arr (list): A list of URLs pointing to files to be downloaded.
        dest (str): The destination directory where files will be saved.

    Returns:
        list: A list of dictionaries containing information about each download attempt.
                Each dictionary includes:
                - 'original_url': The original URL of the file.
                - 'file_name': The name of the file (either original or generated).
                - 'dl_status': A boolean indicating if the download was successful.
    """
    f_name_cnt = 0
    result_json = []
    for file_url in file_url_arr:
        entry = {}
        f_name = file_url.split("/")[-1]
        if not validate_pdf_filename(f_name):
            f_name = f"tmp_fn_{f_name_cnt}.pdf"
            f_name_cnt += 1
        entry = {
                "original_url": file_url,
                "file_name": f_name,
            }
        f_name = f"{dest}/{f_name}"
        if download_file(file_url, f_name):
            entry["dl_status"] = True
        else:
            entry["dl_status"] = False
        result_json.append(entry)
