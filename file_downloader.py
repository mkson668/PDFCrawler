import os
import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class FileDownloader:
    def __init__(self, destination, logger):
        """
        Initializes the FileDownloader with a destination directory.

        Args:
            destination (str): The destination directory where files will be saved.
        """
        self.destination = destination
        self.logger = logger

    def requests_retry_session(
        self,
        retries=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 503, 504),
        session=None,
    ):
        session = session or requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def download_file(self, url, filename):
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
            session = self.requests_retry_session()
            response = session.get(url, stream=True, timeout=10)
            response.raise_for_status()

            # Open the file in binary write mode
            with open(filename, "wb") as file:
                # Iterate over the response data in chunks
                for chunk in response.iter_content(chunk_size=(1024**2) * 10):
                    # Write each chunk to the file
                    file.write(chunk)

            # Check if the file exists and has content
            if os.path.exists(filename) and os.path.getsize(filename) > 0:
                self.logger.info(f"File downloaded successfully: {filename}")
                return True
            else:
                self.logger.info(f"File download failed or file is empty: {filename}")
                return False

        except requests.RequestException as e:
            self.logger.info(f"Error downloading file: {e}")
            return False
        except IOError as e:
            self.logger.info(f"Error writing file: {e}")
            return False

    def validate_pdf_filename(self, filename):
        """
        Validates if the given filename is correctly formatted for a PDF file.

        Args:
            filename (str): The filename to validate.

        Returns:
            bool: True if the filename is valid, False otherwise.
        """
        pattern = r"^[a-zA-Z0-9_-]+\.pdf$"
        return bool(re.match(pattern, filename))

    def download_all_files(self, file_url_arr):
        """
        Downloads all files from the given array of URLs to the specified destination.

        Args:
            file_url_arr (list): A list of URLs pointing to files to be downloaded.

        Returns:
            list: A list of dictionaries containing information about each download attempt.
                    Each dictionary includes:
                    - 'original_url': The original URL of the file.
                    - 'file_name': The name of the file (either original or generated).
                    - 'dl_status': A boolean indicating if the download was successful.
        """
        f_name_cnt = 0
        result_json = []
        for file_idx, file_url in enumerate(file_url_arr):
            self.logger.info(f"running {file_url}")
            self.logger.info(file_idx)
            entry = {}
            f_name = file_url.split("/")[-1]

            if os.path.exists(f"pdf_files/{f_name}"):
                # edge case; only run set on full URL path fails to catch duplicate pdf_file names
                # we will assume that the files are identical as they have the same name
                continue

            if not self.validate_pdf_filename(f_name):
                f_name = f"tmp_fn_{f_name_cnt}.pdf"
                f_name_cnt += 1
            entry = {
                "original_url": file_url,
                "file_name": f_name,
            }
            full_path = f"{self.destination}/{f_name}"
            if self.download_file(file_url, full_path):
                entry["dl_status"] = True
            else:
                entry["dl_status"] = False
            result_json.append(entry)
            self.logger.info(f"completed {file_url}")
        return result_json
