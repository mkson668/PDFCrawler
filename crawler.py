import logging
import re
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('app.log')
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

INIT_URL = "https://www.cncbinternational.com/home/en/index.jsp"
BASE_URL = "https://www.cncbinternational.com"
REMOVAL_STRS = ["fragment", "tc/", "sc/", ".doc", ".jpg", ".png", ".apk"]
DESTINATION_DIR = "pdf_files"

url_list = [INIT_URL]
traversed_url_list = []
all_pdf_links = []

def contains_any(string, substrings):
    """Checks if any substring from a list is present in a given string.

    Args:
        string (str): The string to search in.
        substrings (list): A list of substrings to search for.

    Returns:
        bool: True if any substring is found, False otherwise.
    """
    return any(substr in string for substr in substrings)

def is_valid_url(check_url, timeout=5):
    """Checks if a given URL is valid and reachable.

    Args:
        check_url (str): The URL to validate.
        timeout (int, optional): The timeout for the request in seconds. Defaults to 5.

    Returns:
        bool: True if the URL is valid and reachable, False otherwise.

    Logs:
        Info: The status of the URL check.
    """
    try:
        response = requests.head(check_url, timeout=timeout, allow_redirects=True)
        if response.status_code < 400:
            logger.info(f"URL {check_url} is valid")
            return True
        else:
            logger.info(f"URL {check_url} could not be reached")
            return False
    except requests.exceptions.RequestException:
        logger.info(f"URL {check_url} could not be reached")
        return False

def filter_unique_destinations(urls):
    """Filters out duplicate redirect URLs from a list of URLs.

    Args:
        urls (list): A list of URLs to filter.

    Returns:
        list: A list of unique destination URLs after following redirects.

    Logs:
        Info: Any errors encountered while accessing URLs.
    """
    unique_destinations = []

    for url in urls:
        try:
            response = requests.get(url, allow_redirects=True, timeout=10)
            final_url = response.url

            # Parse the URL to remove query parameters
            parsed_url = urlparse(final_url)
            normalized_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
            if normalized_url not in unique_destinations:
                unique_destinations.append(normalized_url)
        except requests.RequestException as e:
            logger.info(f"Error accessing {url}: {e}")

    return unique_destinations

def recursive_url_fetcher(urls):
    """Recursively fetches and processes URLs to find all PDF-like links.

    This function traverses through the given URLs, extracts links from their HTML content,
    and recursively processes these links. It identifies PDF links and adds them to a global list.

    Args:
        urls (list): A list of URLs to process.

    Global Variables:
        all_pdf_links (list): Stores all found PDF links.
        traversed_url_list (list): Keeps track of all visited URLs.
        REMOVAL_STRS (list): Strings used to filter out unwanted URLs.
        BASE_URL (str): The base URL used for joining relative URLs.

    Logs:
        Info: Various stages of the URL processing.

    Note:
        This function modifies global variables and doesn't return a value directly.
        It recursively calls itself with new sets of URLs.
    """
    logger.info(len(urls))
    logger.info(len(all_pdf_links))
    updated_link_set = []

    if len(urls) <= 0:
        # base case: empty list to iterate on direct return
        return

    # base case: prune urls that have been visited before or contain certain str

    urls = set([url for url in urls if not contains_any(url.lower(), REMOVAL_STRS)])

    urls = set([url for url in urls if url not in traversed_url_list])

    for url in urls:
        # base case: list containing .pdf like files add to global and remove from all iterations going forward
        if url.lower().endswith('.pdf'):
            if url not in all_pdf_links:
                logger.info(f"pdf {url} has been added")
                all_pdf_links.append(url)

    urls = set([url for url in urls if url not in all_pdf_links])

    for curr_url in urls:
        if curr_url in traversed_url_list:
            continue
        else:
            traversed_url_list.append(curr_url)
            try:
                # get html and convert to soup
                response = requests.get(curr_url, allow_redirects=True, timeout=5)
                if response.status_code == 200:
                    try:
                        soup = BeautifulSoup(response.text, 'html.parser')
                    except AttributeError as e:
                        print(f"bs4 html parsing exception occurred: {e}")
                    # get all acnhors contain hrefs
                    anchor_list = soup.find_all("a")
                    if len(anchor_list) > 0:
                        href_list = set([anchor.get("href") for anchor in anchor_list])
                        # get the href link, append root and check if valid link before adding to updated_link_list
                        for link in href_list:
                            if link and "#" not in link:
                                full_link = urljoin(BASE_URL, link)
                                full_link = urlparse(full_link)
                                normalized_url = f"{full_link.scheme}://{full_link.netloc}{full_link.path}"
                                # short circuiting and ignore irrelavant domains without
                                # substring www.cncbinternational.com
                                if "www.cncbinternational.com" in str(normalized_url) \
                                    and str(normalized_url) not in traversed_url_list:
                                    updated_link_set.append(normalized_url)
            except requests.exceptions.HTTPError as e:
                logger.info(f"HTTP Error: {e}")
            except requests.exceptions.ConnectionError as e:
                logger.info(f"Error Connecting: {e}")
            except requests.exceptions.Timeout as e:
                logger.info(f"Timeout Error: {e}")
            except requests.exceptions.RequestException as e:
                logger.info(f"Something went wrong: {e}")

    # recursive case
    uniq_dest_url_list = filter_unique_destinations(list(set(updated_link_set)))

    recursive_url_fetcher(list(set(uniq_dest_url_list)))

recursive_url_fetcher(url_list)

pdf_df = pd.DataFrame({"pdf_link": all_pdf_links})
traversed_df = pd.DataFrame({"traversed_link": traversed_url_list})

pdf_df.to_excel("pdf_links.xlsx", index=False)
traversed_df.to_excel("traversed_links.xlsx", index=False)
