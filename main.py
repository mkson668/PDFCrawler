import logging
import json
import pandas as pd
from crawler import RecursiveUrlFetcher
from file_downloader import FileDownloader

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler("app.log")
    file_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    INIT_URL = "https://www.cncbinternational.com/home/en/index.jsp"
    BASE_URL = "https://www.cncbinternational.com"
    REMOVAL_STRS = ["fragment", "tc/", "sc/", ".doc", ".jpg", ".png", ".apk"]
    DESTINATION_DIR = "pdf_files"

    fetcher = RecursiveUrlFetcher(
        INIT_URL, BASE_URL, REMOVAL_STRS, DESTINATION_DIR, logger
    )
    fetcher.fetch_all_urls()
    fetcher.save_results()

    pdf_df = pd.read_excel("pdf_links.xlsx")

    print(f"Total PDF links found: {len(pdf_df)}")

    downloader = FileDownloader(DESTINATION_DIR, logger)

    results = downloader.download_all_files(pdf_df["pdf_link"])

    DL_FNAME = "result_dl.json"

    try:
        with open(DL_FNAME, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
        print(f"Data successfully saved to {DL_FNAME}")
    except IOError as e:
        print(f"Error saving data to JSON: {e}")

    with open("result_dl.json", "r", encoding="utf-8") as result:
        result_json = json.load(result)

    result_df = pd.read_json("result_dl.json", orient="records")
    result_df = result_df[result_df["dl_status"]]
    result_df = result_df.rename(
        columns={"original_url": "link", "file_name": "filename"}
    )
    result_df["file_display_name"] = result_df["filename"]
    result_df["last_updated"] = "15-07-2024"
    result_df = result_df.drop(columns=["dl_status"])
    result_dict_arr = result_df.to_dict(orient="records")

    with open("danswer_metadata.json", "w", encoding="utf-8") as f_handle:
        json.dump(result_dict_arr, f_handle)
