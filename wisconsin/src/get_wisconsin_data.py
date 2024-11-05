import pandas as pd
import os
from newscatcherapi  import NewsCatcherApiClient
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
import logging

logging.basicConfig(level=logging.INFO, filename="get_wisconsin_data.log", filemode="w")



load_dotenv()
api_key = os.getenv("NEWS_CATCHER_API_KEY")




class NewscatcherScraper:

    def __init__(self, api_key=api_key, source_csv_path="../data/wisconsin_newspaper.csv", excluded_sources=['oakcreeklibrary.org','loc.gov']):
        self.api = NewsCatcherApiClient(api_key)
        self.source_table = pd.read_csv(source_csv_path)
        self.source_table.drop_duplicates(subset=['website_core'], inplace=True)
        self.source_table = self.source_table[self.source_table["domain"].isna()].reset_index(drop=True)
        self.source_list=self.source_table.website_core.dropna().tolist()
        self.source_list = [source for source in self.source_list if source not in excluded_sources]
        print("Number of sources: ", len(self.source_list))


    def get_response(self, query, from_: datetime, to_: datetime, page_size=1000):
        from_ = from_.strftime("%Y/%m/%d %H:%M:%S")
        to_ = to_.strftime("%Y/%m/%d %H:%M:%S")
        resp = self.api.get_search_all_pages(q=query, from_=from_, to_=to_, page_size=page_size, sources=self.source_list)
        return resp
    

    def save_response(self, response, filename):
        with open(filename, 'w') as f:
            json.dump(response, f)

    @staticmethod
    def daterange(start_date, end_date):
        start_date = datetime.combine(start_date, datetime.min.time())
        end_date = datetime.combine(end_date, datetime.min.time())
        for n in range(0, int((end_date - start_date).days) + 1, 7):
            if n == 0:
                continue
            yield start_date + timedelta(n-6), start_date.replace(hour=23, minute=59, second=59) + timedelta(n)

    def main(self, query, from_date, to_date, filename):
        for from_date, to_date in self.daterange(from_date, to_date):
            print(from_date, to_date)
            response = self.get_response(query, from_date, to_date)
            if len(response["articles"]) >= 10000:
                logging.warning(f"Number of articles for {from_date} to {to_date} exceeds 10000")
            _filename = filename + "_" + from_date.strftime("%Y-%m-%d") + ".json"
            self.save_response(response, _filename)
            time.sleep(1)


if __name__ == "__main__":

    scraper = NewscatcherScraper()
    scraper.main("*", datetime(2020, 1, 1), datetime(2024, 10, 1), "../data/output/wisconsin")