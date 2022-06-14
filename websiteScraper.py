import re
import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd

URL = 'https://genametor.com/en/list/indonesian/'
#URL = 'https://genametor.com/en/list/kazakh/'
#URL = 'https://genametor.com/en/list/polish/'

def parser_soup(URL):
    scraper = cloudscraper.create_scraper()  # returns a CloudScraper instance
    # Or: scraper = cloudscraper.CloudScraper()  # CloudScraper inherits from requests.Session
    page = scraper.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")
    return soup

def max_table(soup):
    page_link_class = soup.find_all("a", {"class": "page-link"})
    take_href = set([element['href'] for element in page_link_class])
    list_number = [href.split('/')[-2] for href in take_href if href.split('/')[-2].isdigit()]
    return int(max(list_number))

soup = parser_soup(URL)
max_page = max_table(soup)

dictionary_data = {'#':[],
                   'name':[],
                   'nation':[],
                   'sex':[]}

for page in range(1,max_page+1):
    URL_PAGE = URL+f"{page}/"
    soup = parser_soup(URL_PAGE)

    for child in soup.find_all('table')[0].children:
        for tr in child:
            string_tr = str(tr)
            data_list = re.findall('>([\w\d\s\#]+)<', string_tr)
            for key, value in zip(dictionary_data.keys(), data_list):
                dictionary_data[key].append(value)

name_dataframe = pd.DataFrame(dictionary_data)
index_drop = name_dataframe[name_dataframe["#"] == "#"].index
name_dataframe.drop(index_drop, inplace=True)
name_dataframe.to_csv('data/name_dataframe.csv', index=False)