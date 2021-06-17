from multiprocessing import Pool, Queue
import pandas as pd
from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
import sys

sys.setrecursionlimit(50000)

HEADERS = {
        'authority': 'olx.com.br',
        'method': 'GET',
        'scheme': 'https',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
    }


def extract_car_info(link):
    response = requests.get(url=link, headers=HEADERS)
    soup = BeautifulSoup(response.content, 'lxml')

    items = soup.find_all('div', {'class': 'sc-hmzhuo eNZSNe sc-jTzLTM iwtnNi'})

    car = {}
    # Caracteristics
    for item in items:
        title = item.find_all('span', {'class': 'sc-ifAKCX dCObfG'})[0].contents[0]
        try:
            value = item.find_all('span', {'class': 'sc-ifAKCX cmFKIN'})[0].contents[0]
        except:
            value = item.find_all('a', {'class': 'sc-57pm5w-0 XtcoW'})[0].contents[0]

        car[title] = value

    locations = soup.find_all('div', {'class': 'sc-hmzhuo sc-1f2ug0x-3 ONRJp sc-jTzLTM iwtnNi'})

    # Location
    for location in locations:
        title = location.find_all('dt', {'class': 'sc-1f2ug0x-0 cLGFbW sc-ifAKCX cmFKIN'})[0].contents[0]
        try:
            value = location.find_all('dd', {'class': 'sc-1f2ug0x-1 ljYeKO sc-ifAKCX kaNiaQ'})[0].contents[0]
        except:
            value = None

        car[title] = value

    car['url'] = link.strip()
    return car

def main():
    links_file = 'links.txt'

    urls = []
    with open(links_file, 'r') as infile:
        urls = infile.readlines()

    urls = urls[200000:]
    
    pool = Pool(10)
    results = []
    count = 39
    for result in tqdm(pool.imap_unordered(extract_car_info, urls), total=len(urls)):
        if len(results) > 5000:
            df = pd.DataFrame(results)
            df.to_csv(f'olx_cars_{count}.csv')
            count += 1
            
            results.clear()
            del df
        
        results.append(result)

    df = pd.DataFrame(results)
    df.to_csv(f'olx_cars_{count}.csv')

    pool.terminate()
    pool.join()


if __name__ == '__main__':
    main()