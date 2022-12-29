import os
import logging
import requests
import pandas as pd
from tqdm.auto import tqdm
from fastapi import FastAPI
from pydantic import BaseModel
from dateutil.parser import parse
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info(os.environ["PATH"])
app = FastAPI()


class SearchItem(BaseModel):

    search: str = ''
    pages: int = 1


class SectionItem(BaseModel):

    section: str = 'business'
    pages: int = 1


@app.get("/")
def root():
    return {
        "info": {
            "title": "ReutersAPI",
            "version": "2022.12.29"
        },
    }


@app.get("/list-sections")
def list_sections():
    return {
        "info": {
            "sections": [
                'world', 
                'business', 
            ]
        },
    }


@app.post("/reuters-by-search/")
async def get_reuters_by_search(item: SearchItem):
    return get_reuters_by_search_(
        search=item.search, 
        pages=item.pages
    )


@app.post("/reuters-by-section/")
async def get_reuters_by_section(item: SectionItem):
    return get_reuters_by_section_(
        section=item.section, 
        pages=item.pages
    )


def get_reuters_by_search_(search='', pages=10):

    def get_single_page(search, page):
        offset = str(page * 20)
        url = 'https://www.reuters.com/pf/api/v3/content/fetch/articles-by-search-v2'
        headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36', 
            'content-type': 'application/json; charset=utf-8'
        }
        params = {
            'query': '{"keyword":"' + search + '","offset":' + offset + ',"orderby":"display_date:desc","size":20,"website":"reuters"}', 
            'd': 124, 
            '_website': 'reuters'

        }
        res = requests.get(url, headers=headers, params=params)
        articles = res.json()['result']['articles']
        df = pd.DataFrame(articles)
        df = df[['id', 'title', 'description', 'published_time', 'canonical_url']]
        df['published_time'] = df['published_time'].apply(parse)
        df['date'] = df['published_time'].apply(lambda x: x.strftime("%Y-%m-%d"))
        df['time'] = df['published_time'].apply(lambda x: x.strftime("%H:%M:%S"))
        df['link'] = df['canonical_url'].apply(lambda x: 'https://www.reuters.com' + x)
        df = df.drop(columns=['published_time', 'canonical_url'])
        return df


    def get_multi_pages(search, pages):
        df = pd.concat([get_single_page(search, page) for page in tqdm(range(pages))], axis=0)
        df = df.drop_duplicates(subset=['id'])
        df = df.sort_values(by=['date', 'time'], ascending=False)
        df = df.reset_index(drop=True)
        return df

    df = get_multi_pages(search=search, pages=pages)

    # Convert to JSON format
    data = defaultdict(list)
    data['length'] = len(df)
    for idx, row in df.iterrows():
        data['data'].append(dict(row))

    return data


def get_reuters_by_section_(section, pages):

    def get_single_page(section, page):
        offset = str(page * 20)
        url = 'https://www.reuters.com/pf/api/v3/content/fetch/articles-by-section-alias-or-id-v1'
        headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36', 
            'content-type': 'application/json; charset=utf-8'
        }
        query = f'"arc-site":"reuters","called_from_a_component":true,"fetch_type":"sophi","offset":{offset},"section_id":"/{section}/","size":20,"sophi_page":"*","sophi_widget":"topic","uri":"/{section}/","website":"reuters"'
        params = {
            'query': '{' + query + '}', 
            'd': 124, 
            '_website': 'reuters'

        }
        res = requests.get(url, headers=headers, params=params)
        articles = res.json()['result']['articles']
        df = pd.DataFrame(articles)
        df = df[['id', 'title', 'description', 'published_time', 'canonical_url']]
        df['published_time'] = df['published_time'].apply(parse)
        df['date'] = df['published_time'].apply(lambda x: x.strftime("%Y-%m-%d"))
        df['time'] = df['published_time'].apply(lambda x: x.strftime("%H:%M:%S"))
        df['link'] = df['canonical_url'].apply(lambda x: 'https://www.reuters.com' + x)
        df = df.drop(columns=['published_time', 'canonical_url'])
        return df

    def get_multi_pages(section, pages):
        df = pd.concat([get_single_page(section, page) for page in tqdm(range(pages))], axis=0)
        df = df.drop_duplicates(subset=['id'])
        df = df.sort_values(by=['date', 'time'], ascending=False)
        df = df.reset_index(drop=True)
        return df

    df = get_multi_pages(section=section, pages=pages)

    # Convert to JSON format
    data = defaultdict(list)
    data['length'] = len(df)
    for idx, row in df.iterrows():
        data['data'].append(dict(row))

    return data