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


class Item(BaseModel):

    search: str = ''
    pages: int = 10


@app.get("/")
def root():
    return {
        "info": {
            "title": "ReutersAPI",
            "version": "2022.12.29"
        },
    }


@app.post("/reuters/")
async def get_reuters(item: Item):
    return get_reuters_(
        search=item.search, 
        pages=item.pages
    )


def get_reuters_(search='', pages=10):
    df = get_multi_pages(search=search, pages=pages)

    # Convert to JSON format
    data = defaultdict(list)
    data['length'] = len(df)
    for idx, row in df.iterrows():
        data['data'].append(dict(row))

    return data


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
    df = df[['id', 'title', 'description', 'published_time']]
    df['published_time'] = df['published_time'].apply(parse)
    df['date'] = df['published_time'].apply(lambda x: x.strftime("%Y-%m-%d"))
    df['time'] = df['published_time'].apply(lambda x: x.strftime("%H:%M:%S"))
    df = df.drop(columns=['published_time'])
    return df


def get_multi_pages(search, pages):
    df = pd.concat([get_single_page(search, page) for page in tqdm(range(pages))], axis=0)
    df = df.reset_index(drop=True)
    return df