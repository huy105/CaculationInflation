from typing import List
from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
from multiprocess_instance import pre_process   

app = FastAPI()

class Tasks(BaseModel):
    urls: List
    category: List
    worker: int

@app.get('/')
async def test_get():
    return 'Hello world'

@app.post('/crawl_product/')
async def crawl_product(tasks: Tasks):
    tasks = tasks.model_dump()

    urls = []
    category = []
    for url in tasks['urls']:
        urls.append(url)
    for cate in tasks['category']:
        category.append(cate)

    data = pre_process(urls, category, tasks['worker'])

    return data