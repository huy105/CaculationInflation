from multiprocessing import Process, Queue
from crawl_food import CrawlFood
from typing import List
import pandas as pd
import datetime
import time

# Hàm save to csv
def save_to_csv(data: pd.DataFrame, dir_path: str):
    today = datetime.datetime.now()
    # Lấy ngày, tháng và năm
    day = str(today.day)
    mon = str(today.month)
    year = str(today.year)
    name = f'{day}-{mon}-{year}-raw_data.csv'
    name = dir_path + name

    data.to_csv(name, index=False)

#  Hàm worker
def bot_crawl(q_task: Queue, q_result: Queue, col: List):
    bot = CrawlFood()
    data = pd.DataFrame(columns=col)

    while not q_task.empty():
        url, category  = q_task.get()
        bot.get_link(url)
        data = bot.get_product(data, category)
        q_result.put(data)
    

# Hàm triển khai multi process
def execute_process(urls: List, category: List, col: List, worker: int = 4):
    q_task = Queue()
    q_result = Queue()

    tasks = zip(urls, category)
    for task in tasks:
        q_task.put(task)

    processes = [Process(target=bot_crawl, args=(q_task, q_result, col)) for _ in range(worker)]
    for process in processes:
        process.start()
    for process in processes:
        process.join()
    
    # lấy tất cả kết quả ra và đưa vào results
    results = []
    while not q_result.empty():
        results.append(q_result.get())

    return results

#  Hàm thực hiện chính
def pre_process(list_data: List, category: List, worker: int):
    # list_data = ['https://winmart.vn/thit--c0111?storeCode=1535', 'https://winmart.vn/rau-la--c01167?storeCode=1535']
    # category = ['Meat', 'Vegetables']
    col = ['sku', 'category', 'name', 'price']
    data = execute_process(list_data, category, col, worker)
    df = pd.DataFrame(columns=col)

    for data in data:   
        df = pd.concat([df, data], axis=0)
    
    df.reset_index(inplace= True)
    df = df.drop(axis = 1, columns= ['index'])

    path = 'E:/Data/AI_Files/Spark/crawl_food/raw_data/'
    save_to_csv(df, path)
    json_data = df.to_json()

    return json_data