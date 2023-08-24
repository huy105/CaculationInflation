from typing import List
from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
from ELT import ETLSpark
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from dateutil.relativedelta import relativedelta
import requests
import configparser

app = FastAPI()


config = configparser.ConfigParser()
config.read('config.ini')

class Tasks(BaseModel):
    days: int


def process_schema(column_names):

    schema = StructType([
        StructField(column_names[0], StringType(), True),
        StructField(column_names[1], StringType(), True),
        StructField(column_names[2], StringType(), True),
        StructField(column_names[3], FloatType(), True), 
    ])

    return schema

def get_data(spark_obj, schema: StructType, the_pass: int = 1):
    path = 'E:/Data/AI_Files/Spark/crawl_food/raw_data/'
    today = datetime.now()
    day_now = today.day
    mon_now = today.month
    year_now = today.year
    data_now = spark_obj.read.csv(path + f'{day_now}-{mon_now}-{year_now}-raw_data.csv', header= True, schema = schema)

    previous = today - relativedelta(day = the_pass)
    pre_day = previous.day
    pre_mon = previous.month
    pre_year = previous.year
    data_old = spark_obj.read.csv(path + f'{pre_day}-{pre_mon}-{pre_year}-raw_data.csv', header= True, schema = schema)

    return data_old, data_now

@app.post('/caculate_cpi/')
async def caculate_cpi(tasks: Tasks):
    tasks = tasks.model_dump()
    day_cal = tasks['days']

    spark = ETLSpark('Caculate CPI')
    spark.set_config()
    spark_obj = spark.return_spark_obj()

    column_names = ['sku', 'category', 'name', 'price']
    schema = process_schema(column_names)

    data_old, data_now = get_data(spark_obj, schema)

    inflation_rate = spark.spark_tranform_cpi(data_old, data_now)

    return inflation_rate