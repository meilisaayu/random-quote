import requests
import json
from datetime import datetime as dt
import pandas as pd
from google.cloud import bigquery

url = 'https://api.quotable.io/quotes/random'

quote_data = requests.get(url).text
quote_dict = json.loads(quote_data)
result_dict = {}
for d in quote_dict:
    result_dict.update(d)

quote_id = result_dict['_id']
quote_content = result_dict['content']
quote_author = result_dict['author']
quote_tags = result_dict['tags']
quote_datetime_extracted = dt.now()

quote_df = pd.DataFrame(columns=['_id', 'content', 'author', 'tags', 'datetime_extracted'])
quote_df.loc[len(quote_df)] = [quote_id, quote_content, quote_author, quote_tags, quote_datetime_extracted]

client = bigquery.Client()

# Creating BQ table reference:
dataset_ref = bigquery.DatasetReference('euphoric-coast-400712', 'random_quote')
table_ref = dataset_ref.table('quotes')

job = client.load_table_from_dataframe(quote_df, table_ref)
job.result()
