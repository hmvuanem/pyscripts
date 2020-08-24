import numpy as np
import pandas as pd
import pyodbc
from datetime import date, timedelta
import matplotlib.pyplot as plt

import os
import logging

from google.oauth2 import service_account
import pandas_gbq
from google.cloud import bigquery

"""# Query"""

cnxn = pyodbc.connect('DSN=NetSuiteML;uid=minh.le@vuanem.com;PWD=BI@2023cute')

yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
yesterday

query = f'''
SELECT
    main.CREATE_DATE,
    TRANSACTION_LINES.LOCATION_ID,
    TRANSACTION_LINES.ITEM_ID,
    TRANSACTION_LINES.ITEM_COUNT,
    TRANSACTION_LINES.NET_AMOUNT
  FROM
    "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".TRANSACTION_LINES
    LEFT JOIN "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".TRANSACTIONS AS main ON TRANSACTION_LINES.TRANSACTION_ID = main.TRANSACTION_ID
    LEFT JOIN "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".TRANSACTIONS AS sources ON sources.TRANSACTION_ID = main.CREATED_FROM_ID
    LEFT JOIN "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".INVENTORY_ITEMS ON TRANSACTION_LINES.ITEM_ID = INVENTORY_ITEMS.ITEM_ID
  WHERE
    TRANSACTION_LINES.ACCOUNT_ID IN (365, 366, 367, 370, 371)
    AND TRANSACTIONS.CREATE_DATE < '{yesterday}'
    AND TRANSACTIONS.TRANSACTION_TYPE IN (
      'Credit Memo',
      'Invoice',
      'Item Fulfillment',
      'Inventory Adjustment',
      'Item Receipt'
    )
'''

query_results = pd.read_sql(query, cnxn)

query_results['CREATE_DATE'] = query_results['CREATE_DATE'] + pd.Timedelta('7 hours')

#query_results.rename
query_results['CREATE_DATE'] = pd.to_datetime(query_results['CREATE_DATE'].dt.date)
query_results = query_results.fillna(0)
query_results[['LOCATION_ID', 'ITEM_ID']] = query_results[['LOCATION_ID', 'ITEM_ID']].astype(int)
query_results = query_results.sort_values(['CREATE_DATE']).reset_index(drop=True)
query_results = query_results.groupby(['CREATE_DATE', 'LOCATION_ID', 'ITEM_ID']).sum().reset_index()

"""# Snapshot"""

snapshot = query_results.copy()
snapshot[['NET_AMOUNT', 'ITEM_COUNT']] = snapshot.groupby(['LOCATION_ID', 'ITEM_ID'])[['NET_AMOUNT', 'ITEM_COUNT']].cumsum()

snapshot['shift'] = snapshot.groupby(['LOCATION_ID', 'ITEM_ID'])['CREATE_DATE'].shift(1)
snapshot['date_diff'] = (snapshot['CREATE_DATE'] - snapshot['shift']).dt.days
snapshot['som'] = pd.to_datetime(snapshot['CREATE_DATE'].apply(lambda x:x.replace(day=1).date()))
snapshot['som_date_diff'] = (snapshot['CREATE_DATE'] - snapshot['som']).dt.days

snapshot[['NET_AMOUNT', 'ITEM_COUNT']] = snapshot[['NET_AMOUNT', 'ITEM_COUNT']].astype('int64')

snapshot.columns = map(str.lower, snapshot.columns)

date_cols = ['create_date', 'shift', 'som']
for col in date_cols:
  snapshot[col] = snapshot[col].dt.date


"""# BigQuery API"""

client = bigquery.Client.from_service_account_json(r'voltaic-country-280607-ea3eb5348029.json')

table_id = 'NetSuite.InventoryValuation'
job_config = bigquery.LoadJobConfig(
    schema=[
      bigquery.SchemaField("create_date", "DATE"),
      bigquery.SchemaField('location_id', "INTEGER"),
      bigquery.SchemaField('item_id', "INTEGER"),
      bigquery.SchemaField("shift", "DATE"),
      bigquery.SchemaField('date_diff', "FLOAT"),
      bigquery.SchemaField("som", "DATE"),
      bigquery.SchemaField('som_date_diff', "INTEGER")],
    write_disposition = 'WRITE_TRUNCATE',
    parquet_compression='snappy',
    source_format= 'PARQUET',
    time_partitioning = bigquery.TimePartitioning(
        type_ = bigquery.TimePartitioningType.DAY,
        field = "create_date")
    )

job = client.load_table_from_dataframe(
    snapshot, table_id, job_config=job_config)

job.result()

