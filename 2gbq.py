import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import pyodbc
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime
import logging
import requests
import time
from sua_dau_so import change_phone

#create GCP Client
client = bigquery.Client.from_service_account_json('voltaic-country-280607-ea3eb5348029.json')

#All C2 Google Sheets Data Source
C2M_query = 'https://docs.google.com/spreadsheets/d/13OpfBdAgoiZRERF4SW9zQlopv-_Io3eiLHKylf_y08o/export?format=csv&id=13OpfBdAgoiZRERF4SW9zQlopv-_Io3eiLHKylf_y08o&gid=0'
C2L_FB_Ads_query = 'https://docs.google.com/spreadsheets/d/1NMAr8S_F92KCN5gl1LJ9I11sIYAyzbzfr2b7oRK7Q_o/export?format=csv&id=1NMAr8S_F92KCN5gl1LJ9I11sIYAyzbzfr2b7oRK7Q_o&gid=734520631'
C2L_FB_3rd1_query = 'https://docs.google.com/spreadsheets/d/1NMAr8S_F92KCN5gl1LJ9I11sIYAyzbzfr2b7oRK7Q_o/export?format=csv&id=1NMAr8S_F92KCN5gl1LJ9I11sIYAyzbzfr2b7oRK7Q_o&gid=0'
C2L_FB_3rd2_query = 'https://docs.google.com/spreadsheets/d/1NMAr8S_F92KCN5gl1LJ9I11sIYAyzbzfr2b7oRK7Q_o/export?format=csv&id=1NMAr8S_F92KCN5gl1LJ9I11sIYAyzbzfr2b7oRK7Q_o&gid=1739949882'
C2L_Shopify_Form_query = 'https://docs.google.com/spreadsheets/d/1DYkPloEJLyAcvLacsW8EnjMHt153-gSdwqxBtEwefOo/export?format=csv&id=1DYkPloEJLyAcvLacsW8EnjMHt153-gSdwqxBtEwefOo&gid=1388039008'

logs = []

def load_gbq(df, table_id, schema):
    '''
    Load Pandas DataFrame to Bigquery
    Dataset & Table must be created beforehand
    Temporary disable paritioning
    Defaults to WRITE_TRUNCATE
    Planning to switch to APPEND
    '''
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition = 'WRITE_TRUNCATE',
        parquet_compression='snappy',
        source_format= 'PARQUET')
    #    time_partitioning = bigquery.TimePartitioning(
    #        type_ = bigquery.TimePartitioningType.DAY,
    #        field = partition_field))
    print(f'Loading to `{table_id}`...')
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config)
    print('Sleep for 5 sec...')
    time.sleep(5)
    table = client.get_table(table_id)  # Make an API request.
    print(job.result())
    result = "Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id)
    print(result)
    logs.append(result)

def get_netsuite(query):
    '''
    Query NetSuite using ODBC
    Planning to switch to JDBC or REST API
    '''
    cnxn = pyodbc.connect('DSN=NetSuiteML;uid=minh.le@vuanem.com;PWD=BI2024BI2024')
    sales_order = pd.read_sql(query, cnxn)
    print(sales_order.head())
    return sales_order

sql_specs = {
    'username': 'root',
    'pwd': 'VuaNem@2020',
    'server': '1.55.215.47',
    'db_name': 'vuanem_ecommerce'
}

def get_mysql(query):
    '''
    Query Vuanem MySQL DB using pymysql
    '''
    sqlEngine = create_engine(r'mysql+pymysql://{0}:{1}@{2}/{3}'.format(sql_specs['username'], sql_specs['pwd'], sql_specs['server'], sql_specs['db_name']))
    dbConnection = sqlEngine.connect()
    df = pd.read_sql(query, dbConnection)
    print(df.head())
    return df

#NetSuite Sales Order
netsuite_query = '''
    SELECT
        CAST(TRANSACTIONS.TRANDATE AS date) AS TRANDATE,
        TRANSACTIONS.TRANID,
        TRANSACTIONS.CUSTOMER_PHONE,
        TRANSACTION_LINES.LOCATION_ID,
        - SUM(TRANSACTION_LINES.ITEM_COUNT) AS 'ITEM_COUNT',
        - SUM(TRANSACTION_LINES.NET_AMOUNT) AS 'NET_AMOUNT'
    FROM
        "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".TRANSACTION_LINES TRANSACTION_LINES
        LEFT JOIN "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".TRANSACTIONS TRANSACTIONS ON TRANSACTION_LINES.TRANSACTION_ID = TRANSACTIONS.TRANSACTION_ID
        LEFT JOIN "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".INVENTORY_ITEMS ON TRANSACTION_LINES.ITEM_ID = INVENTORY_ITEMS.ITEM_ID
        LEFT JOIN "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".SERVICE_ITEMS ON SERVICE_ITEMS.ITEM_ID = TRANSACTION_LINES.ITEM_ID
    WHERE
        TRANSACTION_LINES.ACCOUNT_ID IN (480, 482, 487, 498, 505, 508, 509, 510, 511, 54, 1079, 1170)
        AND TRANSACTIONS.TRANSACTION_TYPE IN ('Sales Order')
        AND TRANSACTIONS.STATUS <> 'Closed'
        AND (
                (INVENTORY_ITEMS.DISPLAYNAME IS NOT NULL)
                OR(SERVICE_ITEMS.ITEM_ID IN ('136263', '136264'))
        )
    GROUP BY
        TRANSACTIONS.TRANDATE,
        TRANSACTIONS.CREATE_DATE,
        TRANSACTIONS.TRANID,
        TRANSACTION_LINES.LOCATION_ID,
        TRANSACTIONS.CUSTOMER_PHONE
        '''

sales_order = get_netsuite(netsuite_query)

netsuite_schema = [
    bigquery.SchemaField("TRANDATE", "DATE"),
    bigquery.SchemaField('TRANID', "STRING"),
    bigquery.SchemaField('CUSTOMER_PHONE', "STRING"),
    bigquery.SchemaField("LOCATION_ID", "FLOAT"),
    bigquery.SchemaField('ITEM_COUNT', "FLOAT"),
    bigquery.SchemaField("NET_AMOUNT", "FLOAT")]

load_gbq(df=sales_order,
         table_id='NetSuite.SalesOrder',
         schema=netsuite_schema)

del sales_order

#Caresoft Tickets
caresoft_tickets_query = '''
SELECT
    cst.care_soft_ticket_id,
    cst.ticket_no,
    cst.ticket_subject,
    cst.ticket_status,
    cst.ticket_source,
    cst.ticket_priority,
    cst.assignee_id,
    cst.requester_id,
    cst.requester_name,
    cst.requester_email,
    cst.requester_phone_no,
    cst.sla,
    cst.campaign_name,
    cst.created_at,
    cst.updated_at
FROM
    vuanem_ecommerce.care_soft_tickets cst
'''
caresoft_tickets = get_mysql(caresoft_tickets_query)

caresoft_tickets_schema = [
    bigquery.SchemaField('care_soft_ticket_id', "INTEGER"),
    bigquery.SchemaField('ticket_no', "INTEGER"),
    bigquery.SchemaField("ticket_subject", "STRING"),
    bigquery.SchemaField('ticket_status', "STRING"),
    bigquery.SchemaField("ticket_source", "STRING"),
    bigquery.SchemaField('ticket_priority', "STRING"),
    bigquery.SchemaField('assignee_id', "INTEGER"),
    bigquery.SchemaField('requester_id', "INTEGER"),
    bigquery.SchemaField('requester_name', "STRING"),
    bigquery.SchemaField('requester_email', "STRING"),
    bigquery.SchemaField('requester_phone_no', "STRING"),
    bigquery.SchemaField('sla', "STRING"),
    bigquery.SchemaField('campaign_name', "STRING"),
    bigquery.SchemaField('created_at', "TIMESTAMP"),
    bigquery.SchemaField('updated_at', "TIMESTAMP")]

load_gbq(
    df=caresoft_tickets,
    table_id='Caresoft.caresoft_tickets',
    schema=caresoft_tickets_schema
)

def get_care_receive_status():
    '''
    Get Care & Receive Status through Caresoft API
    Planning to switch to temp table
    '''
    headers = {
        'authorization': 'Bearer VBxdGeTLbdcPs1M',
        'content-type': 'application/json'
    }
    r2 = requests.get('https://api.caresoft.vn/VUANEM/api/v1/tickets/custom_fields', headers=headers).json()
    care_status_df = pd.DataFrame(r2['custom_fields'][0])
    care_status_df = pd.json_normalize(care_status_df.to_dict('list'), ['values']).apply(pd.Series)
    receive_status_df = pd.DataFrame(r2['custom_fields'][1])
    receive_status_df = pd.json_normalize(receive_status_df.to_dict('list'), ['values']).apply(pd.Series)
    return care_status_df, receive_status_df

def transform_tickets_custom_fields(df):
    df = df.merge(care_status_df, how='left', left_on='trang_thai_cham_soc', right_on='id')
    df = df.merge(receive_status_df, how='left', left_on='trang_thai_tiep_nhan', right_on='id')
    df = df.drop(['trang_thai_cham_soc', 'trang_thai_tiep_nhan', 'id_x', 'id_y'], axis=1)
    df.rename(mapper={
        'lable_x': 'trang_thai_cham_soc',
        'lable_y': 'trang_thai_tiep_nhan'
    }, axis=1, inplace=True)
    return df

#Caresoft Tickets Custom Fields
caresoft_tickets_custom_fields_query = '''
SELECT
	ticket_id,
	SUM(CASE WHEN custom_filed_id = 4871 THEN custom_filed_value ELSE null END) AS trang_thai_cham_soc,
	SUM(CASE WHEN custom_filed_id = 4893 THEN custom_filed_value ELSE null END) AS trang_thai_tiep_nhan
FROM
	vuanem_ecommerce.care_soft_ticket_custom_fileds cstcf
GROUP BY ticket_id
'''

caresoft_tickets_custom_fields = get_mysql(caresoft_tickets_custom_fields_query)

care_status_df, receive_status_df = get_care_receive_status()

caresoft_tickets_custom_fields = transform_tickets_custom_fields(caresoft_tickets_custom_fields)

caresoft_tickets_custom_fields_schema = [
    bigquery.SchemaField('ticket_id', "INTEGER"),
    bigquery.SchemaField('trang_thai_cham_soc', "STRING"),
    bigquery.SchemaField("trang_thai_tiep_nhan", "STRING")]

load_gbq(
    df=caresoft_tickets_custom_fields,
    table_id='Caresoft.caresoft_tickets_custom_fields',
    schema=caresoft_tickets_custom_fields_schema
)

del caresoft_tickets, caresoft_tickets_custom_fields

#Caresoft Call Logs
caresoft_call_logs_query = '''
SELECT
    call_id,
    care_soft_id,
    customer_id,
    caller,
    agent_id,
    group_id,
    call_type,
    call_status,
    start_time,
    end_time,
    created_at,
    updated_at
FROM
    vuanem_ecommerce.care_soft_call_logs cscl
'''

caresoft_call_logs = get_mysql(caresoft_call_logs_query)

caresoft_call_logs_schema = [
    bigquery.SchemaField('call_id', "STRING"),
    bigquery.SchemaField('care_soft_id', "INTEGER"),
    bigquery.SchemaField("customer_id", "INTEGER"),
    bigquery.SchemaField('caller', "STRING"),
    bigquery.SchemaField("agent_id", "INTEGER"),
    bigquery.SchemaField('group_id', "INTEGER"),
    bigquery.SchemaField('call_type', "INTEGER"),
    bigquery.SchemaField('call_status', "STRING"),
    bigquery.SchemaField('start_time', "TIMESTAMP"),
    bigquery.SchemaField('end_time', "TIMESTAMP"),
    bigquery.SchemaField('created_at', "TIMESTAMP"),
    bigquery.SchemaField('updated_at', "TIMESTAMP")]

load_gbq(
    df=caresoft_call_logs,
    table_id='Caresoft.caresoft_call_logs',
    schema=caresoft_call_logs_schema
)

# FUNNEL
vuanem_salescall_salescall_query = '''
SELECT
    customer_name AS name,
    customer_tel AS phone,
    customer_email AS email,
    gclid,
    campain,
    from_landing,
    created_at AS dt,
    CAST(created_at AS date) as date,
    shopify_order_id,
    (CASE WHEN shopify_order_id <> '0' THEN 'Shopify Order' ELSE 'LP EC' END) AS source
FROM
    vuanem_ecommerce.vuanem_salescall_salescall vss
'''

vuanem_call = '''
SELECT
    call_date AS dt,
    CAST(call_date AS date) AS date,
    phone,
    status,
    'Hotline' AS channel
FROM
    vuanem_ecommerce.vuanem_call
WHERE hotline = '18002092'
ORDER BY call_date
'''

#C2C
C2C = get_mysql(vuanem_call)

#C2M
C2M = pd.read_csv(
    C2M_query,
    usecols=['Tên', 'Phone(raw)', 'Date(raw)'],
    dtype={'Tên':str, 'Phone(raw)':str, 'Date(raw)':str})
C2M['channel'] = 'Messages'
C2M.rename(mapper={
    'Tên': 'name',
    'Phone(raw)': 'phone',
    'Date(raw)': 'date'
},axis=1,inplace=True)
C2M['dt'] = pd.to_datetime(C2M['date'], format='%d/%m/%Y')
C2M['date'] = pd.to_datetime(C2M['date'], format='%d/%m/%Y').dt.date

#C2L
#C2L Salescalls
C2L_vuanem_salescalls = get_mysql(vuanem_salescall_salescall_query)
C2L_vuanem_salescalls['shopify_order_id'] = C2L_vuanem_salescalls['shopify_order_id'].astype(str)

#C2L_FB_Ads
C2L_FB_Ads = pd.read_csv(C2L_FB_Ads_query, usecols=['Date', 'Name', 'Phone', 'Email'], parse_dates=['Date'])
C2L_FB_Ads['source'] = 'LP Digital - FB Ads'
C2L_FB_Ads.columns = C2L_FB_Ads.columns.str.lower()
C2L_FB_Ads.rename(mapper={
    'date': 'dt'
},axis=1,inplace=True)
C2L_FB_Ads['date'] = C2L_FB_Ads['dt'].dt.date

#C2L_FB_3rd1
C2L_FB_3rd1 = pd.read_csv(C2L_FB_3rd1_query, usecols=['Date', 'Name', 'Phone', 'Email', 'Gclid'], parse_dates=['Date'])
C2L_FB_3rd1.columns = C2L_FB_3rd1.columns.str.lower()
C2L_FB_3rd1['source'] = 'LP Digital - FB Ads 3rd1'
C2L_FB_3rd1.rename(mapper={
    'date': 'dt'
},axis=1,inplace=True)
C2L_FB_3rd1['date'] = C2L_FB_3rd1['dt'].dt.date

#C2L_FB_3rd2
C2L_FB_3rd2 = pd.read_csv(C2L_FB_3rd2_query, usecols=['Date', 'Name', 'Phone', 'Email', 'Gclid'], parse_dates=['Date'])
C2L_FB_3rd2.columns = C2L_FB_3rd2.columns.str.lower()
C2L_FB_3rd2['source'] = 'LP Digital - FB Ads'
C2L_FB_3rd2.rename(mapper={
    'date': 'dt'
},axis=1,inplace=True)
C2L_FB_3rd2['date'] = C2L_FB_3rd2['dt'].dt.date

#C2L_Shopify_Form
C2L_Shopify_Form = pd.read_csv(C2L_Shopify_Form_query, usecols=['First Subscribed On', 'Phone Number', 'Email', 'gclid', 'initial_utm_medium', 'initial_utm_source'], parse_dates=['First Subscribed On'],date_parser=lambda col: pd.to_datetime(col, utc=True))
C2L_Shopify_Form.rename(mapper={
    'First Subscribed On': 'dt',
    'Phone Number': 'phone',
    'Email': 'email'
}, axis=1, inplace=True)
C2L_Shopify_Form['source'] = 'LP Digital - Shopify Form'
C2L_Shopify_Form['dt']= C2L_Shopify_Form['dt'].dt.tz_localize(None)
C2L_Shopify_Form['date'] = C2L_Shopify_Form['dt'].dt.date

#All C2L Source
C2L = pd.concat([C2L_FB_Ads, C2L_FB_3rd1, C2L_FB_3rd2, C2L_Shopify_Form, C2L_vuanem_salescalls])
C2L['channel'] = 'Leads'

#Concat all C2Leads
C2Leads = pd.concat([C2C, C2M, C2L])
C2Leads['phone'] = C2Leads['phone'].apply(change_phone)

C2Leads_schema = [
    bigquery.SchemaField('dt', "TIMESTAMP"),
    bigquery.SchemaField('date', "DATE"),
    bigquery.SchemaField("phone", "STRING"),
    bigquery.SchemaField('status', "STRING"),
    bigquery.SchemaField("channel", "STRING"),
    bigquery.SchemaField('name', "STRING"),
    bigquery.SchemaField('email', "STRING"),
    bigquery.SchemaField('source', "STRING"),
    bigquery.SchemaField('gclid', "STRING"),
    bigquery.SchemaField('initial_utm_medium', "STRING"),
    bigquery.SchemaField('initial_utm_source', "STRING"),
    bigquery.SchemaField('campain', "STRING"),
    bigquery.SchemaField('from_landing', "STRING"),
    bigquery.SchemaField('shopify_order_id', "STRING")]

load_gbq(
    df=C2Leads,
    table_id='C2Leads.C2Leads',
    schema=C2Leads_schema
)
client.close()

logging.basicConfig(filename='2GBQ.log', level=logging.INFO)
logging.info(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
logging.info('\n'.join(logs))