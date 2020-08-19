import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

def get_mysql(query, db):
    username = 'root'
    pwd = 'VuaNem@2020'
    server = '1.55.215.47'
    db_name = db
    sqlEngine = create_engine(r'mysql+pymysql://{0}:{1}@{2}/{3}'.format(username, pwd, server, db_name))
    dbConnection = sqlEngine.connect()
    df = pd.read_sql(query, dbConnection)
    return df

C2L_query = '''
SELECT
    salescall_id,
    customer_name,
    customer_tel,
    customer_email,
    gclid,
    campain,
    created_at
FROM
    vuanem_salescall_salescall
'''

LuckyWheel_query = '''
SELECT
   	km_posts.ID AS ID,
	km_posts.post_name AS telephone,
	km_postmeta.meta_value AS data,
	km_posts.post_date
FROM km_posts 
left join km_postmeta on km_postmeta.post_id = km_posts.ID
WHERE km_posts.post_type = 'wplwl_email'
'''

c2l_df = get_mysql(C2L_query, 'vuanem_ecommerce')
c2l_df['created_at'] = c2l_df['created_at'].dt.date.astype(str)
c2l_df = c2l_df.drop_duplicates(['customer_name', 'customer_tel', 'gclid', 'campain', 'created_at'])

LuckyWheel_df = get_mysql(LuckyWheel_query, 'vuanem_landing')
LuckyWheel_df['post_date'] = pd.to_datetime(LuckyWheel_df['post_date']).dt.date.astype(str)
print(LuckyWheel_df.dtypes)
SAMPLE_SPREADSHEET_ID_input = '1DYkPloEJLyAcvLacsW8EnjMHt153-gSdwqxBtEwefOo'
c2l = r"'C2L'!A1:G1000000"
LuckyWheel = r"'LuckyWheel'!A1:D1000000"

def Create_Service(client_secret_file, api_service_name, api_version, *scopes):
    global service
    SCOPES = [scope for scope in scopes[0]]
    cred = None

    if os.path.exists('token_write.pickle'):
        with open('token_write.pickle', 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
            cred = flow.run_local_server()

        with open('token_write.pickle', 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(api_service_name, api_version, credentials=cred)
        print(api_service_name, 'Done')
    except Exception as e:
        print(e)

Create_Service(r'credentials.json', 'sheets', 'v4',['https://www.googleapis.com/auth/spreadsheets'])

def Export_Data_To_Sheets(table, range_name):
    response_date = service.spreadsheets().values().update(
        spreadsheetId=SAMPLE_SPREADSHEET_ID_input,
        valueInputOption='USER_ENTERED',
        range=range_name,
        body=dict(
            majorDimension='ROWS',
            values=table.T.reset_index().T.values.tolist())
    ).execute()
    print('Done' + range_name)

Export_Data_To_Sheets(c2l_df, c2l)
Export_Data_To_Sheets(LuckyWheel_df, LuckyWheel)

print('Done at 1DYkPloEJLyAcvLacsW8EnjMHt153-gSdwqxBtEwefOo')
