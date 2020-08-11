#!/usr/bin/env python
# coding: utf-8

# In[18]:


import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


# In[19]:


username = 'root'
pwd = 'VuaNem@2020'
server = '1.55.215.47'
db_name = 'vuanem_landing'


# In[20]:


sqlEngine = create_engine(r'mysql+pymysql://{0}:{1}@{2}/{3}'.format(username, pwd, server, db_name))
dbConnection = sqlEngine.connect()


# In[21]:


query = '''
SELECT
    ID,
    customer_name,
    customer_tel,
    customer_email,
    gclid,
    campain,
    created_at
FROM
    km_salescall_index
'''


# In[22]:


df = pd.read_sql(query, dbConnection)


# In[23]:


df['created_at'] = df['created_at'].dt.date.astype(str)
df = df.drop_duplicates(['customer_name', 'customer_tel', 'gclid', 'campain', 'created_at'])

# In[24]:


SAMPLE_SPREADSHEET_ID_input = '1DYkPloEJLyAcvLacsW8EnjMHt153-gSdwqxBtEwefOo'
non_luckywheel = r"'C2L'!A1:G1000000"

# In[25]:


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


# In[26]:


Create_Service(r'F:/credentials.json', 'sheets', 'v4',['https://www.googleapis.com/auth/spreadsheets'])


# In[27]:


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

Export_Data_To_Sheets(df, non_luckywheel)
print('Done at 1DYkPloEJLyAcvLacsW8EnjMHt153-gSdwqxBtEwefOo')
