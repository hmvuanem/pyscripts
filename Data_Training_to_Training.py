#!/usr/bin/env python
# coding: utf-8

# In[48]:


import numpy as np
import pandas as pd
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


# In[59]:


df = pd.read_csv(r'https://docs.google.com/spreadsheets/d/1gDJ7MblhK03oHEg3f3iouBgW3z1W70yx4skkMZVbAus/export?format=csv&id=1gDJ7MblhK03oHEg3f3iouBgW3z1W70yx4skkMZVbAus&gid=1833861518')


# In[60]:


df


# In[61]:


df['Timestamp'] = pd.to_datetime(df['Timestamp'], dayfirst=True).dt.date.astype(str)


# In[62]:


regex = r'(VN\d{4})'


# In[63]:


df.rename(mapper={
    'Tên Trainer phụ trách khóa đào tạo (có thể chọn nhiều)':'trainer',
    'Mã số nhân viên ': 'VN code'}, axis=1, inplace=True)


# In[64]:


df.columns


# In[66]:


df['VN code'] = df['VN code'].str.replace(' ', '')
df['VN code'] = df['VN code'].str.extract(regex)


# In[67]:


b = df['trainer'].str.extractall(regex)


# In[68]:


b = b.reset_index().set_index('level_0').drop('match', axis = 1)


# In[69]:


a = b.merge(df, how='left', left_index=True, right_index=True).drop('trainer', axis=1).rename(mapper={0:'trainer'}, axis=1)


# In[70]:


a = a.rename(mapper={0:'trainer'}, axis=1)


# In[71]:


a = a.fillna('')


# In[72]:


a['Số điện thoại'] = '\'' + a['Số điện thoại']


# In[73]:


a.head()


# In[74]:


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


# In[75]:


Create_Service('F:/credentials.json', 'sheets', 'v4',['https://www.googleapis.com/auth/spreadsheets'])


# In[76]:


SAMPLE_SPREADSHEET_ID_input = '1gDJ7MblhK03oHEg3f3iouBgW3z1W70yx4skkMZVbAus'
SAMPLE_RANGE_NAME = r'Data_BI!A1:I50000'
def Export_Data_To_Sheets():
    response_date = service.spreadsheets().values().update(
        spreadsheetId=SAMPLE_SPREADSHEET_ID_input,
        valueInputOption='USER_ENTERED',
        range=SAMPLE_RANGE_NAME,
        body=dict(
            majorDimension='ROWS',
            values=a.T.reset_index().T.values.tolist())
    ).execute()
    print('Done')


# In[77]:


Export_Data_To_Sheets()


# In[ ]:




