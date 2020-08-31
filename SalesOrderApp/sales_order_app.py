import numpy as np
import pandas as pd
import jaydebeapi
#import pyodbc
from datetime import datetime, timedelta
import plotly.graph_objects as go
import streamlit as st

@st.cache
def get_connection_jdbc():
    conn = jaydebeapi.connect(
        'com.netsuite.jdbc.openaccess.OpenAccessDriver',
        "jdbc:ns://4975572.connect.api.netsuite.com:1708;"
            + "ServerDataSource=NetSuite.com;"
            + "Encrypted=1;"
            + "CustomProperties=(AccountID=4975572;RoleID=1022)",
        {'user': "minh.le@vuanem.com", 'password': "BI@2023cute"},
        'NQjc.jar')
    return conn
'''
@st.cache
def get_connection_odbc():
    conn = pyodbc.connect('DSN=NetSuiteML;uid=minh.le@vuanem.com;PWD=BI@2023cute')
    return conn
'''
def get_data(conn):
    day = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')
    netsuite_query = f'''
    SELECT
        CAST(TRANSACTIONS.TRANDATE AS date) AS "TRANDATE",
        - SUM(TRANSACTION_LINES.NET_AMOUNT) AS "Sales Order",
        COUNT(DISTINCT TRANSACTIONS.TRANID) AS "No of Trans"
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
        AND TRANDATE >= '{day}'
    GROUP BY
        TRANSACTIONS.TRANDATE
    ORDER BY
        TRANSACTIONS.TRANDATE
    '''
    df = pd.read_sql(netsuite_query, con=conn)
    return df

def get_plot(df):
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            name='Today',
            x=df['TRANDATE'][-2:],
            y=df['Sales Order'][-2:],
            mode='markers+lines',
            marker={
                'size':20,
                'color':'red'
            }
        )
    )

    fig.add_trace(
        go.Scatter(
            name='Sales Order',
            x=df['TRANDATE'][:-1],
            y=df['Sales Order'][:-1],
            mode='markers+lines',
            marker={
                'size':10,
                'color':'blue'
            }
        )
    )
    return fig

conn = get_connection_jdbc()
df = get_data(conn)
plot = get_plot(df)

st.title('Sales Order')

st.plotly_chart(plot)