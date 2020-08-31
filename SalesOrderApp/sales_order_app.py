import numpy as np
import pandas as pd
import jaydebeapi
#import pyodbc
from datetime import datetime, timedelta
import plotly.graph_objects as go
import streamlit as st
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler
from sklearn.externals.joblib import load

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

def get_model():
    return load_model('model')

@st.cache
def get_sc():
    return load('model/mm_scaler.bin')

def get_data(conn):
    day = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')
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

def process(df):
    today_30 = df[['Sales Order']].iloc[-31:-1,:].values
    today_30 = today_30.reshape(-1,1)
    sc = get_sc()
    today_30 = sc.transform(today_30)
    today_30 = np.reshape(today_30, (1, today_30.shape[0], today_30.shape[1]))
    model = get_model()
    today_pred = model.predict(today_30)
    today_pred = sc.inverse_transform(today_pred)
    return today_pred

def get_plot(df, today_pred):
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            name='Sales Order',
            x=df['TRANDATE'].iloc[-7:-1],
            y=df['Sales Order'].iloc[-7:-1],
            mode='markers+lines',
            marker={
                'size':10,
                'color':'blue'
            }
        )
    )

    fig.add_trace(
        go.Scatter(
            name='Sales Order',
            x=df['TRANDATE'].iloc[-2:],
            y=df['Sales Order'].iloc[-2:],
            mode='markers+lines',
            marker={
                'size':10,
                'color':'blue'
            },
            line={
                'color':'blue',
                'dash':'dash'
            },
            showlegend=False,
            hoverinfo='skip'
        )
    )
    
    fig.add_trace(
        go.Scatter(
            name='Prediction',
            x=df['TRANDATE'][-1:],
            y=today_pred[0],
            mode='markers+lines',
            marker={
                'size':20,
                'color':'green'
            }
        )
    )

    fig.add_trace(
        go.Scatter(
            name='Actual',
            x=df['TRANDATE'][-1:],
            y=df['Sales Order'][-1:],
            mode='markers+lines',
            marker={
                'size':20,
                'color':'red'
            }
        )
    )
    
    fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
            ),
        autosize=True,
        height=400,
        width=700,
        margin=dict(
            l=0,
            r=0,
            t=0,
            b=0,
            pad=0
            )
        )
    return fig

conn = get_connection_jdbc()
df = get_data(conn)
today_pred = process(df)
plot = get_plot(df,today_pred)

st.title('Sales Order')

st.plotly_chart(plot)