import streamlit as st
from pathlib import Path
import base64
from calendar import month_abbr
import datetime
from dateutil.relativedelta import relativedelta


START_YEAR = 2021

def img_to_bytes(img_path):
    img_bytes = Path(img_path).read_bytes()
    encoded = base64.b64encode(img_bytes).decode()
    return encoded

def month_display(start_year):
    years = range(datetime.datetime.now().year, start_year, -1)
    year = st.selectbox(" ", years,label_visibility='hidden',)
    months = month_abbr[1:]
    if year == datetime.datetime.now().year:
        months = month_abbr[1:datetime.datetime.now().month-1]
    
    report_month_str = st.radio(label="insert a month", options=months, index=0, horizontal=True,label_visibility='collapsed')
    month = months.index(report_month_str) + 1
    return year, month


def date_display(start_year):
    date = st.date_input(" ",min_value=datetime.date(start_year,1,1),max_value=datetime.datetime.now(),label_visibility='hidden')
    return date


def year_display(start_year):
    years = range(datetime.datetime.now().year, start_year, -1)
    year = st.selectbox(" ", years,label_visibility='hidden')
    return year

def fetch_time_query(time,table,key):
    if time == 'daily':
        date = date_display(START_YEAR,key)
        query = f"""
            SELECT e.*, r.consumption 
            FROM {table} e, residential_consumption r
            WHERE e.postal_code = r.postalcode and e.time = r.time and DATE(e.time) = '{date.strftime('%Y-%m-%d')}'
            ORDER BY time;
        """
    elif time == 'monthly':
        year, month = month_display(START_YEAR,key)
        start_date = datetime.date(year,month,1)
        end_date = start_date + relativedelta(months=1)
        query = f"""
            SELECT e.*,r.consumption
            FROM {table} e, residential_consumption_aggregated r
            WHERE e.postal_code = r.postalcode and r."date" = e."date" and e.date >= '{start_date.strftime('%Y-%m-%d')}' AND e.date < '{end_date.strftime('%Y-%m-%d')}'
            ORDER BY date;
        """
    elif time == 'annual':
        year = year_display(START_YEAR,key)
        query = f"""
            SELECT e.*, r.consumption
            FROM {table} e, residential_consumption_monthly r
            WHERE e.postal_code = r.postalcode and r.month = e.month and r.year=e.year and e.year = '{year}'
            ORDER BY month;
        """
    return query



def sidebar():
    st.sidebar.markdown(f"""
                <style>
                    [data-testid="stSidebar"] {{
                        background-image: url("data:image/png;base64,{img_to_bytes("src/img/cimne-logo.png")}");
                        background-repeat: no-repeat;
                        background-size: 152px 32px;
                        padding-top: 10px;
                        background-position: 20px 20px;
                    }}
                    [data-testid="stSidebar"]::before {{
                        content: "BEE Energy";
                        margin-left: 20px;
                        margin-top: 0px;
                        font-size: 30px;
                        position: relative;
                        top: 55px;
                    }}
                </style>
                """,
            unsafe_allow_html=True,
        )
    
    return None
