import streamlit as st
from src.ui import sidebar,fetch_time_query
from src.utils import region_granularity,time_granularity
from src.plots import * 
import psycopg2
import pandas as pd
import json
import calendar
from streamlit_tags import st_tags
from calendar import month_abbr
import datetime
from dateutil.relativedelta import relativedelta

st.set_page_config(
    page_title="BEE Energy",
    page_icon="🔋",
    layout="wide",
    initial_sidebar_state="expanded"
)
sidebar()


tab1, tab2 = st.tabs(["Climate DT", "Extreme DT"])
time_agg = {
    'annual':'month',
    'monthly':'date',
    'daily':'time'
}

##### (data, prediction)
tables = {
    'ClimateDT':{
        'annual':('climatedt_monthly','residential_consumption_predicted_climatedt_monthly'),
        'monthly':('climatedt_aggregated','residential_consumption_predicted_climatedt'),
        'daily':('climatedt_materialized',None)
    },
    
    'ExtremeDT':{
        'annual':('extremesdt_monthly','residential_consumption_predicted_extremedt_monthly'),
        'monthly':('extremesdt_aggregated','residential_consumption_predicted_extremedt'),
        'daily':('extremesdt_materialized',None),
    }
}
def fileter_slide(time,df_grouped,x):
    if time == 'annual': return  df_grouped['month'] == str(x).zfill(2)
    if time == 'monthly': return pd.to_datetime(df_grouped['date']).dt.day == x
    if time == 'daily': return pd.to_datetime(df_grouped['time'].astype(str)).dt.hour == x

with st.sidebar:
    with st.expander("Time granularity",expanded=True):
        key = 1     
        time = st.selectbox(' ', ['annual','monthly'],index=1,label_visibility='collapsed',key=1)
    with st.expander("Region granularity",expanded=True):
        region = st.selectbox(" dasda", ['postal codes', 'provinces','catalonia' ],label_visibility='collapsed',key=5)
        
               
with tab1:
    model1 = 'ClimateDT'
    table1 = tables[model1][time]
    
    st.markdown('#### Climate DT Data')           
    col = st.columns((5, 4), gap='small')
    with col[0]:
        migrationcols = st.columns((5, 4), gap='small')
        with migrationcols[1]:
            feature1 = st.selectbox("Select a feature to analyse", ['airtemperature', 'cdd', 'hdd', 'relativehumidity', 'windspeed', 'winddirection', 'ghi', 'dni', 'sunelevation'])
        with migrationcols[0]:
            if time == 'monthly':
                year1 = st.selectbox("Select a time", ['2026'],key=4)
                months1 = month_abbr[1:]
                report_month_str1 = st.radio(label="Select a month", options=months1, index=0, horizontal=True,key=11,label_visibility='collapsed')
                month1 = months1.index(report_month_str1) + 1
                start_date1 = datetime.date(int(year1),int(month1),1)
                
            elif time == 'annual':
                start_date1 = st.selectbox("Select a time", [2026],key=3*key)
        df1 = time_granularity(time,table1,start_date1)
        df_grouped1,geojson_file1 = region_granularity(df1,region)
        with open(geojson_file1, 'r') as f:
            geojson_data1 = json.load(f)
                

        if time == 'annual':
            number1 = st.slider("Time in months", 1, 12)
        elif time == 'monthly':
            month1 = df1['date'].iloc[0].month
            year1 = df1['date'].iloc[0].year
            num_days1 = calendar.monthrange(year1, month1)[1]
            number1 = st.slider("Time in days", 1, num_days1)
       
        choropleth = make_choropleth(df_grouped1[fileter_slide(time,df_grouped1,number1)], feature1, geojson_data1,'blues')
        st.plotly_chart(choropleth, use_container_width=True)

    with col[1]:
        if region == 'postal codes':
            postalcodes = st_tags(
                label=f'Enter {region}:',
                text='Press enter to add more',
                value=['08031'],
                maxtags=100,
                key="6")
        if region == 'provinces':
            postalcodes = st.multiselect(
                f'Enter {region}:',
                ["Barcelona", "Lleida", "Girona", "Tarragona"],
                ["Barcelona", "Lleida"],key=22
            )
        if region == 'catalonia':
            postalcodes = ['catalonia']

        fig = energy_character(df_grouped1,feature1,postalcodes[-1],time_agg[time])
        st.plotly_chart(fig, theme="streamlit", use_container_width=True)
        
        st.line_chart(df_grouped1[df_grouped1['postalcode'].isin(postalcodes)].rename(columns={'postalcode':region}),x=time_agg[time],y=feature1,color=region)

with tab2:
    print("holaaaa")
    model2 = 'ExtremeDT'
    table2 = tables[model2][time]
    
    st.markdown('#### Extreme DT Data')           
    col = st.columns((5, 4), gap='small')

    with col[0]:
        feature2 = st.selectbox("Select a feature to analyse", ['airtemperature', 'cdd', 'hdd', 'relativehumidity', 'windspeed', 'winddirection', 'ghi', 'dni', 'sunelevation'],key=7)

        if time == 'monthly':
            start_date2 = datetime.date(int('2024'),int('10'),1)
            
        elif time == 'annual':
            start_date2 = 2024
            
        df2 = time_granularity(time,table2,start_date2)
        df_grouped2,geojson_file2 = region_granularity(df2,region)
        with open(geojson_file2, 'r') as f:
            geojson_data2 = json.load(f)
        
        if time == 'annual':
            number2 = st.slider("Time in months", 1, 12,key = 18)
        elif time == 'monthly':
            month2 = df2['date'].iloc[0].month
            year2 = df2['date'].iloc[0].year
            num_days2 = calendar.monthrange(year2, month2)[1]
            number2 = st.slider("Time in days", 1, num_days2,key=19)
       
        choropleth = make_choropleth(df_grouped1[fileter_slide(time,df_grouped1,number1)], feature1, geojson_data1,'blues')
        st.plotly_chart(choropleth, use_container_width=True)
    
    with col[1]:
        if region == 'postal codes':
            postalcodes = st_tags(
                label=f'Enter {region}:',
                text='Press enter to add more',
                value=['08031'],
                maxtags=100,
                key="13")
        if region == 'provinces':
            postalcodes = st.multiselect(
                f'Enter {region}:',
                ["Barcelona", "Lleida", "Girona", "Tarragona"],
                ["Barcelona", "Lleida"],key=21
            )
        if region == 'catalonia':
            postalcodes = ['catalonia']

        fig2 = energy_character(df_grouped2,feature2,postalcodes[-1],time_agg[time])
        st.plotly_chart(fig2, theme="streamlit", use_container_width=True)
        st.line_chart(df_grouped2[df_grouped2['postalcode'].isin(postalcodes)].rename(columns={'postalcode':region}),x=time_agg[time],y=feature2,color=region)

    # query = fetch_time_query(time,time_agg[time][3],2)
    # df = pd.read_sql_query(query, conn)
    # df_grouped,geojson_file = region_granularity(df,region)
    # with open(geojson_file, 'r') as f:
    #     geojson_data = json.load(f)
        
#     # col = st.columns((5, 4), gap='small')
#     # with col[0]:
#     #     st.markdown('#### Extremes DT Data')
#     #     feature = st.selectbox("Select a feature to analyse", ['airtemperature', 'dewairtemperature', 'dhi','dni','ghi','relativehumidity', 'windspeed', 'winddirection','sunazimuth', 'sunelevation','surfacepressure','thermalradiation','totalcloudcover'])
#     #     if time == 'annual':
#     #         number = st.slider("Time in months", 1, 12)
#     #     elif time == 'monthly':
#     #         month = df['date'].iloc[0].month
#     #         year = df['date'].iloc[0].year
#     #         num_days = calendar.monthrange(year, month)[1]
#     #         number = st.slider("Time in days", 1, num_days)
#     #     else:
#     #         number = st.slider("Time in hours", 0, 23)
            
#     #     print(df_grouped)
#     #     choropleth = make_choropleth(df_grouped[fileter_slide[time](number)], feature, geojson_data,'blues')
#     #     st.plotly_chart(choropleth, use_container_width=True)

    # with col[1]:
    #     if region == 'postal codes':
    #         postalcodes = st_tags(
    #             label=f'Enter {region}:',
    #             text='Press enter to add more',
    #             value=['08031'],
    #             maxtags=100,
    #             key="2")
    #     if region == 'provinces':
    #         postalcodes = st.multiselect(
    #             f'Enter {region}:',
    #             ["Barcelona", "Lleida", "Girona", "Tarragona"],
    #             ["Barcelona", "Lleida"],
    #         )
    #     if region == 'catalonia':
    #         postalcodes = ['catalonia']
        
    #     fig = energy_character(df_grouped,feature,postalcodes[-1],time_agg[time][0])
    #     st.plotly_chart(fig, theme="streamlit", use_container_width=True)
    #     st.line_chart(df_grouped[df_grouped['postal_code'].isin(postalcodes)],x=time_agg[time][0],y=feature,color="postal_code")