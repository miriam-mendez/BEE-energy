"""
Streamlit DestinE Dashborad

App to characterize electricity consumption

There is also an accompanying png and pdf version

Author:
    @miriam-mendez : https://github.com/miriam-mendez

Contributors:
    @gmor : https://github.com/gmor
    @jbroto : https://github.com/jbroto
"""

import streamlit as st
from src.ui import sidebar,date_display,month_display,year_display
import psycopg2
import matplotlib.pyplot as plt
import json
from dateutil.relativedelta import relativedelta
import pandas as pd
import calendar
import datetime
import plotly.express as px
from streamlit_tags import st_tags
from statsmodels.tsa.seasonal import seasonal_decompose

st.set_page_config(
    page_title="BEE Energy",
    page_icon="🔋",
    layout="wide",
    initial_sidebar_state="expanded"
)
sidebar()


conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='',
    host='',  # e.g., 'localhost'
    port='5432'   # default is 5432
)

START_YEAR = 2021 # Store last 5 years: year - 5[2024,2023,2022,2021,2020]


# def fetch_residential_hourly_consumption(minDate,maxDate):
#     query = f"""
#         SELECT time, SUM(consumption) AS consumption
#         FROM residential_consumption_aggregated
#         WHERE date >= '2023-01-01' date <= '2023-12-31'
#         GROUP BY time
#         ORDER BY time;
#     """
#     time_series = pd.read_sql_query(query, conn)
#     return time_series


def fetch_time_query(time):
    if time == 'daily':
        date = date_display(START_YEAR)
        query = f"""
            SELECT * 
            FROM residential_consumption
            WHERE DATE(time) = '{date.strftime('%Y-%m-%d')}'
            ORDER BY time;
        """
    elif time == 'monthly':
        year, month = month_display(START_YEAR)
        start_date = datetime.date(year,month,1)
        end_date = start_date + relativedelta(months=1)
        query = f"""
            SELECT *
            FROM residential_consumption_aggregated
            WHERE date >= '{start_date.strftime('%Y-%m-%d')}' AND date < '{end_date.strftime('%Y-%m-%d')}'
            ORDER BY date;
        """
    elif time == 'annual':
        year = year_display(START_YEAR)
        query = f"""
            SELECT *
            FROM residential_consumption_monthly
            WHERE year = '{year}'
            ORDER BY month;
        """
    return  query

province_mapping = {
    '0':'Barcelona',
    '1':'Girona',
    '2':'Lleida',
    '4':'Tarragona'
}
with st.sidebar:
    # st.date_input("Enter :blue[minimum] date for analysis",min_value=datetime.date(2021,9,30),max_value=datetime.date(2024,9,1),value=datetime.date(2023,1,1))
    # st.date_input("Enter :blue[maximum] date for analysis",min_value=datetime.date(2021,9,30),max_value=datetime.date(2024,9,1),value=datetime.date(2023,12,31))
    with st.expander("Time granularity",expanded=True):
        time = st.selectbox(' ', ['annual','monthly','daily'],label_visibility='collapsed')
        query = fetch_time_query(time) 
        df = pd.read_sql_query(query, conn)
        df["consumption/contracts"] = df["consumption"] / df["contracts"]
        
    with st.expander("Region granularity",expanded=True):
        region = st.selectbox(" dasda", ['postal codes', 'provinces','catalonia' ],label_visibility='collapsed')
        if region == 'catalonia':
            geojson_file = './src/data/catalonia.geojson'
            if 'date' in df.columns:
                df_grouped = df.groupby(df['date']).mean(numeric_only=True).reset_index()
            elif 'month' in df.columns:
                df_grouped = df.groupby([df['year'], df['month']]).mean(numeric_only=True).reset_index()                
            else:
                df_grouped = df.groupby(df['time']).mean().reset_index()        
            df_grouped['postalcode'] = ['catalonia'] * len(df_grouped)
            
        elif region == 'provinces':
            geojson_file = './src/data/provinces.geojson'
            if 'date' in df.columns:
                df_grouped = df.groupby([df['postalcode'].str.slice(0, 1), df['date']]).mean(numeric_only=True).reset_index()
            elif 'month' in df.columns:
                df_grouped = df.groupby([df['postalcode'].str.slice(0, 1), df['year'], df['month']]).mean(numeric_only=True).reset_index()                
            else:
                df_grouped = df.groupby([df['postalcode'].str.slice(0, 1), df['time']]).mean().reset_index()        
            df_grouped['postalcode'] = df_grouped['postalcode'].replace(province_mapping)
        elif region == 'postal codes':
            geojson_file = './src/data/postalcodes.geojson'
            df_grouped = df
        
        with open(geojson_file, 'r') as f:
            geojson_data = json.load(f)
            
   

#######################
# Plots
def make_choropleth(input_df, input_id, geojson_data, input_color_theme='blues'):
    choropleth = px.choropleth_mapbox(
            input_df,
            locations="postalcode",
            featureidkey="properties.region",
            geojson=geojson_data,
            color=input_id,
            color_continuous_scale=input_color_theme,
            mapbox_style="carto-positron",
            zoom=7,
            center={"lat": 41.8, "lon": 1.5}
    )
    choropleth.update_traces(
        hovertemplate='<b>Location: %{text} </b><br>' + 'Value: %{customdata}',
        text = input_df['postalcode'],
        customdata=input_df[input_id] 
    )
    choropleth.update_layout(
        template='plotly_dark',
        plot_bgcolor='rgba(0, 0, 0, 0)',
        paper_bgcolor='rgba(0, 0, 0, 0)',
        margin=dict(l=0, r=0, t=0, b=0),
        height=600
    )
    return choropleth


def time_series_consumption(df,date,input,postalcodes=['08001'],inputly='hourly'):
    fig, ax = plt.subplots()  # Changed 'x' to 'ax'
    df = df.reset_index()
    # df['time'] = pd.to_datetime(df['time'])  # Ensure 'time' column is in datetime format
    df.set_index(date, inplace=True)  # Set 'time' column as the index
    
    for code in postalcodes:
        # Filter data for the postal code
        postalcode_data = df[df['postalcode'] == code]
        hourly_data = postalcode_data[input]
        
        # Plot the data
        ax.plot(hourly_data.index, hourly_data, marker='o', linestyle='-', label=f'Postal Code: {code}')
    
    # Adding labels and title
    ax.set_title(f'{inputly} {input} for Selected Region')  # Corrected to ax.set_title
    ax.set_xlabel(date)  # Corrected to ax.set_xlabel
    ax.set_ylabel(input)  # Corrected to ax.set_ylabel
    ax.legend()  # Display legend
    ax.grid()  # Add grid lines
    plt.xticks(rotation=45)  # Rotate x-axis labels
    plt.tight_layout()  # Adjust layout to prevent overlapping
    
    return fig


#######################
# Dashboard Main Panel
col = st.columns((5, 4), gap='small')
fileter_slide = {
    'annual': lambda x: df_grouped['month'] == str(x).zfill(2),
    'monthly': lambda x: pd.to_datetime(df_grouped['date']).dt.day == x,
    'daily': lambda x: pd.to_datetime(df_grouped['time'].astype(str)).dt.hour == x
}
with col[0]:
    st.markdown('#### Electricity Load Data')
    # st.dataframe(df)
    # print(df.dtypes)
    feature = st.selectbox("Select a feture to analyse", ["consumption","contracts", "consumption/contracts"])

    # feature = st.selectbox("Select a feature to analyse", ['sumEnergy','sumContracts'])
    
    if time == 'annual':
        number = st.slider("Time in months", 1, 12)
    elif time == 'monthly':
        print("monthly:")
        print(df)
        month = df['date'].iloc[0].month
        year = df['date'].iloc[0].year
        num_days = calendar.monthrange(year, month)[1]
        number = st.slider("Time in days", 1, num_days)
    else:
        number = st.slider("Time in hours", 0, 23)
        
    choropleth = make_choropleth(df_grouped[fileter_slide[time](number)], feature, geojson_data)
    st.plotly_chart(choropleth, use_container_width=True)
    
def top5(df):
    grouped_data = df.groupby('postalcode').agg({
        'consumption': 'sum', 
        'contracts': 'mean',
        'consumption/contracts':'mean'  
    }).reset_index()
    grouped_data['rate'] = grouped_data.consumption/grouped_data.contracts
    result = grouped_data.sort_values(by='rate', ascending=True)
    return result.reset_index().drop(columns=['index'])

  
with col[1]:
    # st.markdown('#### Aggregation by provinces (mean values)')
    
    print(df_grouped)
    st.markdown('#### Top 5 - The lowest consumption per capita')
    st.dataframe(top5(df_grouped).head(),
                column_order=("postalcode","consumption", "rate"),
                hide_index=True,
                width=None,
                column_config={
                    "postalcode": st.column_config.TextColumn(
                    "Postal Code",
                ),
                "consumption": st.column_config.TextColumn(
                    "Consumption (MWh)",
                ),
                "rate": st.column_config.ProgressColumn(
                    "Consumption per capita (MWh)",
                    format="%.2f",
                    min_value=0,
                    max_value=max(df.consumption/df.contracts),
                    )}
                )

        # st.altair_chart(make_donut(25, 'Inbound Migration', 'blue'))
    if region == 'postal codes':
        postalcodes = st_tags(
            label=f'Enter {region}:',
            text='Press enter to add more',
            value=['08031'],
            maxtags=100,
            key="1")
    if region == 'provinces':
        postalcodes = st.multiselect(
            f'Enter {region}:',
            ["Barcelona", "Lleida", "Girona", "Tarragona"],
            ["Barcelona", "Lleida"],
        )
    if region == 'catalonia':
        postalcodes = ['catalonia']
        
    
    time_agg = {
        'annual':'month',
        'monthly':'date',
        'daily':'time'
    }
    time_aggly = {
        'annual':'monthly',
        'monthly':'daily',
        'daily':'hourly'
    }
    st.table(df_grouped[(df_grouped['postalcode'].isin(postalcodes)) & (fileter_slide[time](number))])
    # res = time_series_consumption(df_grouped,time_agg[time],'consumption',postalcodes,time_aggly[time])
    # st.pyplot(res, use_container_width=True)

    st.line_chart(df_grouped[df_grouped['postalcode'].isin(postalcodes)],x=time_agg[time],y=feature,color="postalcode")


#########################3333

# st.subheader('Time Series Decomposition')
# minDate = st.date_input(label='Enter :orange[minimum] date for analysis', value=datetime.date(2021,9,30),
#                             min_value=datetime.date(2023,1,1),
#                             max_value=datetime.date(2024,8,31))

# maxDate = st.date_input(label='Enter :orange[maximum] date for analysis', value=datetime.date(2024,8,31),
#                             min_value=datetime.date(2023,2,2),
#                             max_value=datetime.date(2024,8,31))
# if minDate > maxDate:
#     st.warning('Minimum Date should be earlier than maximum Date')
    
# timeSeriesData= fetch_residential_hourly_consumption()
# timeSeriesData = timeSeriesData.set_index('time')
# print(timeSeriesData)

# st.write(f'with decomposition model as :blue[additive]')
# ts_decomposition = seasonal_decompose(timeSeriesData,model='add',period=30)
# T,S,R = ts_decomposition.trend, ts_decomposition.seasonal, ts_decomposition.resid
# with st.expander("See the Trend, Seasonality and Residual Plots"):
#     st.subheader('Trend')
#     st.line_chart(T)
#     st.subheader('Seasonality')
#     st.line_chart(S)
#     st.subheader('Residual')
#     st.line_chart(R,width=1)

    
