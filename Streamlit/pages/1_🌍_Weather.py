import streamlit as st
from src.ui import sidebar,date_display,month_display,year_display
import datetime
from dateutil.relativedelta import relativedelta
import json
import pandas as pd
import psycopg2
import plotly.express as px
from streamlit_tags import st_tags
import calendar
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import seaborn as sns
import matplotlib.pyplot as plt

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


def fetch_time_query(time):
    if time == 'daily':
        date = date_display(START_YEAR)
        query = f"""
            SELECT e.*, r.consumption 
            FROM era5 e, residential_consumption r
            WHERE e.postalcode = r.postalcode and e.time = r.time and DATE(e.time) = '{date.strftime('%Y-%m-%d')}'
            ORDER BY time;
        """
    elif time == 'monthly':
        year, month = month_display(START_YEAR)
        start_date = datetime.date(year,month,1)
        end_date = start_date + relativedelta(months=1)
        query = f"""
            SELECT e.*,r.consumption
            FROM era5_daily e, residential_consumption_aggregated r
            WHERE e.postalcode = r.postalcode and r."date" = e."date" and e.date >= '{start_date.strftime('%Y-%m-%d')}' AND e.date < '{end_date.strftime('%Y-%m-%d')}'
            ORDER BY date;
        """
    elif time == 'annual':
        year = year_display(START_YEAR)
        query = f"""
            SELECT e.*, r.consumption
            FROM era5_monthly e, residential_consumption_monthly r
            WHERE e.postalcode = r.postalcode and r.month = e.month and r.year=e.year and e.year = '{year}'
            ORDER BY month;
        """
    return  query

province_mapping = {
    '0':'Barcelona',
    '1':'Girona',
    '2':'Lleida',
    '4':'Tarragona'
}

#######################
# Sidebar
with st.sidebar:
    with st.expander("Time granularity",expanded=True):
        time = st.selectbox(' ', ['annual','monthly','daily'],label_visibility='collapsed')
        query = fetch_time_query(time)
        df = pd.read_sql_query(query, conn)

    with st.expander("Region granularity",expanded=True):
        region = st.selectbox(" dasda", ['postal codes', 'provinces','catalonia' ],label_visibility='collapsed')
        if region == 'catalonia':
            geojson_file = './src/data/catalonia.geojson'
            if 'date' in df.columns:
                df_grouped = df.groupby(df['date']).mean(numeric_only=True).reset_index()
            elif 'month' in df.columns:
                df_grouped = df.groupby([df['year'], df['month']]).mean(numeric_only=True).reset_index()                
            else:
                df_grouped = df.groupby(df['time']).mean(numeric_only=True).reset_index()        
            df_grouped['postalcode'] = ['catalonia'] * len(df_grouped)
            
        elif region == 'provinces':
            geojson_file = './src/data/provinces.geojson'
            if 'date' in df.columns:
                df_grouped = df.groupby([df['postalcode'].str.slice(0, 1), df['date']]).mean(numeric_only=True).reset_index()
            elif 'month' in df.columns:
                df_grouped = df.groupby([df['postalcode'].str.slice(0, 1), df['year'], df['month']]).mean(numeric_only=True).reset_index()                
            else:
                df_grouped = df.groupby([df['postalcode'].str.slice(0, 1), df['time']]).mean(numeric_only=True).reset_index()        
            df_grouped['postalcode'] = df_grouped['postalcode'].replace(province_mapping)
        elif region == 'postal codes':
            geojson_file = './src/data/postalcodes.geojson'
            df_grouped = df
        
        with open(geojson_file, 'r') as f:
            geojson_data = json.load(f)
            
    # color_theme_list = ['blues', 'cividis', 'greens', 'inferno', 'magma', 'plasma', 'reds', 'rainbow', 'turbo', 'viridis']
    # selected_color_theme = st.selectbox('Select a color theme', color_theme_list)
            
#######################
# Plots
def make_choropleth(input_df, input_id, geojson_data, input_color_theme):
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
        hovertemplate='<b>Location: %{text} </b><br>' + 'Value: %{customdata:.2f}',
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

def energy_character(df,input,postal_codes=['08001'],inputly='month'):
    df = df[df['postalcode'] == postal_codes]
    feature = df[input].values
    consumption = df['consumption'].values
    maxf,minf = max(feature)+0.1*max(feature), min(feature)-0.1*min(feature)
    maxc,minc = max(consumption)+0.1*max(consumption), min(consumption)-0.1*min(consumption)
    fig = go.Figure(
        data=go.Bar(
            x= df[inputly].values,
            y=feature,
            name=input,
            marker=dict(color="lightskyblue"),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df[inputly].values,
            y=consumption,
            yaxis="y2",
            name="Consumption",
            marker=dict(color="midnightblue"),
        )
    )

    fig.update_layout(
        legend=dict(orientation="h"),
        yaxis=dict(
            title=dict(text=input),
            side="left",
            range=[minf,maxf],
        ),
        yaxis2=dict(
            title=dict(text="Consumption"),
            side="right",
            range=[minc, maxc],
            overlaying="y",
            tickmode="sync",
        ),
    )
    return fig

def time_series_consumption(df,date,input,postal_codes=['08001'],inputly='hourly'):
    fig, ax = plt.subplots()  # Changed 'x' to 'ax'
    df = df.reset_index()
    # df['time'] = pd.to_datetime(df['time'])  # Ensure 'time' column is in datetime format
    df.set_index(date, inplace=True)  # Set 'time' column as the index
    
    for code in postal_codes:
        # Filter data for the postal code
        postal_code_data = df[df['postalcode'] == code]
        hourly_data = postal_code_data[input]
        
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
    st.markdown('#### ERA5 Land Data')
    # st.dataframe(df)
    # print(df.dtypes)
    feature = st.selectbox("Select a feature to analyse", ['airtemperature', 'cdd', 'hdd', 'relativehumidity', 'windspeed', 'winddirection', 'ghi', 'dni', 'sunelevation'])
    if time == 'annual':
        number = st.slider("Time in months", 1, 12)
    elif time == 'monthly':
        month = df['date'].iloc[0].month
        year = df['date'].iloc[0].year
        num_days = calendar.monthrange(year, month)[1]
        number = st.slider("Time in days", 1, num_days)
    else:
        number = st.slider("Time in hours", 0, 23)
    choropleth = make_choropleth(df_grouped[fileter_slide[time](number)], feature, geojson_data,'blues')
    st.plotly_chart(choropleth, use_container_width=True)

with col[1]:
    # st.markdown('#### Aggregation by provinces (mean values)')

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
    fig = energy_character(df_grouped,feature,postalcodes[-1],time_agg[time])
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
    # st.table(df_grouped[(df_grouped['postal_code'].isin(postalcodes)) & (fileter_slide[time](number))])
    st.line_chart(df_grouped[df_grouped['postalcode'].isin(postalcodes)],x=time_agg[time],y=feature,color="postalcode")
    # res = time_series_consumption(df_grouped,time_agg[time],feature,postalcodes, time_aggly[time])
    # st.pyplot(res, use_container_width=True)
    
   


fig, ax = plt.subplots()
sns.heatmap(df_grouped.corr(), ax=ax)
st.write(fig)    
    
    # df1 = pd.read_csv('/home/eouser/Documentos/population-dashboard/era5land_provinces_time.csv')
    # print(df1)
    # res = time_series_consumption(df1,date,selected_color_theme,postalcodes)
    # st.pyplot(res, use_container_width=True)

    
