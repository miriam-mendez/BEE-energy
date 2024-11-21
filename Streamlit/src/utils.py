from src.ui import date_display,month_display,year_display
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import psycopg2

START_YEAR = 2021

province_mapping = {
    '0':'Barcelona',
    '1':'Girona',
    '2':'Lleida',
    '4':'Tarragona'
}

conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='',
    host='',  # e.g., 'localhost'
    port='5432'   # default is 5432
)

def time_granularity(time,table,start_date):
    data, prediction = table
    print(data)
    print(prediction)
    if time == 'monthly':
        end_date = start_date + relativedelta(months=1)
        query = f"""
            SELECT e.*,r.consumption
            FROM {data} e, {prediction} r
            WHERE e.postalcode = r.postalcode and r."date" = e."date" and e.date >= '{start_date.strftime('%Y-%m-%d')}' AND e.date < '{end_date.strftime('%Y-%m-%d')}'
            ORDER BY date;
        """
    elif time == 'annual':
        year = start_date
        query = f"""
            SELECT e.*, r.consumption
            FROM {data} e, {prediction} r
            WHERE e.postalcode = r.postalcode and r.month = e.month and r.year=e.year and e.year = '{year}'
            ORDER BY year,month;
        """
        print(query)
    df = pd.read_sql_query(query, conn)
    print(df)
    return  df


def region_granularity(df,region):
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
    return df_grouped, geojson_file
    

#######################
# Plots
def make_choropleth(input_df, input_id, geojson_data, input_color_theme):
    choropleth = px.choropleth_mapbox(
            input_df,
            locations="postal_code",
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
        text = input_df['postal_code'],
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



