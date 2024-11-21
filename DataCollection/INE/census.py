import pandas as pd
from io import StringIO
import os
import requests
import re


DATA_CENSO2021 = "https://www.ine.es/censos2021/C2021_Indicadores.csv"

censo_ingestion_urls = {
    'Tamaño del hogar': {
        'url':'https://www.ine.es/jaxi/files/tpx/en/csv_bdsc/59543.csv?nocab=1',
        'by':'Municipio',
        'filter': lambda df: df.loc[(df['Municipality code'] != 'Total')],
        'columns': {
            'rename':{'1 persona':'Households with 1 person', '2 personas':'Households with 2 people','3 personas':'Households with 3 people', '4 personas':'Households of 4 people', '5 o más personas':'Households of 5 or more people','Total (tamaño del hogar)': 'Total Households'}
        }
    },
    'Tipo de vivienda (principal o no)': {
        'url': 'https://www.ine.es/jaxi/files/tpx/en/csv_bdsc/59525.csv?nocab=1',
        'by':'Municipio',
        'filter': lambda df: df.loc[(df['Municipality code'] != 'Total')],
        'columns': {
            'rename':{'Total':'Total Homes', 'Vivienda no principal':'Non-main Homes','Vivienda principal':'Main Homes'}
        }
    },
    'Edad en grandes grupos': {
        'url':'https://www.ine.es/jaxi/files/tpx/en/csv_bdsc/55242.csv',
        'by':'Municipio de residencia',
        'columns':{
            'aggregate':{
                'Population': lambda df: df.sum(axis=1,numeric_only=True),
                'Percentage of people between 16 (inclusive) and 64 (inclusive) years': lambda df: df['16-64']/ df['Population'],
                'Percentage of people over 64 years of age': lambda df: df['65 o más']/ df['Population'],
                'Percentage of people under 16 years': lambda df: df['Menos de 16']/ df['Population'],
            }
        },
        'filter':lambda df: df.loc[(df['Municipality code'] != 'Total') &  (df['Nacionalidad (española/extranjera)'] == 'TOTAL')& (df['Sexo'] == 'Ambos sexos')],
    },
    'Sexo': {
        'url':'https://www.ine.es/jaxi/files/tpx/en/csv_bdsc/55242.csv',
        'by':'Municipio de residencia',
        'columns':{
            'aggregate':{
                'Percentage of women': lambda df: df['Mujeres']/ df['Ambos sexos'],
                'Percentage of men': lambda df: df['Hombres']/ df['Ambos sexos'],
            }
        },
        'filter':lambda df: df.loc[(df['Municipality code'] != 'Total') & (df['Edad en grandes grupos'] == 'TOTAL') & (df['Nacionalidad (española/extranjera)'] == 'TOTAL') ],
    },
    'Nacionalidad (española/extranjera)': {
        'url':'https://www.ine.es/jaxi/files/tpx/en/csv_bdsc/55242.csv',
        'by':'Municipio de residencia',
        'columns':{
            'aggregate':{
                'Percentage foreigners': lambda df: df['Extranjera']/ df['TOTAL'],
            }
        },
        'filter':lambda df: df.loc[(df['Municipality code'] != 'Total') & (df['Edad en grandes grupos'] == 'TOTAL') & (df['Sexo'] == 'Ambos sexos') ],
    },
    'Unidades de medida': {
        'url':'https://www.ine.es/jaxi/files/tpx/en/csv_bdsc/55245.csv?nocab=1',
        'by':'Municipio de residencia',
        'filter': lambda df: df.loc[(df['Municipality code'] != 'Total') & (df['Nacionalidad (española/extranjera)'] == 'TOTAL') & (df['Sexo'] == 'Ambos sexos')],
        'columns':{
            'rename':{'Edad media':'Average age'}
        },
    },
    'País de nacimiento (grandes grupos)': {
        'url': 'https://www.ine.es/jaxi/files/tpx/en/csv_bdsc/55243.csv?nocab=1',
        'by':'Municipio de residencia',
        'filter':lambda df: df.loc[(df['Municipality code'] != 'Total') & (df['Sexo'] == 'Ambos sexos') ],
        'columns': {
            'aggregate':{'Percentage of people born abroad': lambda df: 1 - (df['España'] / df['TOTAL'])}
        },
        
    },
    'Nivel de estudios (grado)': {
        'url': 'https://www.ine.es/jaxi/files/tpx/en/csv_bdsc/55249.csv?nocab=1',
        'by':'Municipio de residencia',
        'filter':lambda df: df.loc[(df['Municipality code'] != 'Total') & (df['Nacionalidad (española/extranjera)'] == 'TOTAL') & (df['Sexo'] == 'Ambos sexos') ],
        'columns': {
            'aggregate':{'Percentage of people with higher education (esreal_cneda=08 09 10 11 12) on population aged 16 and over': lambda df: df['Educación Superior']/(df['TOTAL'] - df['No aplicable (menor de 15 años)'])}
        },

    } 
}


def aggregate(df,operations):
    for key in operations:
        df[key] = operations[key](df)
    return df

def rename(df, cols):
    return  df.rename(columns=cols,inplace=True)


operation_dict = {
    'aggregate': aggregate,
    'rename': rename
}

def fetch_data(url,separation=";",type=None):
    r = requests.get(url)
    r.encoding = 'utf-8'
    return pd.read_csv(StringIO(r.text), sep=separation, encoding="utf-8",thousands='.', decimal=',',dtype=type)



def download_censo2021():
    os.makedirs('data/INECensus', exist_ok=True)
    filename = "data/INECensus/census.tsv"

    if not os.path.exists(filename):
        print("Downloading CEnso 2021..")
        # Downloading Censo 2021
        g_df = fetch_data(DATA_CENSO2021,separation=",",type=str)
        g_df['cmun'] = g_df['cpro'] + g_df['cmun']
        column_names = {'cmun':'Municipality code','dist':'District code', 'secc': 'Section code', 't1_1': 'Total People', 't2_1': 'Percentage of women', 't2_2': 'Percentage of men', 't3_1': 'Average age', 't4_1': 'Percentage of people under 16 years', 't4_2': 'Percentage of people between 16 (inclusive) and 64 (inclusive) years', 't4_3': 'Percentage of people over 64 years of age', 't5_1': 'Percentage foreigners', 't6_1': 'Percentage of people born abroad', 't7_1': 'Percentage of people pursuing higher education (escur =08 09 10 11 12 ) of the population aged 16 and over', 't8_1': 'Percentage of people pursuing university studies ( escur = 09 10 11 12) on population aged 16 and over', 't9_1': 'Percentage of people with higher education (esreal_cneda=08 09 10 11 12) on population aged 16 and over', 't10_1': 'Percentage of population unemployment over active population= Unemployed /Active', 't11_1': 'Percentage of employed population over population aged 16 and over =Employed/ Population 16 and +', 't12_1': 'Percentage of active population over population aged 16 and over= Active / Population 16 and +', 't13_1': 'Percentage of disability pensioner population over population aged 16 and over = Disability pensioners / Population 16 and +', 't14_1': 'Percentage of retirement pensioner population over population 16 and over=Retirement pensioners / Population 16 and +', 't15_1': 'Percentage of population in another situation of inactivity over population 16 and over=Population in another situation of inactivity / Population 16 and +', 't16_1' : 'Percentage of student population over population 16 and over = Students / Population 16 and +', 't17_1': 'Percentage of people with single marital status', 't17_2': 'Percentage of people with married marital status', 't17_3': 'Percentage of people with marital status widowed', 't17_4': 'Percentage of people for whom their marital status is not stated', 't17_5': 'Percentage of people with marital status legally separated or divorced', 't18_1': 'Total Homes', 't19_1': 'Main Homes', 't19_2': 'Non-main Homes', 't20_1': 'Owned Homes', 't20_2': 'Rental Homes', 't20_3' : 'Homes in another type of tenure regime', 't21_1': 'Total Households', 't22_1': 'Households with 1 person', 't22_2': 'Households with 2 people', 't22_3': 'Households with 3 people', 't22_4': 'Households of 4 people', 't22_5': 'Households of 5 or more people'}
        g_df.rename(columns=column_names,inplace=True)
        g_df.drop(columns=['ccaa','cpro'],inplace=True)

        # Integrating data sources at municipal level to fill the missing information (1363 rows). This can be done since all the municipalities with missing values were found to have unique districts.
        for x in censo_ingestion_urls:
            data= fetch_data(censo_ingestion_urls[x]['url'])
            data['Municipality code'] = data[censo_ingestion_urls[x]['by']].str[:5]
            data = censo_ingestion_urls[x]['filter'](data)
            data = data.pivot(index=['Municipality code'], columns=x, values='Total').reset_index()
            for op_code, operations in censo_ingestion_urls[x]['columns'].items():
                operation_dict.get(op_code)(data,operations)
            
            # Fill missing values (data ingestion)
            g_df = g_df.set_index('Municipality code').fillna(data.set_index('Municipality code')).reset_index()

        g_df.to_csv(filename,sep="\t", index=False)

    else:
        g_df = pd.read_csv('data/INECensus/census.tsv',sep="\t",dtype={0:str,1:str,2:str})

    municipality = g_df[pd.isna(g_df["District code"]) & pd.isna(g_df["Section code"])]
    municipality = municipality[municipality.columns[municipality.notna().any()]]
    districts = g_df[-pd.isna(g_df["District code"]) & pd.isna(g_df["Section code"])]
    districts = districts[districts.columns[districts.notna().any()]]
    sections = g_df[-pd.isna(g_df["District code"]) & -pd.isna(g_df["Section code"])]
    sections = sections[sections.columns[sections.notna().any()]]

    return ({
        "Municipality": municipality,
        "Districts": districts,
        "Sections": sections
    })

def INEConsumo_electrico():
    # Indicadores de distribución de consumo eléctrico
    os.makedirs('data/INECensus', exist_ok=True)
    filename = "data/INECensus/consumption.tsv"
    if not os.path.exists(filename):
        print("Downloading  electrical consumption...")
        DATA_CONSUMO = "https://www.ine.es/jaxi/files/tpx/en/csv_bd/59532.csv?nocab=1"

        g_df = fetch_data(DATA_CONSUMO, separation="\t")
        g_df['Municipality code'] = g_df['Distritos'].str[:5]
        g_df['District code'] = g_df['Distritos'].str[5:7]
        g_df['Municipality name'] = g_df['Distritos'].str.extract(r"^\d*(.+?)\sdistrito")
        g_df = g_df.pivot_table(index=['Municipality code', 'District code', 'Municipality name'], columns='Percentil', values='Total').reset_index()
        g_df.rename(columns=lambda x: re.sub(r'Percentil (\d+) de consumo eléctrico en kwh', r'Percentile \1 of electricity consumption in kwh', x), inplace = True)

        g_df.to_csv(filename,sep="\t", index=False)

    else:
        g_df = pd.read_csv(filename,sep="\t",dtype={0:str,1:str,2:str})

    municipality = g_df[-pd.isna(g_df["Municipality code"]) & pd.isna(g_df["District code"])]
    municipality = municipality[municipality.columns[municipality.notna().any()]]
    districts = g_df[-pd.isna(g_df["Municipality code"]) & -pd.isna(g_df["District code"])]
    districts = districts[districts.columns[districts.notna().any()]]

    return ({
        "Municipality": municipality,
        "Districts": districts
    })
h = INEConsumo_electrico()
print(h)
