import pandas as pd
from io import StringIO
import os
import requests
import re
from bs4 import BeautifulSoup
from tqdm import tqdm
import sys
import numpy as np


def get_links_that_contain(regexp, html):

    soup = BeautifulSoup(html, "html.parser")
    links = []
    for link in soup.findAll('a', attrs={'href': re.compile(regexp)}):
        links.append(link.get('href'))

    return (links)


def is_number(s):
    if s is None or isinstance(s, float) and s != s:  # Check for None or NaN
        return False
    try:
        float(s)
        return True
    except ValueError:
        return False

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

def INERentalDistributionAtlas(municipality_code=None):

    os.makedirs('data/INERentalDistributionAtlas', exist_ok=True)
    filename = "data/INERentalDistributionAtlas/df.tsv"

    if not os.path.exists(filename):

        print("Reading the metadata to gather the INE Rental Distribution Atlas", file=sys.stdout)
        req = requests.get('https://www.ine.es/dynt3/inebase/en/index.htm?padre=7132',
                           headers={'User-Agent': 'Mozilla/5.0'})
        urls = get_links_that_contain("capsel", req.text)
        g_ids = []

        for url in urls:
            req = requests.get(f'https://www.ine.es/dynt3/inebase/en/index.htm{url}',
                               headers={'User-Agent': 'Mozilla/5.0'})
            x = [re.search(r'(t=)(?P<x>\w+)(&L)', link).group('x') for link in
                 get_links_that_contain("Export", req.text)]
        
            g_ids.append(x)

        g_urls = [[f"https://www.ine.es/jaxiT3/files/t/en/csv_bd/{id}.csv?nocab=1" for id in ids] for ids in g_ids]
        g_df = pd.DataFrame()

        for urls in tqdm(g_urls, desc="Downloading files from INE by provinces..."):
            df = pd.DataFrame()
            for url in urls:
                r = requests.get(url)
                r.encoding = 'utf-8'
                df_ = pd.read_csv(StringIO(r.text), sep="\t", encoding="utf-8")
                df_["Municipality name"] = df_["Municipalities"].astype(str).str[6:]
                df_["Municipality code"] = df_["Municipalities"].astype(str).str[:5]
                df_["District code"] = df_["Distritos"].astype(str).str[5:7]
                df_["Section code"] = df_["Secciones"].astype(str).str[7:10]
                df_["Year"] = df_["Periodo"]
                df_["Value"] = df_["Total"]
                df_["Value"] = pd.to_numeric(df_["Total"].astype(str).str.replace('.', '').str.replace(',', '.'), errors="coerce")
                df_ = df_.sort_values(by='Value', na_position='last')
                df_ = df_.drop(columns=["Municipalities","Distritos","Secciones","Periodo","Total"])
                df_ = df_.drop_duplicates([col for col in df_.columns if col not in 'Value'])
                if "Nationality" in df_.columns:
                    df_["Nationality"] = df_["Nationality"].replace({"Extranjera":"Foreign"})
                if "Age ranges" in df_.columns:
                    df_["Age"] = df_["Age ranges"].replace({
                        "From 18 to 64 years old": "18-64",
                        "65 and over": ">64",
                        "Less than 18 years": "<18"
                    })
                    df_ = df_.drop(columns=["Age ranges"])
                df_ = pd.pivot(df_,
                         index=[col for col in df_.columns if col in
                                    ['Municipality name', 'Municipality code', 'District code', 'Section code', 'Year']],
                         columns= [col for col in df_.columns if col not in
                                    ['Municipality name', 'Municipality code', 'District code', 'Section code', 'Year', 'Value']],
                         values = "Value")
                df_ = df_.reset_index()
                df_.rename(columns={
                    "Tamaño medio del hogar": "Average size of households",
                    "Fuente de ingreso: otras prestaciones": "Source:Other benefits ~ Average per person gross income",
                    "Fuente de ingreso: otros ingresos": "Source:Other incomes ~ Average per person gross income",
                    "Fuente de ingreso: pensiones": "Source:Pension ~ Average per person gross income",
                    "Fuente de ingreso: prestaciones por desempleo": "Source:Unemployment benefits ~ Average per person gross income",
                    "Fuente de ingreso: salario": "Source:Salary ~ Average per person gross income",
                    "Porcentaje de hogares unipersonales": "Percentage of single-person households"
                })
                if isinstance(df_.columns, pd.MultiIndex):
                    subgroups = ["Nationality","Age","Sex"]
                    allcols = df_.columns.names
                    maincol = [col for col in allcols if col in subgroups]
                    maincol.extend([col for col in allcols if col not in subgroups])
                    df_.columns = df_.columns.reorder_levels(order=maincol)
                    df_.columns = [" ~ ".join([f"{level}:{value}" if level in subgroups else f"{value}"
                                               for level, value in zip(df_.columns.names, cols)])
                                   if cols[1]!='' else cols[0] for cols in df_.columns.to_flat_index()]
                df_.columns = [cols.strip() for cols in df_.columns]

                df_.columns = [re.sub(" ~ Sex:Total","", cols) for cols in df_.columns]

                if len(df) == 0:
                    df = df_
                else:
                    merge_on = ['Municipality name', 'Municipality code', 'District code', 'Section code', 'Year']
                    df = pd.merge(
                        df,
                        df_[[col for col in df_.columns if ((col not in df.columns) or (col in merge_on))]],
                        on = merge_on)
                del(df_)

            g_df = pd.concat([g_df,df])
            del(df)

        g_df.to_csv(filename, sep="\t", index=False)

    else:
        g_df = pd.read_csv(filename, sep="\t", dtype={0:'str',1:'str',2:'str',3:'str'})

    if municipality_code is not None:
        if type(municipality_code) == str:
            g_df = g_df[(g_df["Municipality code"] == municipality_code).values]
        elif type(municipality_code) == list:
            g_df = g_df[g_df["Municipality code"].isin(municipality_code)]

    g_df["Country code"] = "ES"
    g_df["Province code"] = g_df["Municipality code"].str[:2]
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

def INEPopulationAnualCensus():

    os.makedirs('data/INEPopulationAnualCensus', exist_ok=True)
    filename = "data/INEPopulationAnualCensus/df.tsv"

    if not os.path.exists(filename):

        print("Reading the metadata to gather the INE population and household anual census", file=sys.stdout)
        base_link = "https://www.ine.es/dynt3/inebase/es/index.htm"
        sections_link = "?padre=10358"
        req = requests.get(f"{base_link}{sections_link}", headers={'User-Agent': 'Chrome/51.0.2704.103'})
        sections_link = get_links_that_contain("capsel", req.text)[-1]
        req = requests.get(f"{base_link}{sections_link}",headers={'User-Agent': 'Chrome/51.0.2704.103'})
        urls = get_links_that_contain("capsel", req.text)
        urls = urls[urls.index(sections_link)+1:]
        g_ids = []

        for url in urls:
            req = requests.get(f'{base_link}{url}',headers={'User-Agent': 'Mozilla/5'})
            x = [re.search(r'(tpx=)(?P<x>\w+)(&L)', link).group('x') for link in
                 get_links_that_contain("Export", req.text)]
            g_ids.append(x)

        g_urls = [[f"https://www.ine.es/jaxi/files/tpx/es/csv_bd/{id}.csv?nocab=1" for id in ids] for ids in g_ids]
        g_df = pd.DataFrame()
        year = 2021

        for urls in tqdm(g_urls, desc="Downloading files from INE by year..."):

            df = pd.DataFrame()

            for url in urls:

                r = requests.get(url)
                r.encoding = 'utf-8'
                df_ = pd.read_csv(StringIO(r.text), sep="\t", encoding="utf-8", dtype={3:'str',6:'str'})

                cols = df_.columns
                if all([col in cols for col in ['Total Nacional', 'Provincias', 'Municipios', 'Secciones']]):
                    df_['Provincias'] = df_['Provincias'].fillna(df_['Total Nacional'])
                    df_['Municipios'] = df_['Municipios'].fillna(df_['Provincias'])
                    df_['Secciones'] = df_['Secciones'].fillna(df_['Municipios'])
                    df_ = df_.drop(columns = ['Total Nacional', 'Provincias', 'Municipios'])
                    cols = df_.columns

                allcols = {
                    "Sección censal": "Location",
                    "Secciones": "Location",
                    "Sexo": "Sex",
                    "Lugar de nacimiento (España/extranjero)": "Place of birth",
                    "Nacionalidad (española/extranjera)": "Nationality",
                    "Relación entre lugar de nacimiento y lugar de residencia": "Detailed place of birth",
                    "Total": "Value",
                    "Edad (grupos quinquenales)": "Age"
                }

                df_ = df_.rename(columns={col:allcols[col] for col in cols})
                cols = df_.columns

                if "Sex" in cols:
                    df_["Sex"] = df_["Sex"].replace({
                        "Hombre": "Males",
                        "Mujer": "Females",
                        "Ambos sexos": "Total"
                    })

                if "Place of birth" in cols:
                    df_["Place of birth"] = df_["Place of birth"].replace({
                        "España": "Spain",
                        "Extranjero": "Foreign country"
                    })

                if "Nationality" in cols:
                    df_["Nationality"] = df_["Nationality"].replace({
                        "Española": "Spanish",
                        "Extranjera": "Foreign"
                    })

                if "Detailed place of birth" in cols:
                    df_["Detailed place of birth"] = df_["Detailed place of birth"].replace({
                        "Mismo municipio": "Born in the same municipality",
                        "Distinto municipio de la misma provincia": "Born in a municipality of the same province",
                        "Distinta provincia de la misma comunidad": "Born in a municipality of the same autonomous community",
                        "Distinta comunidad": "Born in a municipality of another autonomous community",
                        "Nacido en el extranjero": "Born in another country"
                    })

                if "Age" in cols:
                    df_["Age"] = df_["Age"].str.replace("De ","").\
                        str.replace(" años","").\
                        str.replace(" a ","-").\
                        str.replace(" y más","").\
                        str.replace("100",">99")

                df_["Year"] = year
                df_["Value name"] = "Population"
                df_["Value"] = pd.to_numeric(df_["Value"].astype(str).str.replace(',', '').str.replace('.', ''), errors="coerce")

                df_ = pd.pivot(df_,
                               index=[col for col in df_.columns if col in
                                      ['Location', 'Year']],
                               columns=[col for col in df_.columns if col not in
                                        ['Location', 'Year', 'Value']],
                               values="Value")

                subgroups = ["Nationality", "Age", "Sex", "Place of birth", "Detailed place of birth"]
                if isinstance(df_.columns, pd.MultiIndex):
                    allcols = df_.columns.names
                    maincol = [col for col in allcols if col not in subgroups]
                    maincol.extend([col for col in allcols if col in subgroups])
                    df_.columns = df_.columns.reorder_levels(order=maincol)
                    df_.columns = [" ~ ".join([f"{level}:{value}" if level in subgroups else f"{value}"
                                               for level, value in zip(df_.columns.names, cols)])
                                   if cols[1]!='' else cols[0] for cols in df_.columns.to_flat_index()]
                df_.columns = [cols.strip() for cols in df_.columns]

                for subgroup in subgroups:
                    df_.columns = [re.sub(f" ~ {subgroup}:Total","", cols) for cols in df_.columns]

                df_ = df_.reset_index()

                if len(df)>0:
                    df = pd.merge(df,df_[[col for col in df_.columns if col not in df.columns or col=="Location"]],
                                  on="Location")
                else:
                    df = df_

            year = year + 1
            if len(g_df)>0:
                g_df = pd.concat([g_df,df[g_df.columns]])
            else:
                g_df = df

        g_df["Country code"] = "ES"
        g_df["Location"] = g_df["Location"].replace({"Total Nacional":""})
        g_df["Province code"] = np.where(g_df["Location"].str[0].apply(is_number), g_df["Location"].str[0:2], np.nan)
        g_df["Municipality code"] = np.where(g_df["Location"].str[2].apply(is_number), g_df["Location"].str[0:5], np.nan)
        g_df["District code"] = np.where(g_df["Location"].str[5].apply(is_number), g_df["Location"].str[5:7], np.nan)
        g_df["Section code"] = np.where(g_df["Location"].str[7].apply(is_number), g_df["Location"].str[7:10], np.nan)
        g_df = g_df.drop(columns=["Location"])

        district = g_df.groupby(["Country code", "Province code", "Municipality code", "District code", "Year"])[
            [col for col in g_df.columns if col not in ["Country code", "Province code", "Municipality code", "District code", "Year","Section code"]]
            ].sum()
        district["Section code"] = np.nan
        district = district.set_index("Section code", append=True)
        district = district.reset_index()
        g_df = pd.concat([g_df[district.columns], district])

        g_df.to_csv(filename,sep="\t", index=False)

    else:
        g_df = pd.read_csv(filename, sep="\t")

    # national = g_df[pd.isna(g_df["Province code"]) & pd.isna(g_df["Municipality code"]) & pd.isna(g_df["District code"]) & pd.isna(g_df["Section code"])]
    # national = national[national.columns[national.notna().any()]]
    # province = g_df[-pd.isna(g_df["Province code"]) & pd.isna(g_df["Municipality code"]) & pd.isna(g_df["District code"]) & pd.isna(g_df["Section code"])]
    # province = province[province.columns[province.notna().any()]]
    municipality = g_df[-pd.isna(g_df["Province code"]) & -pd.isna(g_df["Municipality code"]) & pd.isna(g_df["District code"]) & pd.isna(g_df["Section code"])]
    municipality = municipality[municipality.columns[municipality.notna().any()]]
    districts = g_df[-pd.isna(g_df["Province code"]) & -pd.isna(g_df["Municipality code"]) & -pd.isna(g_df["District code"]) & pd.isna(g_df["Section code"])]
    districts = districts[districts.columns[districts.notna().any()]]
    sections = g_df[-pd.isna(g_df["Province code"]) & -pd.isna(g_df["Municipality code"]) & -pd.isna(g_df["District code"]) & -pd.isna(g_df["Section code"])]
    sections = sections[sections.columns[sections.notna().any()]]

    return ({
        # "National": national,
        # "Province": province,
        "Municipality": municipality,
        "Districts": districts,
        "Sections": sections
    })

INERentalDistributionAtlas('08031')