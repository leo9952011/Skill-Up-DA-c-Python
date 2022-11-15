"""Transformation function for the universities of group G."""


from pathlib import Path

import numpy as np
import pandas as pd


def calculate_age(row):
    """Calculate age based on birth_date. If age is lower than 15, set to null."""

    age = row['inscription_date'].year - row['birth_date'].year
    row['age'] = np.nan if age < 15 else age
    return row


def complete_age(row, mean_age):
    """Complete null values with the mean of ages."""

    if pd.isna(row['age']):
        row['age'] = mean_age
    return row


def complete_year(date):
    """For 2-digit years only, complete so the age of the person is 15 or higher."""

    year = date[:2]
    full_date = '20' if int(year) <= 7 else '19'
    full_date = full_date + year + date[2:]

    return full_date


def basic_normalization(text):
    """Strip extra spaces, dashes and make lowercase."""
    return text.strip().lower().replace('-', ' ')


def minimal_normalization(text):
    """Strip extra spaces and make lowercase."""
    return text.strip().lower()


def transform_dataset(input_path, output_path):
    """Read csv files, apply transformations and save as txt."""
    df = pd.read_csv(input_path, encoding='utf8', index_col=0)

    # university, career, last_name: eliminar espacios extra, guiones, pasar a minúscula
    df['university'] = df['university'].map(basic_normalization)
    df['career'] = df['career'].map(basic_normalization)
    df['last_name'] = df['last_name'].map(basic_normalization)

    # email: eliminar espacios extra, pasar a minúscula
    df['email'] = df['email'].map(minimal_normalization)

    # gender: choice(male, female)
    gender_values = sorted(list(df['gender'].unique()))
    df['gender'].replace(gender_values, ['female', 'male'], inplace=True)
    df['gender'] = df['gender'].astype('category')

    # check format for inscription_date 
    first_part = df.loc[0,'inscription_date'].split('-')[0]
    last_part = df.loc[0,'inscription_date'].split('-')[2]
    # first part will be considered year only if the last part is not 4 digits long.
    # in that case, complete 2-digit year to 4-digit and set that date_format
    if len(first_part) == 2 and len(last_part) != 4:
        date_format = '%y-%b-%d'
    else:
        date_format = '%d-%m-%Y'
    # inscription_date: pasar a formato %Y-%m-%d
    df['inscription_date'] = pd.to_datetime(df['inscription_date'], format=date_format)

    # birth_date chequear si el año tiene 2 o 4 dígitos
    first_part = df.loc[0,'birth_date'].split('-')[0]
    last_part = df.loc[0,'birth_date'].split('-')[2]
    # first part will be considered year only if the last part is not 4 digits long.
    # in that case, complete 2-digit year to 4-digit and set that date_format
    if len(first_part) == 2 and len(last_part) != 4:
        df['birth_date'] = df['birth_date'].map(complete_year)
        date_format = '%Y-%b-%d'
    # birth_date: pasar a formato %Y-%m-%d
    df['birth_date'] = pd.to_datetime(df['birth_date'], format=date_format)

    # age: calcular en base a birth_date, tipo Int64 para poder colocar nulos
    #df['age'] = df['age'].astype('Int64') #TODO verificar porque se pasan a float igual...
    df = df.apply(calculate_age, axis='columns')
    mean_age = round(df['age'].mean())
    df = df.apply(complete_age, axis='columns', mean_age=mean_age)
    df['age'] = df['age'].astype('int64')
    df.drop('birth_date', inplace=True, axis='columns')
    
    # obtener location o postal_code según lo que falte
    # location_1: cargo datos auxiliares, mapeo nombres de columnas y paso a lowercase
    local_basepath = Path(__file__).resolve().parent.parent
    csv_location_path = local_basepath / 'assets/codigos_postales.csv'
    df_location = pd.read_csv(csv_location_path, encoding='utf8')
    df_location.drop_duplicates(subset=['localidad'], keep='first', inplace=True)
    df_location.rename(columns={'codigo_postal': 'postal_code', 'localidad': 'location'},
                        inplace=True)
    df_location['location'] = df_location['location'].map(lambda x: x.lower())
    # find missing column: location or postal_code is null
    if df['location'].count() == 0:
        missing_col = 'location'
        present_column = 'postal_code'
    else:
        missing_col = 'postal_code'
        present_column = 'location'
        # como tengo location, pasar a lower, sin spaces ni dashes
        df['location'] = df['location'].map(basic_normalization)
    # elimino la columna nula antes de hacer el merge
    df.drop(missing_col, inplace=True, axis='columns')
    df = pd.merge(df, df_location, how='left', on=present_column)

    # guardo el dataset transformado en un txt
    df.to_csv(output_path, encoding='utf8', sep=',')


if __name__ == '__main__':

    transform_dataset(input_path = 'files/GGFLCienciasSociales_select.csv',
                        output_path = 'datasets/GGFLCienciasSociales_process.txt',)
                        #date_format='%d-%m-%Y')

    transform_dataset(input_path = 'files/GGUKennedy_select.csv',
                        output_path = 'datasets/GGUKennedy_process.txt',)
                        #date_format='%y-%b-%d')