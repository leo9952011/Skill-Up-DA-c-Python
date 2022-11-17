import pandas as pd
from datetime import datetime
from datetime import date
from pathlib import Path


def transform_Palermo(csvpath, txtpath):

    CUR_DIR = Path(__file__).resolve().parent
    PAR_DIR = CUR_DIR.parent

    df_palermo = pd.read_csv(csvpath/'GCUPalermo_select.csv')

    df_palermo = df_palermo.rename(
        columns={'careers': 'career'})

    df_cp = pd.read_csv (PAR_DIR/'assets/codigos_postales.csv')

    df_palermo['birth_dates'] = df_palermo['birth_dates'].map(
        lambda x: x[:-2] + '19' + x[-2:] if int(x[-2:]) > 7 else x[:-2] + '20' + x[-2:])

    def edad(nac):
        nac = datetime.strptime(nac, "%d/%b/%Y").date()
        today = date.today()
        age = today.year - nac.year - ((today.month, today.day) < (nac.month, nac.day))
        return age



    edades = df_palermo['birth_dates'].apply(edad)
    generos = df_palermo['gender'].map (lambda x: 'female' if x == 'f' else 'male').astype ('category')
    inscripcion = df_palermo['inscription_date'].map (lambda x: datetime.strptime (x, "%d/%b/%y").strftime ('%Y/%m/%d'))
    universidades = df_palermo['university'].map(lambda x: x.replace('_', ' ').replace(' ', '', 1))
    carreras = df_palermo['career'].map(lambda x: x.replace('_', ' ')[::-1].replace(' ', '', 1)[::-1]
                                        if x[-1] == '_' else x.replace('_', ' '))
    def get_name():
        nombre = []
        apellido = []
        count = -1
        names1 = df_palermo['last_name']
        names = df_palermo['last_name'].map(lambda x: x.split('_'))
        for name in names:
            count += 1
            if len(name) == 2:
                nom = name[0]
                nombre.append(nom)
                ape = name[1]
                apellido.append(ape)
            else:
                nombre.append(None)
                apellido.append(names1[count])

        nombre = pd.Series(nombre)
        apellido = pd.Series(apellido)
        return nombre, apellido

    def get_loc():
        codigos = []
        for cp in df_cp['codigo_postal']:
            codigos.append (cp)
        df_cp['codigo_postal'] = pd.Series (codigos)
        mer = df_palermo.merge (df_cp, left_on='postal_code', right_on='codigo_postal')
        localidades = mer['localidad']
        return localidades

    def change_df():
        df_palermo['first_name'], df_palermo['last_name'] = get_name ()
        df_palermo['age'] = edades
        df_palermo['gender'] = generos
        df_palermo['inscription_date'] = inscripcion
        df_palermo['university'] = universidades
        df_palermo['career'] = carreras
        df_palermo['location'] = get_loc().map(lambda x: x.lower())

    change_df()
    df_palermo = df_palermo.drop(
        columns={'Unnamed: 0', 'birth_dates'})

    df_palermo.to_csv (txtpath/'GCUpalermo_process.txt')
    return df_palermo

def transform_Jujuy(csvpath, txtpath):

    CUR_DIR = Path(__file__).resolve().parent
    PAR_DIR = CUR_DIR.parent
    df_jujuy = pd.read_csv(csvpath/'GCUNjujuy_select.csv')

    df_cp = pd.read_csv(PAR_DIR/'assets/codigos_postales.csv')
    def edad(nac):
        nac = datetime.strptime(nac, "%Y/%m/%d").date()
        today = date.today()
        age = today.year - nac.year - ((today.month, today.day) < (nac.month, nac.day))
        return age

    def get_name():
        nombre = []
        apellido = []
        count = -1
        names1 = df_jujuy['last_name']
        names = df_jujuy['last_name'].map(lambda x: x.split(' '))
        for name in names:
            count += 1
            if len(name) == 2:
                nom = name[0]
                nombre.append(nom)
                ape = name[1]
                apellido.append(ape)
            else:
                nombre.append(None)
                apellido.append(names1[count])

        nombre = pd.Series(nombre)
        apellido = pd.Series(apellido)
        return nombre, apellido

    def get_cp():
        localidades = []
        for loc in df_cp['localidad']:
            localidades.append (loc.lower ())
        df_cp['localidad'] = pd.Series (localidades)
        mer = df_jujuy.merge (df_cp, left_on='location', right_on='localidad')
        postal_code = mer['codigo_postal']
        return postal_code

    def delele_corrupted_age(age, mean):
        if age < 15:
            age = int (round (mean))
        return age


    edades = df_jujuy['birth_date'].apply(edad)
    edades = edades.apply(lambda x: delele_corrupted_age(x, mean=edades.mean()))
    generos = df_jujuy['gender'].map (lambda x: 'female' if x == 'f' else 'male').astype ('category')
    codigo_postal = get_cp()

    def change_df():
        df_jujuy['first_name'], df_jujuy['last_name'] = get_name()
        df_jujuy['age'] = edades
        df_jujuy['gender'] = generos
        df_jujuy['postal_code'] = codigo_postal

    change_df()
    df_jujuy = df_jujuy.drop(
        columns={'Unnamed: 0', 'birth_date'})

    df_jujuy.to_csv (txtpath/'GCUNjujuy_process.txt')
    return df_jujuy