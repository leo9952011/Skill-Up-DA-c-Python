"""
GBfunciones.py: funciones que se llaman desde los dags

## Grupo de Universidades B

# Dev: Aldo Agunin
# Fecha: 15/11/2022
"""
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
# import logging
# import logging.config
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
# ------- DECLARACIONES -----------
#univ = 'UNComahue'
#univ = 'USalvador'

# def configure_logger():
#     logger_name = 'GB' + univ + '_dag_etl'
#     logger_cfg = Path.cwd() / 'plugins' / 'GB_logger.cfg'
#     logging.config.fileConfig(logger_cfg)
#     # Set up logger
#     logger = logging.getLogger(logger_name)
#     return logger

# # def datos_a_csv_prueba(univ, sql_file, csv_file):
#     """
#     Extraccion: solo esta para probar, la que se usa esta en el dag
#     lee datos de la BD, escribe en un csv
#     """
#     logger = configure_logger()
#     logger.info('*** Comenzando Extraccion ***')
    
#     # Leo el .sql
#     sql_consulta = open(sql_file, 'r').read()
#     # Conexion a la base
#     hook = PostgresHook(postgres_conn_id='alkemy_db')
#     conexion = hook.get_conn()
#     df = pd.read_sql(sql_consulta, conexion)
#     # Guardo .csv
#     df.to_csv(csv_file, index=False)
    
#     logger.info('*** Fin Extraccion ***')
#     return

def normalizar_string(columna):
    """ str minusculas, sin espacios extras, ni guiones """
    columna = columna.str.lower().str.replace('_', ' ').str.strip(' -_')
    return columna

def normalizar_genero(columna):
    """ str choice(male, female) """
    columna = columna.str.replace('F', 'female')
    columna = columna.str.replace('M', 'male')
    return columna

def eliminar_abreviaturas(columna):
    """ last_name: elimino abreviaturas """
    abrevs = ['MISS', 'III','PHD', 'DDS', 'DVM', 'MRS', 'II', 'IV',  'MR',  'JR', 'DR', 'MD', '.']
    for abrev in abrevs:
        columna = columna.str.replace(abrev, '')
    return columna

def normalizar_fecha(univ, columna):
    """ convierte fechas a str %Y-%m-%d format """
    if univ == 'UNComahue':
        # las fechas de UNComahue ya vienen en formato %Y-%m-%d
        columna = pd.to_datetime(columna)
    elif univ == 'USalvador':
        # USalvador tiene fechas en formato dd-Mon-yy
        columna = pd.to_datetime(columna).dt.strftime('%Y-%m-%d')
    return columna


def cambiar_siglo(date):
    """
    Para años que estaban en 2 digitos, corrige el prefijo de forma que la edad sea 15 o mayor.
    """
    anio = date[:4]
    if int(anio) > 2005 :
        prefijo = '19'
        fecha = prefijo + date[2:]
    else:
        fecha = date
    return fecha

def reindexardf():
    # de vicio, para dejar las columnas en el orden pedido
    df = df.reindex(columns=[
        'university',
        'career',
        'inscription_date',
        'last_name',
        'first_name',
        'gender',
        'fecha_nacimiento',
        'age',
        'postal_code',
        'location',
        'email',
        ]
    )
    return

def csv_a_txt(univ, in_file, out_file):
    """
    Transformacion: recibe csv, limpia, entrega txt
    """
    
    # Leo el .csv
    # # necesito forzar el tipo de cada columna
    df = pd.read_csv(
        in_file,
        dtype={
            'university': str,
            'career': str,
            'inscription_date': str,
            'last_name': str,
            'first_name': str,
            'gender': str,
            'fecha_nacimiento': str,
            'age': pd.Int64Dtype(),
            'postal_code': str,
            'location': str,
            'email': str,
            },
        index_col=False,
        )
    
    # Normalizacion
    # 1) strings
    # - university: str minusculas, sin espacios extras, ni guiones
    df['university'] = normalizar_string(df['university'])

    # - career: str minusculas, sin espacios extras, ni guiones
    df['career'] = normalizar_string(df['career'])

    # - email: str minusculas, sin espacios extras, ni guiones
    df['email'] =  normalizar_string(df['email'])

    # - last_name: elimino abreviaturas
    df['last_name'] = eliminar_abreviaturas(df['last_name'])

    # - last_name: str minuscula y sin espacios, ni guiones
    df['last_name'] = normalizar_string(df['last_name'])
    
    # separo first_name y last_name
    df[['first_name', 'last_name']] = df['last_name'].str.split(' ', 1, expand=True)

    # - first_name: str minuscula y sin espacios, ni guiones
    # ya quedo
    
    # 2) # - gender: str choice(male, female)
    df['gender'] = normalizar_genero(df['gender'])

    # 3) postal_code y location
    # a) leer la tabla codigos_postales
    loc_path =  Path(__file__).parent.parent / 'assets'
    loc_file =  loc_path / ('codigos_postales.csv')
    df_location = pd.read_csv(
        loc_file, 
        dtype={
            'codigo_postal': str, 
            'localidad': str
            },
        index_col=False
    )

    df_location.rename(
        columns={
            'codigo_postal': 'postal_code',
            'localidad': 'location'
            },
        inplace=True
    )
    # normalizo location
    df_location['location'] = normalizar_string(df_location['location'])

    # para UNComahue tengo postal_code, para USalvador tengo location
    if univ == 'UNComahue':
        # a) postal_code: ya lo tengo
        
        # b) location: la tengo que obtener de la tabla codigos_postales
        # elimino la columna vacia en la tabla de la universidad
        df.drop('location', inplace=True, axis='columns')
 
        # hago un LEFT JOIN de df y df_location
        df = pd.merge(df, df_location, on='postal_code', how='left')

    elif univ == 'USalvador':
        # a) location: ya la tengo
        
        # b) postal_code:  lo tengo que obtener de la tabla codigos_postales
        # print(df_location.duplicated(subset=['location']))
        # tengo localidades, con mas de un codigo, me quedo con el primero
        df_location.drop_duplicates(subset=['location'], keep='first', inplace=True)
                
        # elimino la columna vacia en la tabla de la universidad
        df.drop('postal_code', inplace=True, axis='columns')
        # y normalizo la columna location
        df['location'] = normalizar_string(df['location'])

        # hago un LEFT JOIN de df y df_location
        df = pd.merge(df, df_location, on='location', how='left')
        
    # 4) fechas y edad
    # - inscription_date: str %Y-%m-%d format (yyyy-mm-dd)
    # para UNComahue no es necesaria, ya viene en este formato
    # pero USalvador viene en %d-%b-%y  (dd-Mon-yy)
 
    df['inscription_date'] = normalizar_fecha(univ, df['inscription_date'])
    df['fecha_nacimiento'] = normalizar_fecha(univ, df['fecha_nacimiento'])
    #df['inscription_date'] = df['inscription_date'].astype({"inscription_date": str})
    # calculo la edad
    # elimino la columna vacia en la tabla de la universidad
    df.drop('age', inplace=True, axis='columns')
    doi = pd.to_datetime(df['inscription_date'])
    dob = pd.to_datetime(df['fecha_nacimiento'])
    
    # calculo la edad al momento de la inscripcion
    df['_age_ins_0'] = (doi-dob) / np.timedelta64(1, 'Y')
    age_0_mean = df['_age_ins_0'].mean()
    
    if univ == 'UNComahue':
        # alternativa 1: en caso de que edad < 15 la reemplazo por NaN.
        df['_age_ins_1'] = df['_age_ins_0']
        df.loc[df['_age_ins_1'] < 15, ['_age_ins_1']]= np.NaN
        age_1_mean = df['_age_ins_1'].mean()
            
        # alternativa 2: en caso de que edad < 15 la reemplazo por la media.
        df['_age_ins_2'] = df['_age_ins_0']
        df.loc[df['_age_ins_2'] < 15, ['_age_ins_2']] = age_0_mean
        age_2_mean = df['_age_ins_2'].mean()
                    
        # me quedo con la 2 aunque no hay una diferencia significativa
        # y convierto a int
        df['age'] = df['_age_ins_2'].astype(int)

    elif univ == 'USalvador':
        # USalvador tiene fechas en formato dd-Mon-yy
        # y en la conversion a algunos años de nacimiento se les asigno un prefijo 20
        # con lo que la edad al momento de inscribirse es < de 15.
        # A esos se les cambiara el prefijo 20 por 19.

        # alternativa 1: en caso de que edad < 15 reemplazo las 2 primeras cifras del año.
        # y recalculo la edad
        df['_age_ins_1'] = df['_age_ins_0'].astype(int)
        #df.loc[df['_age_ins_1'] < 15, '_age_ins_1'] = df['_age_ins_1'] + 100
        df['fecha_nacimiento'] = df['fecha_nacimiento'].map(cambiar_siglo)
        dob = pd.to_datetime(df['fecha_nacimiento'])
        df['_age_ins_1'] = (doi-dob) / np.timedelta64(1, 'Y')
        df['_age_ins_2'] = df['_age_ins_1'].astype(int)
        df['age'] = df['_age_ins_2'].astype(int)
        age_1_mean = df['_age_ins_1'].mean()
    
    #reindexardf()

    # txt AUXILIAR para guardar las columnas no requeridas 
    # que uso en el proceso de limpieza de los datos extraidos
    # Ubicacion del .txt AUXILIAR
    #aux_file = Path(__file__).parent.parent / 'notebooks' / ('GB' + univ + '_AUXILIAR.txt')
    aux_path = Path(__file__).parent.parent / 'notebooks'
    aux_file = aux_path / ('GB' + univ + '_AUXILIAR.txt')
        
    # Escribo el .txt AUXILIAR
    df.to_csv(aux_file)
    
    # Elimino columnas no requeridas
    df.drop('fecha_nacimiento', axis=1, inplace=True)
    df.drop('_age_ins_0', axis=1, inplace=True)
    df.drop('_age_ins_1', axis=1, inplace=True)
    df.drop('_age_ins_2', axis=1, inplace=True)

    # Escribo el .txt FINAL
    #df.to_csv(in_file, index=False)
    df.to_csv(out_file, index=False)
    return
#------------------------------------------

if __name__ == '__main__':
    #datos_a_csv(univ, sql_file, csv_file)
    csv_a_txt(univ, in_file, out_file)
