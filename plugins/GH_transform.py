from datetime import datetime, date
from pathlib import Path
import pandas as pd


def str_normalizer(text):
    """Elimina espacios de más, reemplaza los - por " " y cambia el texto a minuscula."""
    text = text.replace("-", " ").lower().strip()
    return text


def inscription_date_normalizer(date):
    """Convierte los datos ingresados al formato de fecha requerido.

    Args:
        date (str): Valor de la columna "inscription_date"

    Returns:
        (datetime): %Y-%m-%d
    """
    try:
        fecha = datetime.strptime(date, "%d-%m-%Y")
    except ValueError:
        fecha = datetime.strptime(date, "%y-%b-%d")

    return fecha


def birth_date_normalizer(date):
    """Convierte los datos ingresados al formato de fecha requerido.

    Args:
        date (str): Valor de la columna "birth_date"

    Returns:
        (datetime): %Y-%m-%d
    """
    try:
        fecha = datetime.strptime(date, "%d-%m-%Y")
    except ValueError:
        # Criterio utilizado en fechas con formato corto:
        # si el año <= 07 le ponemos 20 y sino 19.
        year = int(date[:2])

        if year <= 7:
            date = "20" + date
        else:
            date = "19" + date

        fecha = datetime.strptime(date, "%Y-%b-%d")

    return fecha


def age_normalize(age):
    """Convierte los dias de vida de cada persona a edad en años."""
    return int(round(age / 365))


def delele_corrupted_age(age, mean):
    """Cambia los datos corrupts por el promedio.
    Args:
        age (_type_): Valor correspondiente a la columna age.
        mean (float): Promedio de la columna "age".

    Returns:
        (int): Age.
    """
    if age < 15:
        age = int(round(mean))
    return age


def transform_df(input_path, output_path):
    """Se encarga de ajustar los datos a los requerimientos.

    Args:
        input_path (Path): Path del archivo .csv
        output_path (Path): Path del archivo de salida .txt
    """

    BASE_FILE_DIR = Path(__file__).parent.parent

    df = pd.read_csv(input_path)

    # Dataframe de location y postal_code.
    cp_df = pd.read_csv(BASE_FILE_DIR / "assets/codigos_postales.csv")
    cp_df.drop_duplicates(subset=["localidad"], keep="first", inplace=True)
    cp_df.rename(
        columns={"codigo_postal": "postal_code", "localidad": "location"}, inplace=True
    )
    cp_df["location"] = cp_df["location"].apply(str_normalizer)

    # Lista de columnas a transformar a string
    list_str = ["university", "career", "last_name", "email"]

    for column in list_str:
        # Transformacion de las listas a string
        df[column] = df[column].apply(str_normalizer).astype("string")

    # Transformacion de gender
    df["gender"] = (
        df["gender"].astype("string").str.lower().replace({"f": "female", "m": "male"})
    )
    df["gender"] = df["gender"].map(lambda x: x.lower())
    df["gender"] = df["gender"].astype("category")

    # Transformacion de fechas
    df["inscription_date"] = df["inscription_date"].apply(inscription_date_normalizer)
    df["birth_date"] = df["birth_date"].apply(birth_date_normalizer)

    # calculo de edad:
    df["age"] = (df["inscription_date"] - df["birth_date"]).dt.days
    df["age"] = df["age"].apply(age_normalize)
    df["age"] = df["age"].apply(
        lambda x: delele_corrupted_age(x, mean=df["age"].mean())
    )

    # Postal_code y location (Según corresponda)
    if df["location"].count() == 0:

        missing_col = "location"
        present_column = "postal_code"
    else:

        missing_col = "postal_code"
        present_column = "location"
        df["location"] = df["location"].apply(str_normalizer)

    df.drop(missing_col, inplace=True, axis="columns")
    df = pd.merge(df, cp_df, how="left", on=present_column)

    # Se convierten a string.
    df["location"] = df["location"].astype("string")
    df["postal_code"] = df["postal_code"].astype("string")

    df.to_csv(output_path, header=True, index=None, sep=" ", mode="w")

    return df
