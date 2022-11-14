from datetime import datetime, date
from pathlib import Path
import pandas as pd

# BASE_DIR = Path("").parent.parent
# UCine_path = Path("files/GHUNCine_select.csv")
# UBsAs_path = Path("files/GHUNBuenosAires_select.csv")

# cine_path = BASE_DIR / UCine_path
# bs_path = BASE_DIR / UBsAs_path


# cp_df = pd.read_csv(BASE_DIR / "assets/codigos_postales.csv")

# Elimina los registro duplicados y toma el primero como valido.
# cp_df.drop_duplicates(subset=["localidad"], keep="first", inplace=True)

# Transformacion de fechas.
def date_format(fecha):
    format = "%y-%b-%d"
    fecha = datetime.strptime(fecha, format)
    return fecha.date()


def transform_df(input_path, output_path):

    df = pd.read_csv(input_path)

    # Lista de columnas a transformar a string
    list_str = ["university", "career", "first_name", "last_name", "location", "email"]

    # Transformacion de las listas a string
    for column in list_str:
        df[column] = df[column].astype("string").str.replace("-", " ").str.lower()

    # Transformacion de gender
    df["gender"] = (
        df["gender"]
        .astype("string")
        .str.lower()
        .replace({"f": "female", "m": "male"})
        .astype("category")
    )

    try:
        df["inscription_date"] = df["inscription_date"].map(
            lambda date: date_format(date)
        )
        df["inscription_date"] = pd.to_datetime(df["inscription_date"], yearfirst=True)
        # Poner to_datetime
    except:
        df["inscription_date"] = pd.to_datetime(df["inscription_date"], dayfirst=True)

    # Birth_date: (para calcular las edades)
    # Criterio utilizado en fechas con formato corto:
    # si es <= 07 le ponemos 20 y sino 19.
    # En el caso con fechas que contengan yyyy solo se formatea a: %Y-%m-%d

    # cambiar por un .apply()
    df["birth_date"] = df["birth_date"].map(
        lambda date: date
        if len(date) > 9
        else "19" + date
        if int(date[0:2]) >= 7
        else "20" + date
    )
    try:
        df["birth_date"] = pd.to_datetime(df["birth_date"], dayfirst=True)
    except:
        df["birth_date"]

    # calculo de edad:
    df["age"] = (
        df["inscription_date"] - df["birth_date"]
    ).dt.days  # devuelve una serie con los dias en formato int64
    df["age"] = df["age"] / 365  # transformado en año diviendo por el año
    df["age"] = (
        df["age"].map(lambda age: round(age)).astype(int)
    )  # convirtiendo a entero

    # Postal_code y location (Según corresponda)
    if df["location"].isnull().values.any():
        pass
        # llenar location
    else:
        pass
        # llenar postal_code

    df.to_csv(output_path, header=True, index=None, sep=" ", mode="w")
