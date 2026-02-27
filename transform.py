import pandas as pd
from pandas import json_normalize
from bson import ObjectId
import numpy as np

#convert mongodb object to string
def convert_mongo_types(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: str(x) if isinstance(x, ObjectId) else x
        )
    return df


#detect list columns
def get_list_columns(df: pd.DataFrame) -> list[str]:
    return [
        col for col in df.columns
        if df[col].apply(lambda x: isinstance(x, list)).any()
    ]


#explode list
def explode_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df = df.explode(column).reset_index(drop=True)
    df[column] = df[column].fillna({})

    exploded_cols = json_normalize(df[column], sep="_")
    exploded_cols.index = df.index

    df = pd.concat(
        [df.drop(columns=[column]), exploded_cols],
        axis=1
    )

    return df


#flattening 
def flatten_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    records = df.to_dict(orient="records")
    return json_normalize(records, sep="_")

#handle nulls
def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("")
        elif pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(0)
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].fillna(pd.NaT)
    return df

#replace .of column to _
def sanitize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.replace(".", "_", regex=False)
        .str.lower()
    )
    return df

#main transform function
def transform(df: pd.DataFrame) -> pd.DataFrame:

    # Convert Mongo types
    df = convert_mongo_types(df)

    # Iteratively explode list columns
    while True:
        list_cols = get_list_columns(df)
        if not list_cols:
            break
        for col in list_cols:
            df = explode_column(df, col)

    # Flatten remaining dicts
    df = flatten_dataframe(df)

    # Handle nulls
    df = handle_nulls(df)
    
    #Sanitise column name
    df = sanitize_column_names(df)

    return df