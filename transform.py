import pandas as pd
from pandas import json_normalize
from bson import ObjectId
import numpy as np
import hashlib

# Convert MongoDB ObjectId to string
def convert_mongo_types(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: str(x) if isinstance(x, ObjectId) else x
        )
    return df


# Detect list columns
def get_list_columns(df: pd.DataFrame) -> list[str]:
    return [
        col for col in df.columns
        if df[col].apply(lambda x: isinstance(x, list)).any()
    ]


# Explode list columns and track array position
from pandas import json_normalize
import pandas as pd
import numpy as np

def explode_column(df: pd.DataFrame, column: str) -> pd.DataFrame:

    # # preserve original row id
    # df.reset_index(inplace=True, drop=True)

    # explode the array column
    df = df.explode(column)
    # track array position
    # df["_array_index"] = df.groupby("_row_id").cumcount()

    # reset index
    # df = df.reset_index(drop=True)

    # handle nulls
    df[column] = df[column].apply(lambda x: {} if pd.isna(x) else x)

    # flatten nested fields
    exploded_cols = json_normalize(df[column], sep="_")
    exploded_cols.index = df.index

    # combine dataframe
    df = pd.concat([df.drop(columns=[column]), exploded_cols], axis=1)

    # drop helper column
    # df = df.drop(columns=["_row_id"])

    return df

# Flatten nested dictionaries
def flatten_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    records = df.to_dict(orient="records")
    return json_normalize(records, sep="_")

# Handle null values
def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("")
        elif pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(0)
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].fillna(pd.NaT)
    return df

#Rowhash generation
def add_row_hash(df: pd.DataFrame) -> pd.DataFrame:

    if df.empty:
        return df

    exclude = {
        "etl_loaded_at",
        "row_hash",
        "is_active",
        "_array_index"
    }

    hash_columns = [c for c in df.columns if c not in exclude]

    if not hash_columns:
        return df

    # normalize values
    df_normalized = (
        df[hash_columns]
        .fillna("NULL")
        .astype(str)
    )

    # sort columns to make hash deterministic
    df_normalized = df_normalized[sorted(df_normalized.columns)]

    # generate hash
    df["row_hash"] = df_normalized.agg("|".join, axis=1).apply(
        lambda x: hashlib.md5(x.encode("utf-8")).hexdigest()
    )

    return df
# convert to datetime if column name contains keywords
def convert_timestamps(df: pd.DataFrame) -> pd.DataFrame:

    if df.empty:
        return df

    keywords = {"date", "time", "timestamp", "created", "updated", "inserted"}

    for col in df.columns:

        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue

        if any(k in col.lower() for k in keywords):

            df[col] = pd.to_datetime(
                df[col],
                errors="coerce",
                utc=True
            )

    return df

# Sanitize column names
def sanitize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.replace(".", "_", regex=False).str.lower()
    return df

# Add SCD Type 2 control columns
def add_scd_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Use _id if exists, else generate row hash
    if "_id" in df.columns:
        df["_source_object_id"] = df["_id"].astype(str)
    else:
        df["_source_object_id"] = df.apply(
            lambda row: hashlib.md5(str(tuple(row)).encode()).hexdigest(),
            axis=1
        )
    df["is_active"] = 1
    df["etl_loaded_at"] = pd.Timestamp.now()
    return df

# MAIN TRANSFORM FUNCTION
def transform(df):

    df = convert_mongo_types(df)

    while True:
        list_cols = get_list_columns(df)
        if not list_cols:
            break
        for col in list_cols:
            df = explode_column(df, col)
    df.reset_index(inplace=True, drop=True)
    df['_array_index']=df.groupby(['_id']).cumcount()
    df.sort_values(by=['_id','_array_index'], inplace=True)
    df = flatten_dataframe(df)
    df = sanitize_column_names(df)
    df = convert_timestamps(df)
    df = handle_nulls(df)

    df = add_scd_columns(df)

    if "_array_index" not in df.columns:
        df["_array_index"] = 0
        
    df = add_row_hash(df)

    return df
