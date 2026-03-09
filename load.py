# load.py

import pandas as pd
import numpy as np
import pymysql
import hashlib

def get_connection():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="password",
        database="ecommerce_dw",
        port=3306
    )

# SQL datatype inference

def infer_sql_type(dtype, column_name=None):

    if column_name == "_array_index":
        return "VARCHAR(255)"

    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"

    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE"

    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"

    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"

    return "VARCHAR(255)"


# -----------------------------------
# CLEAN DATAFRAME
# -----------------------------------

def clean_dataframe(df):

    df = df.replace({np.nan: None})

    if "id" in df.columns:
        df = df.drop(columns=["id"])

    return df


# -----------------------------------
# CREATE TARGET TABLE
# -----------------------------------

def create_table_if_not_exists(cursor, df, table_name):

    columns = []

    for col, dtype in df.dtypes.items():

        sql_type = infer_sql_type(dtype, col)

        columns.append(f"`{col}` {sql_type}")

    # system columns

    if "row_hash" not in df.columns:
        columns.append("row_hash VARCHAR(64)")

    if "is_active" not in df.columns:
        columns.append("is_active BOOLEAN DEFAULT TRUE")

    if "etl_loaded_at" not in df.columns:
        columns.append("etl_loaded_at DATETIME")

    column_sql = ", ".join(columns)

    query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (

        id BIGINT AUTO_INCREMENT PRIMARY KEY,

        {column_sql}
    )
    """

    cursor.execute(query)


# -----------------------------------
# INSERT DATAFRAME
# -----------------------------------

def insert_dataframe(cursor, df, table_name):

    cols = ",".join([f"`{c}`" for c in df.columns])

    placeholders = ",".join(["%s"] * len(df.columns))

    query = f"""
    INSERT INTO `{table_name}` ({cols})
    VALUES ({placeholders})
    """

    cursor.executemany(query, df.values.tolist())


# -----------------------------------
# CREATE ETL TRACKER
# -----------------------------------

def create_etl_tracker(cursor):

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS etl_tracker(

        id INT AUTO_INCREMENT PRIMARY KEY,

        collection_name VARCHAR(100),
        load_type VARCHAR(20),

        etl_start_time DATETIME,
        etl_end_time DATETIME,

        records_processed INT,
        max_source_timestamp DATETIME,

        status VARCHAR(20),
        error_message TEXT
    )
    """)


# -----------------------------------
# LOG ETL RUN
# -----------------------------------

def log_etl(cursor, table, load_type, start, end,
            count, max_ts, status, error=None):

    cursor.execute("""
    INSERT INTO etl_tracker(

        collection_name,
        load_type,
        etl_start_time,
        etl_end_time,
        records_processed,
        max_source_timestamp,
        status,
        error_message

    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """,
    (table, load_type, start, end,
     count, max_ts, status, error))


# -----------------------------------
# CHECK FULL LOAD
# -----------------------------------

def is_full_load(cursor, table_name):

    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")

    exists = cursor.fetchone()

    if not exists:
        return True

    cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")

    count = cursor.fetchone()[0]

    return count == 0


# -----------------------------------
# INCREMENTAL LOGIC
# -----------------------------------

def incremental_load(cursor, df, table_name):

    for _, row in df.iterrows():

        source_id = row["_source_object_id"]
        array_index = row["_array_index"]
        row_hash = row["row_hash"]

        cursor.execute(f"""
        SELECT row_hash
        FROM `{table_name}`
        WHERE _source_object_id=%s
        AND _array_index=%s
        AND is_active=TRUE
        """, (source_id, array_index))

        existing = cursor.fetchone()

        # NEW RECORD
        if existing is None:

            insert_dataframe(
                cursor,
                pd.DataFrame([row]),
                table_name
            )

        # UPDATED RECORD
        elif existing[0] != row_hash:

            cursor.execute(f"""
            UPDATE `{table_name}`
            SET is_active=FALSE
            WHERE _source_object_id=%s
            AND _array_index=%s
            AND is_active=TRUE
            """, (source_id, array_index))

            insert_dataframe(
                cursor,
                pd.DataFrame([row]),
                table_name)

        # UNCHANGED RECORD
        else:
            pass


# -----------------------------------
# MAIN LOAD FUNCTION
# -----------------------------------

def load_data(df, table_name,
              updated_col="META_updated_at"):

    conn = get_connection()

    cursor = conn.cursor()

    start_time = pd.Timestamp.now()

    try:

        df = clean_dataframe(df)
 
        df["is_active"] = True
        df["etl_loaded_at"] = pd.Timestamp.now()

        create_etl_tracker(cursor)

        create_table_if_not_exists(cursor,
                                   df,
                                   table_name)

        # ----------------------------
        # FULL LOAD
        # ----------------------------

        if is_full_load(cursor, table_name):

            cursor.execute(
                f"TRUNCATE TABLE `{table_name}`"
            )

            insert_dataframe(cursor,
                             df,
                             table_name)

            load_type = "FULL"

        # ----------------------------
        # INCREMENTAL LOAD
        # ----------------------------

        else:

            incremental_load(cursor,
                             df,
                             table_name)

            load_type = "INCREMENTAL"

        conn.commit()

        end_time = pd.Timestamp.now()

        max_ts = None

        if updated_col in df.columns:
            max_ts = df[updated_col].max()

        log_etl(
            cursor,
            table_name,
            load_type,
            start_time,
            end_time,
            len(df),
            max_ts,
            "SUCCESS"
        )

        conn.commit()

        print(f"{table_name} {load_type} load completed")

    except Exception as e:

        conn.rollback()

        end_time = pd.Timestamp.now()

        log_etl(
            cursor,
            table_name,
            "FAILED",
            start_time,
            end_time,
            0,
            None,
            "FAILED",
            str(e)
        )

        conn.commit()

        raise

    finally:

        cursor.close()
        conn.close()