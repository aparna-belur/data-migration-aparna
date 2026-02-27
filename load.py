import pymysql
from config import MYSQL_CONFIG


#mySQL connection
def get_connection():
    return pymysql.connect(
        host=MYSQL_CONFIG["host"],
        port=MYSQL_CONFIG["port"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database=MYSQL_CONFIG["database"],
        cursorclass=pymysql.cursors.DictCursor
    )


#Load function
def load_to_mysql(df, table_name, load_type="incremental"):

    if df.empty:
        print(f"No data to load into {table_name}")
        return

    conn = get_connection()
    cursor = conn.cursor()

    try:

        # FULL LOAD
        if load_type == "full":
            print(f"Performing FULL load for {table_name}")
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            conn.commit()

        #  Prepare insert query
        columns = df.columns.tolist()
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        update_clause = ", ".join(
            [f"{col}=VALUES({col})" for col in columns]
        )

        sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause};
        """

        data = [tuple(row) for row in df.to_numpy()]

        cursor.executemany(sql, data)
        conn.commit()

        print(f"Loaded {len(data)} rows into {table_name}")

    except Exception as e:
        conn.rollback()
        print(f"Error loading {table_name}: {e}")

    finally:
        cursor.close()
        conn.close()