from pymongo import MongoClient
import pymysql
import pandas as pd
from config import MONGO_CONFIG, MYSQL_CONFIG

_SYSTEM_COLS = {"row_hash", "etl_loaded_at", "is_active"}

def _norm_ts(v):
    """Normalize timestamps for comparison."""
    if v is None:
        return None
    try:
        return pd.to_datetime(v)
    except:
        return None

def reconcile_collection(collection_name, df_transformed):
    """
    Realistic reconciliation for SCD2 ETL:
    - df_transformed: flattened DataFrame after transform()
    """

    # -------------------------
    # MongoDB client
    # -------------------------
    client = MongoClient(MONGO_CONFIG["uri"])
    db = client[MONGO_CONFIG["database"]]
    collection = db[collection_name]

    # Distinct source_object_ids in Mongo
    mongo_ids = df_transformed["_source_object_id"].unique()
    mongo_doc_count = len(mongo_ids)

    # Flattened rows count in Mongo
    mongo_flattened_count = len(df_transformed)

    # Max timestamp in Mongo
    timestamp_cols = [c for c in df_transformed.columns if "updated" in c.lower() or "created" in c.lower()]
    if timestamp_cols:
        mongo_max_ts = df_transformed[timestamp_cols].max().max()
    else:
        mongo_max_ts = None

    # -------------------------
    # MySQL client
    # -------------------------
    conn = pymysql.connect(**MYSQL_CONFIG)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    # Distinct _source_object_id count in MySQL
    cursor.execute(f"""
        SELECT COUNT(DISTINCT _source_object_id) AS cnt
        FROM `{collection_name}`
        WHERE is_active=1
    """)
    mysql_active_count = cursor.fetchone()["cnt"]

    # Total loaded rows in MySQL
    cursor.execute(f"""
        SELECT COUNT(*) AS cnt
        FROM `{collection_name}`
        WHERE is_active=1
    """)
    mysql_total_count = cursor.fetchone()["cnt"]

    # Max timestamp in MySQL
    if timestamp_cols:
        col = timestamp_cols[0]
        cursor.execute(f"""
            SELECT MAX(`{col}`) AS max_ts
            FROM `{collection_name}`
            WHERE is_active=1
        """)
        mysql_max_ts = cursor.fetchone()["max_ts"]
    else:
        mysql_max_ts = None

    # Identify missing rows
    cursor.execute(f"""
        SELECT _source_object_id
        FROM `{collection_name}`
        WHERE is_active=1
    """)
    mysql_ids = {r["_source_object_id"] for r in cursor.fetchall()}
    missing_in_mysql = set(mongo_ids) - mysql_ids
    missing_in_mongo = mysql_ids - set(mongo_ids)

    # Optional: row_hash comparison (safe)
    row_hash_match_count = None
    row_hash_mismatch_count = None
    try:
        cursor.execute(f"""
            SELECT _source_object_id, _array_index, row_hash
            FROM `{collection_name}`
            WHERE is_active=1
        """)
        mysql_rows = cursor.fetchall()
        mysql_hash_map = {
            (r["_source_object_id"], str(r["_array_index"])): r["row_hash"]
            for r in mysql_rows
        }
        # Merge with transformed df
        df_compare = df_transformed.copy()
        df_compare["_array_index"] = df_compare["_array_index"].astype(str)
        df_compare["mysql_hash"] = df_compare.apply(
            lambda r: mysql_hash_map.get((r["_source_object_id"], r["_array_index"])),
            axis=1
        )
        row_hash_match_count = df_compare["row_hash"] == df_compare["mysql_hash"]
        row_hash_match_count = row_hash_match_count.sum()
        row_hash_mismatch_count = len(df_compare) - row_hash_match_count
    except Exception as e:
        # If hash comparison fails, just skip
        row_hash_match_count = None
        row_hash_mismatch_count = None

    # -------------------------
    # Print report
    # -------------------------
    print("\n================ RECONCILIATION =================")
    print(f"Collection: {collection_name}")
    print(f"Distinct IDs: Mongo={mongo_doc_count}, MySQL={mysql_active_count}")
    print(f"Flattened rows: Mongo={mongo_flattened_count}, MySQL={mysql_total_count}")
    if missing_in_mysql:
        print(f"IDs missing in MySQL: {len(missing_in_mysql)} (sample {list(missing_in_mysql)[:5]})")
    if missing_in_mongo:
        print(f"IDs missing in Mongo: {len(missing_in_mongo)} (sample {list(missing_in_mongo)[:5]})")
    if mongo_max_ts or mysql_max_ts:
        print(f"Max timestamp: Mongo={mongo_max_ts}, MySQL={mysql_max_ts}")
    if row_hash_match_count is not None:
        print(f"Row hash matched: {row_hash_match_count}, mismatched: {row_hash_mismatch_count}")
    print("==================================================\n")

    # -------------------------
    # Close connections
    # -------------------------
    cursor.close()
    conn.close()
    client.close()

    return {
        "collection": collection_name,
        "mongo_doc_count": mongo_doc_count,
        "mongo_flattened_count": mongo_flattened_count,
        "mysql_active_count": mysql_active_count,
        "mysql_total_count": mysql_total_count,
        "missing_in_mysql": missing_in_mysql,
        "missing_in_mongo": missing_in_mongo,
        "mongo_max_ts": mongo_max_ts,
        "mysql_max_ts": mysql_max_ts,
        "row_hash_match_count": row_hash_match_count,
        "row_hash_mismatch_count": row_hash_mismatch_count,
    }