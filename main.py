from extract import extract, get_collections
from transform import transform
from load import load_data
from reconciliation import reconcile_collection


def main():

    collections = get_collections()

    for collection in collections:
        try:

            df = extract(collection)

            df = transform(df)

            load_data(df, collection,"last_updated")

            print(f"{collection} loaded successfully")
            
            reconcile_collection(collection, df)

        except Exception as e:

            print(f"ETL failed for {collection}")
            print(e)


if __name__ == "__main__":
    main()