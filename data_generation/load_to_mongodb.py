from pymongo import MongoClient
import json
from pathlib import Path

# -----------------------
# Resolve base directory
# -----------------------
BASE_DIR = Path(__file__).resolve().parents[1]

DATA_DIR = BASE_DIR / "data_generation"

CUSTOMERS_FILE = DATA_DIR / "customers.json"
SELLERS_FILE = DATA_DIR / "sellers.json"
ORDERS_FILE = DATA_DIR / "orders.json"

# -----------------------
# MongoDB connection
# -----------------------
client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_db"]

customers_col = db["customers"]
sellers_col = db["sellers"]
orders_col = db["orders"]

# -----------------------
# Clear existing data
# -----------------------
customers_col.delete_many({})
sellers_col.delete_many({})
orders_col.delete_many({})

# -----------------------
# Load JSON files
# -----------------------
with open(CUSTOMERS_FILE, "r", encoding="utf-8") as f:
    customers = json.load(f)

with open(SELLERS_FILE, "r", encoding="utf-8") as f:
    sellers = json.load(f)

with open(ORDERS_FILE, "r", encoding="utf-8") as f:
    orders = json.load(f)

# -----------------------
# Insert into MongoDB
# -----------------------
customers_col.insert_many(customers)
sellers_col.insert_many(sellers)
orders_col.insert_many(orders)

# -----------------------
# Verification
# -----------------------
print("MongoDB Load Summary")
print("--------------------")
print(f"Customers inserted: {customers_col.count_documents({})}")
print(f"Sellers inserted:   {sellers_col.count_documents({})}")
print(f"Orders inserted:    {orders_col.count_documents({})}")