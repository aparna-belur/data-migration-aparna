import random
import json
from datetime import datetime, timedelta

NUM_SELLERS = 120

cities = [
    ("Pune", "Maharashtra"),
    ("Mumbai", "Maharashtra"),
    ("Bengaluru", "Karnataka"),
    ("Hyderabad", "Telangana"),
    ("Chennai", "Tamil Nadu"),
    ("Delhi", "Delhi"),
    ("Kolkata", "West Bengal")
]

categories = ["Electronics", "Clothing", "Home", "Books", "Beauty"]

sellers = []

for i in range(1, NUM_SELLERS + 1):

    seller_id = f"SELL{i:03d}"
    city, state = random.choice(cities)

    num_products = random.randint(3, 8)
    products = []

    # ✅ Unique products per seller
    for j in range(1, num_products + 1):
        product_id = f"{seller_id}_PRD{j}"

        products.append({
            "product_id": product_id,
            "category": random.choice(categories),
            "price": round(random.uniform(500, 30000), 2),
            "stock": None if random.random() < 0.1 else random.randint(0, 200)
        })

    # Ratings (can repeat customer across sellers, that's fine)
    ratings = []
    used_rating_keys = set()

    for _ in range(random.randint(1, 5)):
        cust_id = f"CUST{random.randint(1, 500):03d}"
        review_date = (
            datetime.now() - timedelta(days=random.randint(1, 60))
        ).date().isoformat()

        # Prevent exact duplicate rating per seller
        rating_key = (cust_id, review_date)

        if rating_key in used_rating_keys:
            continue

        used_rating_keys.add(rating_key)

        ratings.append({
            "customer_id": cust_id,
            "rating": random.randint(1, 5),
            "review": None if random.random() < 0.15 else "Auto generated review",
            "review_date": review_date
        })

    last_updated = datetime.now() - timedelta(
        days=random.randint(0, 7),
        hours=random.randint(0, 23)
    )

    seller = {
        "seller_id": seller_id,

        "seller_profile": {
            "name": f"Seller_{i}",
            "business_type": None if random.random() < 0.1 else random.choice(["Retail", "Wholesale"]),
            "joined_date": (
                datetime.now() - timedelta(days=random.randint(90, 900))
            ).date().isoformat()
        },

        "address": {
            "street": None if random.random() < 0.1 else f"Shop {random.randint(1, 300)}",
            "city": city,
            "state": state,
            "zip": str(random.randint(100000, 999999)),
            "country": "India"
        },

        "products": products,
        "ratings": ratings,

        "performance": {
            "total_orders": random.randint(50, 2000),
            "cancelled_orders": None if random.random() < 0.05 else random.randint(0, 50)
        },

        "last_updated": last_updated.isoformat()
    }

    sellers.append(seller)

with open("sellers.json", "w") as f:
    json.dump(sellers, f, indent=2)

print(f"{NUM_SELLERS} sellers generated successfully.")