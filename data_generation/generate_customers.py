import random
from datetime import datetime, timedelta
import json

NUM_CUSTOMERS = 500

cities = [
    ("Pune", "Maharashtra"),
    ("Mumbai", "Maharashtra"),
    ("Bengaluru", "Karnataka"),
    ("Hyderabad", "Telangana"),
    ("Chennai", "Tamil Nadu"),
    ("Delhi", "Delhi"),
    ("Kolkata", "West Bengal")
]

payment_methods = ["UPI", "Debit Card", "Credit Card"]

customers = []

for i in range(1, NUM_CUSTOMERS + 1):
    cust_id = f"CUST{i:03d}"
    city, state = random.choice(cities)

    last_updated = datetime.now() - timedelta(
        days=random.randint(0, 7),
        hours=random.randint(0, 23)
    )

    customer = {
    

        "customer_id": cust_id,

        "profile": {
            "name": f"Customer_{i}",

            "email": None if random.random() < 0.1 else f"customer{i}@example.com",

            "phone": None if random.random() < 0.15 else f"9{random.randint(100000000, 999999999)}",

            "status": random.choice(["ACTIVE", "INACTIVE"]),

            "signup_date": (
                datetime.now() - timedelta(days=random.randint(30, 365))
            ).isoformat()
        },

        "address": {
            "current": {
             
                "street": None if random.random() < 0.1 else f"Street {random.randint(1, 200)}",
                "city": city,
                "state": state,
                "zip": str(random.randint(100000, 999999)),
                "country": "India"
            }
        },

        "payment_methods": [
            {
                
                "method": None if random.random() < 0.05 else random.choice(payment_methods),
                "provider": random.choice(["HDFC", "ICICI", "SBI"]),
                "last_used": (
                    datetime.now() - timedelta(days=random.randint(1, 30))
                ).date().isoformat()
            }
        ],

        "last_updated": last_updated.isoformat()
    }

    customers.append(customer)

# Write to JSON
with open("customers.json", "w") as f:
    json.dump(customers, f, indent=2)

print(f"{NUM_CUSTOMERS} customers generated successfully.")