import json
import random
from datetime import datetime, timedelta
import os

NUM_ORDERS = 1500

# Load sellers
current_dir = os.path.dirname(__file__)
sellers_path = os.path.join(current_dir, "sellers.json")
with open(sellers_path, "r") as f:
    sellers = json.load(f)

# Build seller → products map
seller_map = {
    seller["seller_id"]: seller["products"]
    for seller in sellers
}

seller_ids = list(seller_map.keys())
customer_ids = [f"CUST{i:03d}" for i in range(1, 501)]

orders = []

for i in range(1, NUM_ORDERS + 1):

    order_id = f"ORD{i:04d}"
    seller_id = random.choice(seller_ids)
    customer_id = random.choice(customer_ids)

    seller_products = seller_map[seller_id]

    num_items = random.randint(1, min(4, len(seller_products)))

    # ✅ Ensure unique products inside order
    selected_products = random.sample(seller_products, num_items)

    items = []
    calculated_total = 0

    for product in selected_products:

        quantity = random.randint(1, 3)
        discount_amount = random.choice([0, 50, 100, 200])

        item_total = (product["price"] * quantity) - discount_amount
        calculated_total += item_total

        items.append({
            "product_id": product["product_id"],
            "product_name": product["category"],
            "quantity": quantity,
            "price": product["price"],
            "discount": {
                "type": "FLAT" if discount_amount > 0 else "NONE",
                "amount": None if random.random() < 0.1 else discount_amount
            }
        })

    # Intentionally wrong totals (10%)
    if random.random() < 0.1:
        order_total = round(calculated_total + random.randint(-500, 500), 2)
    else:
        order_total = round(calculated_total, 2)

    order_date = datetime.now() - timedelta(days=random.randint(1, 30))
    last_updated = order_date + timedelta(hours=random.randint(1, 48))

    order = {
        "order_id": order_id,
        "customer_id": customer_id,
        "seller_id": seller_id,
        "order_date": order_date.isoformat(),

        "order_status": None if random.random() < 0.05 else random.choice(
            ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]
        ),

        "items": items,

        "payment": {
            "method": random.choice(["UPI", "Debit Card", "Credit Card"]),
            "transaction_id": None if random.random() < 0.1 else f"TXN{random.randint(100000,999999)}",
            "payment_status": None if random.random() < 0.05 else random.choice(
                ["SUCCESS", "FAILED"]
            )
        },

        "shipping": {
            "delivery_partner": None if random.random() < 0.1 else random.choice(
                ["Delhivery", "BlueDart", "Ecom Express"]
            ),
            "expected_delivery": None if random.random() < 0.05 else (
                order_date + timedelta(days=random.randint(2, 7))
            ).date().isoformat()
        },

        "order_total": None if random.random() < 0.03 else order_total,
        "last_updated": last_updated.isoformat()
    }

    orders.append(order)

with open("orders.json", "w") as f:
    json.dump(orders, f, indent=2)

print(f"{NUM_ORDERS} orders generated successfully.")