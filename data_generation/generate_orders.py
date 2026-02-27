import json
import random
from datetime import datetime, timedelta

NUM_ORDERS = 1500

# Load sellers data (to get products)
with open("data_generation/sellers.json", "r") as f:
    sellers = json.load(f)

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

    num_items = random.randint(1, 4)
    items = []
    calculated_total = 0

    for _ in range(num_items):
        product = random.choice(seller_products)
        quantity = random.randint(1, 3)

        discount_amount = random.choice([0, 50, 100, 200])
        price = product["price"]

        item_total = (price * quantity) - discount_amount
        calculated_total += item_total

        items.append({
            "product_id": product["product_id"],
            "product_name": product["category"],
            "quantity": quantity,
            "price": price,
            "discount": {
                "type": "FLAT" if discount_amount > 0 else "NONE",
                # nullable discount amount
                "amount": None if random.random() < 0.1 else discount_amount
            }
        })

    # Intentionally wrong totals (~10%)
    if random.random() < 0.1:
        order_total = round(calculated_total + random.randint(-500, 500), 2)
    else:
        order_total = round(calculated_total, 2)

    order_date = datetime.now() - timedelta(days=random.randint(1, 30))
    last_updated = order_date + timedelta(hours=random.randint(1, 48))

    order = {
        # _id NOT provided → MongoDB auto-generates

        "order_id": order_id,
        "customer_id": customer_id,
        "seller_id": seller_id,
        "order_date": order_date.isoformat(),

        # nullable status
        "order_status": None if random.random() < 0.05 else random.choice(
            ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]
        ),

        "items": items,

        "payment": {
            "method": random.choice(["UPI", "Debit Card", "Credit Card"]),
            # nullable transaction_id
            "transaction_id": None if random.random() < 0.1 else f"TXN{random.randint(100000,999999)}",
            # nullable payment_status
            "payment_status": None if random.random() < 0.05 else random.choice(
                ["SUCCESS", "FAILED"]
            )
        },

        "shipping": {
            # nullable delivery partner
            "delivery_partner": None if random.random() < 0.1 else random.choice(
                ["Delhivery", "BlueDart", "Ecom Express"]
            ),
            # nullable expected delivery
            "expected_delivery": None if random.random() < 0.05 else (
                order_date + timedelta(days=random.randint(2, 7))
            ).date().isoformat()
        },

        # nullable order total
        "order_total": None if random.random() < 0.03 else order_total,

        "last_updated": last_updated.isoformat()
    }

    orders.append(order)

with open("orders.json", "w") as f:
    json.dump(orders, f, indent=2)

print(f"{NUM_ORDERS} orders generated successfully.")