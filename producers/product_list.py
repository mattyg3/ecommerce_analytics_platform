import random
import json
from pathlib import Path

def round_price(price: float) -> float:
    if price < 10:
        return round(int(price) + 0.99, 2)
    elif price < 50:
        return round(int(price) + 0.95, 2)
    elif price < 150:
        return round(int(price) + 0.49, 2)
    else:
        return round(round(price), 2)

def generate_products(
        num_products=1500,
        price_ranges=((5, 20), (20, 60), (60, 150), (150, 400)),
        weights=(0.35, 0.40, 0.20, 0.05),
        seed=894
):
    """
    Generate synthetic products with realistic price distribution.
    Returns a list of dicts: [{"product_id": "...", "price_usd": ...}]
    """
    random.seed(seed)

    products = []

    for i in range(1, num_products + 1):
        low, high = random.choices(price_ranges, weights=weights)[0]
        price = random.uniform(low, high)
        formatted_price = round_price(price)

        products.append({
            "product_id": f"SKU-{i:05d}",
            "price_usd": formatted_price
        })

    return products

def save_to_json(products, filename="products.json"):
    filepath = SAVE_DIR / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(products, f, indent=2)


if __name__ == "__main__":
    SAVE_DIR = Path("data")
    SAVE_DIR.mkdir(parents=True, exist_ok=True)
    products = generate_products()
    save_to_json(products)