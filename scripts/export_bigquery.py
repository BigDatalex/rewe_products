"""
Exportiert REWE-Produktdaten aus BigQuery nach data/angebote.json.
Wird wöchentlich von GitHub Actions ausgeführt.
"""

import json
import os
import re
from pathlib import Path

from google.cloud import bigquery


BQ_PROJECT = os.environ["BQ_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
BQ_TABLE   = os.environ["BQ_TABLE"]

OUTPUT_PATH = Path(__file__).parent.parent / "data" / "angebote.json"


def extract_keywords(name: str) -> list[str]:
    """Extrahiert Suchbegriffe aus einem Produktnamen."""
    name = name.lower()
    # Mengenangaben entfernen
    name = re.sub(r'\d+[,.]?\d*\s*(g|kg|ml|l|cl|stk|stück|%)\b', '', name)
    # Füllwörter
    stopwords = {
        'rewe', 'beste', 'wahl', 'bio', 'und', 'mit', 'von', 'der', 'die',
        'das', 'für', 'aus', 'in', 'nach', 'art', 'frisch', 'gebacken',
        'täglich', 'pck', 'pkg', 'ca', 'ohne', 'vegan', 'original',
        'classic', 'premium', 'extra', 'fein', 'zart', 'leicht',
    }
    tokens = re.findall(r'[a-züäöß]+', name)
    return [t for t in tokens if t not in stopwords and len(t) > 2]


def main():
    client = bigquery.Client(project=BQ_PROJECT)

    query = f"""
        SELECT
            product_id,
            product_name,
            brand,
            category_level_1,
            category_level_2,
            listing_price,
            listing_regular_price,
            listing_discount_rate,
            listing_discount_valid_to,
            listing_grammage,
            image_link,
            link
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE inserted_at = (
            SELECT MAX(inserted_at) FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
        )
        ORDER BY product_name
    """

    print("Abfrage läuft...")
    df = client.query(query).to_dataframe()
    print(f"{len(df)} Produkte geladen.")

    products = []
    for _, row in df.iterrows():
        on_offer = row["listing_discount_rate"] is not None and not (
            hasattr(row["listing_discount_rate"], "__float__")
            and str(row["listing_discount_rate"]) == "nan"
        )
        # pandas NaN check
        import math
        discount = row["listing_discount_rate"]
        if discount is not None:
            try:
                on_offer = not math.isnan(float(discount))
            except (TypeError, ValueError):
                on_offer = False
        else:
            on_offer = False

        products.append({
            "id":           str(row["product_id"]),
            "name":         row["product_name"],
            "brand":        row["brand"] if row["brand"] else "",
            "cat1":         row["category_level_1"] if row["category_level_1"] else "",
            "cat2":         row["category_level_2"] if row["category_level_2"] else "",
            "price":        int(row["listing_price"]),           # Cent
            "price_regular": int(row["listing_regular_price"]) if on_offer else None,
            "discount":     int(discount) if on_offer else None, # Prozent
            "offer_until":  str(row["listing_discount_valid_to"])[:10] if on_offer else None,
            "grammage":     row["listing_grammage"] if row["listing_grammage"] else "",
            "image":        row["image_link"] if row["image_link"] else "",
            "link":         row["link"] if row["link"] else "",
            "on_offer":     on_offer,
            "keywords":     extract_keywords(row["product_name"]),
        })

    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False, separators=(",", ":"))

    size_kb = OUTPUT_PATH.stat().st_size / 1024
    on_offer_count = sum(1 for p in products if p["on_offer"])
    print(f"Gespeichert: {OUTPUT_PATH} ({size_kb:.0f} KB)")
    print(f"Davon im Angebot: {on_offer_count}")


if __name__ == "__main__":
    main()
