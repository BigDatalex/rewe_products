# rewe_products

```sql
SELECT DISTINCT(products.id),
product_name,
_embedded__category_path,
_links__detail__href,
brand__name,
_embedded__listing__pricing__current_retail_price,
_embedded__listing__pricing__discount__regular_price,
_embedded__listing__pricing__discount__discount_rate
,_embedded__listing__pricing__discount__valid_to,
_embedded__listing__pricing__grammage
 FROM `splendid-parsec-449218-j6.rewe_products_data.rewe_products` as products
join `splendid-parsec-449218-j6.rewe_products_data.rewe_products___embedded__articles` as articles 
on products._dlt_id=articles._dlt_parent_id
where _embedded__category_path not like "Tierbedarf%" and
_embedded__category_path not like "Küche & Haushalt%" and
_embedded__category_path not like "Drogerie & Kosmetik%"
```

```sql
CREATE OR REPLACE TABLE `splendid-parsec-449218-j6.rewe_products_data.rewe_fact_products` AS
SELECT DISTINCT(products.id),product_name,_embedded__category_path,_links__detail__href,brand__name,_embedded__listing__pricing__current_retail_price,_embedded__listing__pricing__discount__regular_price,_embedded__listing__pricing__discount__discount_rate
,_embedded__listing__pricing__discount__valid_to,_embedded__listing__pricing__grammage
 FROM `splendid-parsec-449218-j6.rewe_products_data.rewe_products` as products
join `splendid-parsec-449218-j6.rewe_products_data.rewe_products___embedded__articles` as articles on products._dlt_id=articles._dlt_parent_id
where _embedded__category_path not like "Tierbedarf%" and
_embedded__category_path not like "Küche & Haushalt%" and
_embedded__category_path not like "Drogerie & Kosmetik%"
```