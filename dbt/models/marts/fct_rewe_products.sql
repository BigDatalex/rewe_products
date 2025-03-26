with rewe_products as (
    select * from {{ ref('stg_rewe_products') }}
),

rewe_articles as(
    select * from {{ ref('stg_rewe_products_articles') }}
),

dlt_loads as (
    select* from {{ ref('stg_dlt_loads') }}
),

final as (

    select
        DISTINCT(rewe_products.product_id),
        rewe_products.product_name,
        rewe_products.category_path,
        rewe_products.category_level_1,
        rewe_products.category_level_2,
        rewe_products.category_level_3,
        rewe_products.category_level_4,
        rewe_products.link,
        rewe_products.brand,
        rewe_articles.listing_price,
        rewe_articles.listing_regular_price,
        rewe_articles.listing_discount_rate,
        rewe_articles.listing_discount_valid_to,
        rewe_articles.listing_grammage,
        rewe_articles.extracted_grammage,
        dlt_loads.inserted_at
    from rewe_products
    join rewe_articles
    on rewe_products._dlt_id=rewe_articles._dlt_parent_id
    join dlt_loads
    on rewe_products._dlt_load_id=dlt_loads.load_id
    where rewe_products.category_path not like "Tierbedarf%" and
    rewe_products.category_path not like "Küche & Haushalt%" and
    rewe_products.category_path not like "Drogerie & Kosmetik%" and
    rewe_products.category_path not like "Haus & Freizeit%" and
    rewe_products.category_path not like "Babybedarf%" and
    rewe_products.category_path not like "Baby & Kind%" 
)

select * from final
