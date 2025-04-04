select
    id as product_id,  
    version,  
    gtin,  
    _embedded__listing__id as listing_id,  
    _embedded__listing__version as listing_version,  
    _embedded__listing__pricing__current_retail_price as listing_price,  
    _embedded__listing__pricing__currency as listing_currency,  
    _embedded__listing__pricing__base_price as listing_base_price,  
    _embedded__listing__pricing__base_unit__kg as listing_base_unit_kg,  
    _embedded__listing__pricing__discount__regular_price as listing_regular_price,  
    _embedded__listing__pricing__discount__discount_rate as listing_discount_rate,  
    CAST(
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', 
        CASE 
        WHEN _embedded__listing__pricing__discount__valid_to LIKE '%CET%' THEN REPLACE(_embedded__listing__pricing__discount__valid_to, 'CET', '+01:00')
        WHEN _embedded__listing__pricing__discount__valid_to LIKE '%CEST%' THEN REPLACE(_embedded__listing__pricing__discount__valid_to, 'CEST', '+02:00')
        ELSE _embedded__listing__pricing__discount__valid_to
        END
    ) AS TIMESTAMP
    ) AS listing_discount_valid_to,
    --_embedded__listing__pricing__discount__valid_to as listing_discount_valid_to,  
    _embedded__listing__pricing__grammage as listing_grammage,
    REGEXP_EXTRACT(_embedded__listing__pricing__grammage, r'^(.*?)(?:\s\(1\s(?:kg|l))') AS extracted_grammage,  
    _embedded__listing__limitations__order_limit as listing_order_limit, 
    _embedded__store__id as store_id,  
    _embedded__store__version as store_version,  
    _embedded__merchant__id as merchant_id, 
    _embedded__merchant__version as merchant_version,  
    _embedded__merchant__name as merchant_name,  
    _embedded__merchant__type as merchant_type,  
    _embedded__merchant__logo as merchant_logo,  
    _dlt_parent_id,
    _dlt_list_idx,  
    _dlt_id,  
    _embedded__listing__pricing__base_unit__l as listing_base_unit_l,  
    _embedded__listing__pricing__total_refund as listing_total_refund,  
    _embedded__listing__limitations__bulky_goods_share as listing_bulky_goods_share  
from {{ source('rewe_products', 'rewe_products___embedded__articles') }}