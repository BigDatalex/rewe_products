select
    load_id,
    schema_name,
    status,
    inserted_at,
    schema_version_hash
from {{ source('rewe_products', '_dlt_loads') }}
