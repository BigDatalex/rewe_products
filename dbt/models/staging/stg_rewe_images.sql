select
    _links__self__href as image_link,
    _dlt_parent_id,
    _dlt_list_idx,
    _dlt_id
from {{ source('rewe_products', 'rewe_products__media__images') }}
