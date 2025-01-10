{% snapshot snapshot__distribution_centers %}

{{
  config(
    target_schema='dbt_test',
    unique_key='id',
    strategy='check',
    check_cols=['name', 'longitude', 'latitude']
    )
}}

select
    id,
    name,
    longitude,
    latitude
from {{ source('thelook_ecommerce', 'distribution_centers') }}

{% endsnapshot %}