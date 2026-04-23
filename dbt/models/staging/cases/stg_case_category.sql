{{config(materialized='view') }}

SELECT id AS category_id
    , kategori as case_category
    , opdateringsdato as updated_at

FROM {{ source('raw', 'case_category') }}