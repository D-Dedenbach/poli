{{config(materialized='view') }}

SELECT id
    , kategori as case_category
    , opdateringsdato as updated_at

FROM {{ source('raw', 'case_category') }}