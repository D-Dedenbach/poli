{{ config(materialized='view') }}

SELECT
    id AS document_category_id
    , kategori AS category
    , opdateringsdato AS updated_at

FROM {{ source('raw', 'document_category') }}
