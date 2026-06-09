{{ config(materialized='view') }}

SELECT
    id AS document_type_id
    , type AS type
    , opdateringsdato AS updated_at

FROM {{ source('raw', 'document_type') }}
