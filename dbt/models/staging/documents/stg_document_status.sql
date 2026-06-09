{{ config(materialized='view') }}

SELECT
    id AS document_status_id
    , status AS status
    , opdateringsdato AS updated_at

FROM {{ source('raw', 'document_status') }}
