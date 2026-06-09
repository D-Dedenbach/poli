{{ config(materialized='view') }}

SELECT
    id AS case_document_role_id
    , rolle AS role
    , opdateringsdato AS updated_at

FROM {{ source('raw', 'case_document_role') }}
