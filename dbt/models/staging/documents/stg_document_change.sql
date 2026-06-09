SELECT
    id AS document_change_id
    , dokumentid AS document_id
    , dato AS change_date
    , begrundelse AS reason
    , opdateringsdato AS updated_at

FROM {{ source('raw', 'document_change') }}
