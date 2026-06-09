SELECT
    id AS case_step_document_id
    , sagstrinid AS case_step_id
    , dokumentid AS document_id
    , opdateringsdato AS updated_at

FROM {{ source('raw', 'case_step_document') }}
