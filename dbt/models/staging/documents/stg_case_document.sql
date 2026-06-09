SELECT
    id AS case_document_id
    , sagid AS case_id
    , dokumentid AS document_id
    , bilagsnummer AS appendix_number
    , frigivelsesdato AS release_date
    , rolleid AS role_id
    , opdateringsdato AS updated_at

FROM {{ source('raw', 'case_document') }}
