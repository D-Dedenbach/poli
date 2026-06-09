SELECT
    id AS file_id
    , dokumentid AS document_id
    , titel AS title
    , versionsdato AS version_date
    , filurl AS file_url
    , opdateringsdato AS updated_at
    , variantkode AS variant_code
    , format AS file_format

FROM {{ source('raw', 'file') }}
