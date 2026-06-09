SELECT
    D.id
    , D.dato AS date
    , D.frigivelsesdato AS date_released
    , D.grundnotatstatus AS reasoning_memo
    , D.kategoriid AS category_id
    , C.kategori AS category_id
    , D.modtagelsesdato AS date_received
    , D.offentlighedskode AS public
    , D.opdateringsdato AS updated_at
    , D.paragraf AS paragraph
    , D.paragrafnummer AS paragraph_no
    , D.procedurenummer AS procedure_no
    , D.sp_rgsm_lsid AS question_id
    , D.sp_rgsm_lsordlyd AS question_wording
    , D.sp_rgsm_lstitel AS question_title
    , D.statusid AS status_id
    , S.status AS status
    , D.titel AS title
    , D.typeid AS type_id
    , T.type

FROM {{ source('raw', 'document') }} D
INNER JOIN {{ source('raw', 'document_category') }} C ON D.kategoriid = C.id
INNER JOIN {{ source('raw', 'document_status') }} S ON D.statusid = S.id
INNER JOIN {{ source('raw', 'document_type') }} T ON D.typeid = T.id