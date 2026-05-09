SELECT CS.case_step_id
    , CS.case_id
    , CS.case_step_status
    , CS.case_step_type
    , CS.title AS case_step_title
    , C.case_title
    , C.case_title_short
    , C.decision
    , C.case_reasoning
    , C.case_category
    , C.case_type
    , C.case_status



FROM {{ ref('stg_case_step') }} CS
INNER JOIN {{ ref('stg_case') }} C ON CS.case_id = C.case_id