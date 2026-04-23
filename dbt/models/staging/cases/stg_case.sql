{{config(materialized='view') }}

SELECT 
    C.id as case_id
    , C.typeid as case_type_id
    , CT.case_type
    , C.statusid as case_status_id 
    , CS.case_status
    , C.titel as case_title
    , C.titelkort as case_title_short
    , C.offentlighedskode as is_public
    , C.nummer as case_nr
    , C.nummerprefix as case_nr_prefix
    , C.nummerpostfix as case_nr_postfix
    , C.resume as case_summary
    , C.periodeid as period_id
    , C.opdateringsdato as updated_at
    , C.statsbudgetsag as is_budget_case
    , C.afg_relsesresultatkode as decision_result_code
    , C.paragrafnummer as legal_paragraph_nr
    , C.paragraf as legal_paragraph
    , C.afg_relsesdato as decision_date
    , C.afg_relse as decision
    , C.kategoriid as category_id
    , CC.case_category
    , C.afstemningskonklusion as poll_conclusion
    , C.fremsatundersagid as parent_case_id
    , C.lovnummer as law_number
    , C.lovnummerdato as law_number_date
    , C.deltundersagid as shared_during_case_id
    , C.begrundelse as case_reasoning
    , C.r_dsm_dedato as council_meeting_date

FROM {{source('raw', 'case') }} C
INNER JOIN {{ref("stg_case_category")}} CC ON C.kategoriid = CC.category_id
INNER JOIN {{ ref('stg_case_status') }} CS ON C.statusid = CS.case_status_id
INNER JOIN {{ ref('stg_case_type') }} CT ON C.typeid = CT.case_type_id