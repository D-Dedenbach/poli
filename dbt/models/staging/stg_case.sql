{{config(materialized='view') }}

SELECT 
    id
    , typeid as case_type_id
    , statusid as case_status_id 
    , titel as case_title
    , titelkort as case_title_short
    , offentlighedskode as is_public
    , nummer as case_nr
    , nummerprefix as case_nr_prefix
    , nummerpostfix as case_nr_postfix
    , resume as case_summary
    , periodeid as period_id
    , opdateringsdato as updated_at
    , statsbudgetsag as is_budget_case
    , afg_relsesresultatkode as decision_result_code
    , paragrafnummer as legal_paragraph_nr
    , paragraf as legal_paragraph
    , afg_relsesdato as decision_date
    , afg_relse as decision
    , kategoriid as category_id
    , afstemningskonklusion as poll_conclusion
    , fremsatundersagid as parent_case_id
    , lovnummer as law_number
    , lovnummerdato as law_number_date
    , deltundersagid as shared_during_case_id
    , begrundelse as case_reasoning
    , r_dsm_dedato as council_meeting_date

FROM {{source('raw', 'case') }}