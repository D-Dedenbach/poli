{{ config(materialized='view') }}

SELECT
    id,
    typeid as actor_type_id,
    gruppenavnkort as group_name_short,
    TRIM(navn) as actor_name,
    fornavn as first_name,
    efternavn as last_name,
    periodeid as period_id,
    opdateringsdato as updated_at,
    startdato as start_date,
    slutdato as end_date,
    biografi as biography
FROM {{ source('raw', 'actors') }}