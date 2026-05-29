{{
    config(
        materialized = 'view',
        schema = 'staging'
    )
}}

-- Party display reference table with colors and left-to-right ordering
-- Source: dbt seed loaded from party_display.csv

SELECT 
    id,
    party_abbr,
    ltr_order,
    party_color
FROM {{ ref('party_display') }}
