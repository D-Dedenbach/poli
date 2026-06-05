WITH prepared_votes AS (
    SELECT V.poll_id
        , V.party_abbr
        , V.vote_type
        , V.vote_count
        , P.party_color
        -- 1. Create an explicit sorting index per poll group.
        -- Window functions natively support multiple conditional ORDER BY clauses.
        , ROW_NUMBER() OVER (
            PARTITION BY V.poll_id
            ORDER BY 
                V.vote_type, 
                CASE WHEN V.vote_type = 'Imod' THEN P.ltr_order END DESC,
                CASE WHEN V.vote_type != 'Imod' THEN P.ltr_order END ASC
          ) AS sort_index
    FROM {{ ref('int_votes_per_party') }} V
    INNER JOIN {{ ref('stg_party_display') }} P ON V.party_abbr = P.party_abbr
)

SELECT poll_id
    , SUM(CASE WHEN vote_type = 'For' THEN vote_count ELSE 0 END) AS total_for_votes
    , SUM(CASE WHEN vote_type = 'Imod' THEN vote_count ELSE 0 END) AS total_against_votes
    , SUM(CASE WHEN vote_type = 'Hverken for eller imod' THEN vote_count ELSE 0 END) AS total_abstain_votes
    , SUM(CASE WHEN vote_type = 'Fravær' THEN vote_count ELSE 0 END) AS total_absent_votes
    , to_json(
        list(
            JSON_OBJECT(
                'party_abbr', party_abbr,
                'vote_type', vote_type,
                'votes', vote_count,
                'fill', party_color
            )
            -- 2. Sort the aggregate list by the single, pre-calculated column
            ORDER BY sort_index ASC
        ) FILTER (WHERE vote_type IN ('For', 'Imod'))
    ) AS for_against_votes
    , to_json(
        list(
            JSON_OBJECT(
                'party_abbr', party_abbr,
                'vote_type', vote_type,
                'votes', vote_count,
                'fill', party_color
            )
            -- No 2 sections here, so sort by ltr order ordinarily
            ORDER BY sort_index ASC
        ) FILTER (WHERE vote_type = 'Fravær')
    ) AS absent_votes
FROM prepared_votes
GROUP BY poll_id