WITH poll_meeting as 
(
SELECT P.poll_id
    , P.case_step_id
    , V.vote_id
    , V.vote_type
    , V.actor_id
    , P.adopted
    , P.poll_type
    , M.meeting_date
    , M.period_id
    , M.title

FROM {{ ref('stg_poll') }} P
INNER JOIN {{ ref('stg_meeting') }} M ON P.meeting_id = M.meeting_id
INNER JOIN {{ ref('stg_vote') }} V ON V.poll_id = P.poll_id
),

agg_votes AS (
SELECT PM.poll_id
    , PM.case_step_id
    , PM.vote_type
    , PM.adopted
    , PM.poll_type
    , PM.meeting_date
    , PM.period_id
    , PM.title
    , MEM.party_abbr
    , count(DISTINCT PM.vote_id) AS vote_count



FROM poll_meeting PM
INNER JOIN {{ ref('int_parliament_members') }} MEM ON PM.actor_id = MEM.member_id 
                                                    AND PM.meeting_date BETWEEN MEM.record_validity_start_date AND MEM.record_validity_end_date
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)

SELECT poll_id
    , case_step_id
    , vote_type
    , adopted
    , poll_type
    , meeting_date
    , period_id
    , title
    , SUM(vote_count) AS total_type_votes
    , JSON_GROUP_ARRAY(
        JSON_OBJECT(
            'party_abbr', party_abbr,
            'votes', vote_count
        )
    ) AS type_votes

FROM agg_votes
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8