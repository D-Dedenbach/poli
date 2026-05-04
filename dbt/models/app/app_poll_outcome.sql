SELECT poll_id
    , party_abbr
    , vote_type
    , poll_type
    , meeting_date
    , title
    , adopted
    , COUNT(DISTINCT vote_id) AS vote_count


FROM {{ ref('int_poll_votes') }}
GROUP BY poll_id, party_abbr, vote_type, poll_type, meeting_date, title, adopted