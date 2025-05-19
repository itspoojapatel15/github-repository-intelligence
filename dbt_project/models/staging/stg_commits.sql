with source as (
    select raw_data, loaded_at from {{ source('raw_github', 'COMMITS') }}
),
parsed as (
    select
        raw_data:sha::varchar as sha,
        raw_data:repo_full_name::varchar as repo_full_name,
        raw_data:message::varchar as message,
        raw_data:author_name::varchar as author_name,
        raw_data:author_email::varchar as author_email,
        raw_data:author_date::timestamp_tz as authored_at,
        raw_data:additions::integer as additions,
        raw_data:deletions::integer as deletions,
        loaded_at,
        row_number() over (partition by raw_data:sha::varchar order by loaded_at desc) as _rn
    from source
)
select * from parsed where _rn = 1
