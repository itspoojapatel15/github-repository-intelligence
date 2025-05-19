with source as (
    select raw_data, loaded_at from {{ source('raw_github', 'ISSUES') }}
),
parsed as (
    select
        raw_data:issue_id::integer as issue_id,
        raw_data:number::integer as issue_number,
        raw_data:repo_full_name::varchar as repo_full_name,
        raw_data:title::varchar as title,
        raw_data:state::varchar as state,
        raw_data:author::varchar as author,
        raw_data:comments::integer as comment_count,
        raw_data:created_at::timestamp_tz as created_at,
        raw_data:updated_at::timestamp_tz as updated_at,
        raw_data:closed_at::timestamp_tz as closed_at,
        loaded_at,
        row_number() over (partition by raw_data:issue_id::integer order by loaded_at desc) as _rn
    from source
)
select * from parsed where _rn = 1
