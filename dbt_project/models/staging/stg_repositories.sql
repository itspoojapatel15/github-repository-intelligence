with source as (
    select raw_data, loaded_at from {{ source('raw_github', 'REPOSITORIES') }}
),
parsed as (
    select
        raw_data:repo_id::integer as repo_id,
        raw_data:full_name::varchar as full_name,
        raw_data:name::varchar as name,
        raw_data:owner::varchar as owner,
        raw_data:description::varchar as description,
        raw_data:language::varchar as language,
        raw_data:stars::integer as stars,
        raw_data:forks::integer as forks,
        raw_data:watchers::integer as watchers,
        raw_data:open_issues::integer as open_issues,
        raw_data:size_kb::integer as size_kb,
        raw_data:created_at::timestamp_tz as created_at,
        raw_data:updated_at::timestamp_tz as updated_at,
        raw_data:pushed_at::timestamp_tz as pushed_at,
        raw_data:default_branch::varchar as default_branch,
        raw_data:license::varchar as license,
        raw_data:is_fork::boolean as is_fork,
        raw_data:archived::boolean as is_archived,
        raw_data:extracted_at::timestamp_tz as extracted_at,
        loaded_at,
        row_number() over (partition by raw_data:repo_id::integer order by loaded_at desc) as _rn
    from source
)
select * from parsed where _rn = 1
