{{ config(materialized='table') }}

with repos as (
    select * from {{ ref('stg_repositories') }}
),
final as (
    select
        repo_id, full_name, name, owner, description, language,
        stars, forks, watchers, open_issues, size_kb,
        created_at, updated_at, pushed_at,
        default_branch, license, is_fork, is_archived,
        datediff('day', created_at, current_timestamp()) as age_days,
        case
            when stars >= 10000 then 'mega'
            when stars >= 1000 then 'popular'
            when stars >= 100 then 'notable'
            else 'emerging'
        end as popularity_tier,
        case when forks > 0 then stars::float / forks else 0 end as star_fork_ratio
    from repos
    where not is_fork and not is_archived
)
select * from final
