{{ config(materialized='table') }}

with commits as (
    select * from {{ ref('stg_commits') }}
),
contributor_stats as (
    select
        author_email,
        author_name,
        count(*) as total_commits,
        count(distinct repo_full_name) as repos_contributed_to,
        sum(additions) as total_additions,
        sum(deletions) as total_deletions,
        min(authored_at) as first_commit_at,
        max(authored_at) as last_commit_at,
        datediff('day', min(authored_at), max(authored_at)) as active_days
    from commits
    where author_email is not null
    group by 1, 2
)
select * from contributor_stats
