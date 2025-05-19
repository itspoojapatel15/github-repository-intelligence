{{ config(materialized='table') }}

with commits as (
    select * from {{ ref('stg_commits') }}
),
daily as (
    select
        repo_full_name,
        date_trunc('day', authored_at) as activity_date,
        count(*) as commit_count,
        count(distinct author_email) as unique_authors,
        sum(additions) as total_additions,
        sum(deletions) as total_deletions,
        sum(additions) + sum(deletions) as total_churn
    from commits
    where authored_at is not null
    group by 1, 2
)
select * from daily
