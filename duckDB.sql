CREATE OR REPLACE VIEW filtered_comments AS
SELECT
  repo.name AS repo,
  id AS event_id,
  type AS event_type,
  payload.comment.id AS comment_id,
  payload.comment.html_url AS comment_url,
  payload.issue.id AS issue_or_pr_id,
  payload.comment.user.login AS user_login,
  payload.comment.created_at AS created_at,
  TRIM(payload.comment.body) AS text
FROM read_ndjson_auto('/gh_data/2019/2019-*.json.gz')
WHERE
  type IN ('IssueCommentEvent', 'PullRequestReviewCommentEvent')
  AND payload.comment IS NOT NULL
  AND payload.comment.body IS NOT NULL
  AND LENGTH(TRIM(payload.comment.body)) > 0
  AND payload.issue IS NOT NULL


CREATE OR REPLACE VIEW filtered_comments_no_bot AS
SELECT *
FROM filtered_comments
WHERE NOT (
    LOWER(user_login) LIKE '%[bot]%' OR
    LOWER(user_login) LIKE '%bot' OR
    LOWER(user_login) LIKE 'bot_%' OR
    LOWER(user_login) LIKE 'bot-%' OR
    LOWER(user_login) LIKE '%-bot%' OR
    LOWER(user_login) LIKE '%_bot%' OR
    LOWER(user_login) IN ('dependabot', 'github-actions', 'renovate', 'semantic-release', 'prettier-ci')
)


CREATE OR REPLACE VIEW filtered_comments_repo AS
SELECT c.*
FROM filtered_comments_no_bot c
INNER JOIN read_parquet('/repo/github_ml_repos_2019.parquet') ml
ON c.repo = ml.full_name


COPY filtered_comments_repo
TO '/score_ml/2019.parquet' (FORMAT 'parquet');
