-- Engagement Aggregation Job
-- Purpose: Calculate user engagement metrics based on their activity (posts, comments, likes)

CREATE OR REPLACE TABLE user_engagement_metrics AS
WITH user_activity AS (
    SELECT
        u.user_id,
        u.username,
        u.display_name,
        COUNT(DISTINCT p.id) as total_posts,
        COUNT(DISTINCT c.id) as total_comments,
        COUNT(DISTINCT l.id) as total_likes
    FROM dim_users u
    LEFT JOIN posts p
        ON u.user_id = p.author_id
    LEFT JOIN comments c
        ON u.user_id = c.author_id
    LEFT JOIN likes l
        ON u.user_id = l.user_id
    WHERE u.is_current = TRUE
    GROUP BY u.user_id, u.username, u.display_name
)
SELECT
    *,
    (total_posts * 10 + total_comments * 5 + total_likes * 2) as engagement_score,
    CURRENT_TIMESTAMP() as calculated_at
FROM user_activity
ORDER BY engagement_score DESC;
