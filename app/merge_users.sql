-- Single Statement SCD Type 2 Merge for Users
MERGE INTO dim_users tgt
USING (
    -- First branch: All records from staging
    -- These will trigger UPDATEs for changed users and INSERTs for brand new users
    SELECT 
        user_id as merge_key,
        user_id, user_name, display_name, created_at
    FROM stg_users
    
    UNION ALL
    
    -- Second branch: Records intended to be the NEW version of updated users
    -- We use NULL as merge_key so it 'fails' the match and triggers a NEW INSERT
    SELECT
        NULL as merge_key,
        src.user_id, src.username, src.display_name, src.created_at
    FROM stg_users src
    JOIN dim_users tgt ON src.user_id = tgt.USER_ID
    WHERE tgt.is_current = TRUE
        AND (tgt.display_name <> src.display_name OR tgt.username <> src.username)
) src

ON tgt.user_id = src.merge_key AND tgt.is_current = TRUE
WHEN MATCHED AND (tgt.display_name <> src.display_name OR tgt.username <> src.username) THEN
    UPDATE SET
        to_date = CURRENT_TIMESTAMP(),
        is_current = FALSE
WHEN NOT MATCHED THEN
    INSERT (user_id, username, display_name, created_at, from_date, to_date, is_current)
    VALUES(src.user_id, src.username, src.display_name, src.created_at, CURRENT_TIMESTAMP(), NULL, TRUE);
