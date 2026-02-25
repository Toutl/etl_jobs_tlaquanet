MERGE INTO dim_users tgt
USING stg_users src
ON tgt.user_id = src.user_id
AND tgt.is_current = TRUE

WHEN MATCHED AND tgt.display_name <> src.display_name THEN
    UPDATE SET
        tgt.to_date = CURRENT_TIMESTAMP(),
        tgt.is_current = FALSE
WHEN NOT MATCHED THEN
    INSERT (
        user_id,
        username,
        display_name,
        created_at,
        from_date,
        to_date,
        is_current
    )
    VALUES(
        src.user_id,
        src.username,
        src.display_name,
        src.created_at,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );