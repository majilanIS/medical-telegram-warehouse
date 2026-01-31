WITH source AS (
    SELECT *
    FROM raw.telegram_messages
)

SELECT
    message_id::BIGINT        AS message_id,
    channel_name::TEXT        AS channel_name,
    message_date::TIMESTAMP   AS message_date,
    message_text::TEXT        AS message_text,
    LENGTH(message_text)      AS message_length,
    COALESCE(views, 0)::INT   AS view_count,
    COALESCE(forwards, 0)::INT AS forward_count,
    has_media::BOOLEAN        AS has_image,
    image_path::TEXT          AS image_path
FROM source
WHERE message_text IS NOT NULL
