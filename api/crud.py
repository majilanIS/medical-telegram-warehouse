from sqlalchemy.orm import Session
from sqlalchemy import text

def get_top_products(db: Session, limit: int = 10):
    query = text("""
        SELECT unnest(string_to_array(lower(message_text), ' ')) AS product
        FROM raw.telegram_messages
    """)
    result = db.execute(query)
    counts = {}
    for row in result:
        word = row[0]
        counts[word] = counts.get(word, 0) + 1
    top = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:limit]
    return [{"product": p, "mentions": m} for p, m in top]

def get_channel_activity(db: Session, channel_name: str):
    query = text("""
        SELECT date(message_date) as date, count(*) as message_count
        FROM raw.telegram_messages
        WHERE channel_name = :channel_name
        GROUP BY date
        ORDER BY date
    """)
    result = db.execute(query, {"channel_name": channel_name})
    return [{"date": row[0].isoformat(), "message_count": row[1]} for row in result]
