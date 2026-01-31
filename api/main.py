# api/main.py

from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from pydantic import BaseModel
from typing import List, Optional

# -------------------------------
# Database configuration
# -------------------------------
DATABASE_URL = "postgresql+psycopg2://postgres:postgres@2025@localhost:5432/medical_telegram_db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# -------------------------------
# FastAPI app
# -------------------------------
app = FastAPI(title="Medical Telegram Analytics API", version="1.0")

# -------------------------------
# Pydantic Schemas
# -------------------------------
class TopProduct(BaseModel):
    product: str
    mentions: int

class ChannelActivity(BaseModel):
    channel_name: str
    total_posts: int
    avg_views: Optional[float] = None

class MessageSearchResult(BaseModel):
    message_id: str
    channel_name: str
    message_text: str
    message_date: str

class VisualContentStats(BaseModel):
    channel_name: str
    total_messages: int
    messages_with_images: int
    image_percentage: float

# -------------------------------
# Helper function
# -------------------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# -------------------------------
# Endpoints
# -------------------------------

@app.get("/api/reports/top-products", response_model=List[TopProduct])
def top_products(limit: int = Query(10, description="Number of top products to return")):
    query = text("""
        SELECT unnest(string_to_array(message_text, ' ')) AS product, COUNT(*) AS mentions
        FROM raw.telegram_messages
        GROUP BY product
        ORDER BY mentions DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"limit": limit}).fetchall()
        return [TopProduct(product=row[0], mentions=row[1]) for row in result]

@app.get("/api/channels/{channel_name}/activity", response_model=ChannelActivity)
def channel_activity(channel_name: str):
    query = text("""
        SELECT
            channel_name,
            COUNT(*) AS total_posts,
            AVG(views) AS avg_views
        FROM raw.telegram_messages
        WHERE channel_name = :channel_name
        GROUP BY channel_name
    """)
    with engine.connect() as conn:
        row = conn.execute(query, {"channel_name": channel_name}).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Channel not found")
        return ChannelActivity(
            channel_name=row[0],
            total_posts=row[1],
            avg_views=row[2]
        )

@app.get("/api/search/messages", response_model=List[MessageSearchResult])
def search_messages(query: str, limit: int = Query(20, description="Max number of messages to return")):
    sql = text("""
        SELECT message_id, channel_name, message_text, message_date
        FROM raw.telegram_messages
        WHERE message_text ILIKE :search
        ORDER BY message_date DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"search": f"%{query}%", "limit": limit}).fetchall()
        return [
            MessageSearchResult(
                message_id=row[0],
                channel_name=row[1],
                message_text=row[2],
                message_date=row[3].isoformat() if row[3] else None
            )
            for row in rows
        ]

@app.get("/api/reports/visual-content", response_model=List[VisualContentStats])
def visual_content_stats():
    query = text("""
        SELECT
            channel_name,
            COUNT(*) AS total_messages,
            SUM(CASE WHEN has_media THEN 1 ELSE 0 END) AS messages_with_images,
            ROUND(SUM(CASE WHEN has_media THEN 1 ELSE 0 END)::decimal / COUNT(*) * 100, 2) AS image_percentage
        FROM raw.telegram_messages
        GROUP BY channel_name
    """)
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()
        return [
            VisualContentStats(
                channel_name=row[0],
                total_messages=row[1],
                messages_with_images=row[2],
                image_percentage=float(row[3])
            )
            for row in rows
        ]
