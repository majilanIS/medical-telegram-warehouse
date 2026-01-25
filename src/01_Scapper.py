
import os
import json
import asyncio
from datetime import datetime
from telethon import TelegramClient, errors
from telethon.tl.types import MessageMediaPhoto
import logging

# =========================
# CONFIGURATION
# =========================

TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID"))  # from .env
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
DATA_DIR = "data/raw"
IMAGE_DIR = os.path.join(DATA_DIR, "images")
LOG_DIR = "logs"

# Create folders if not exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(IMAGE_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Set up logging
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "scraper.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Telegram channels to scrape
CHANNELS = [
    "lobelia4cosmetics",
    "tikvahpharma",
    "CheMedChannel"  # replace with actual channel username
    # Add more channels here
]

# =========================
# HELPER FUNCTIONS
# =========================

async def download_image(client, message, channel_name):
    """Download photo messages to organized folder."""
    try:
        folder = os.path.join(IMAGE_DIR, channel_name)
        os.makedirs(folder, exist_ok=True)
        path = await message.download_media(file=os.path.join(folder, f"{message.id}.jpg"))
        return path
    except Exception as e:
        logging.error(f"Error downloading image {message.id} from {channel_name}: {e}")
        return None

async def scrape_channel(client, channel_name):
    """Scrape messages from a single Telegram channel."""
    logging.info(f"Starting scrape for channel: {channel_name}")
    all_messages = []

    try:
        async for message in client.iter_messages(channel_name):
            msg_data = {
                "message_id": message.id,
                "channel_name": channel_name,
                "message_date": message.date.isoformat(),
                "message_text": message.text,
                "has_media": bool(message.media),
                "views": message.views,
                "forwards": message.forwards,
                "image_path": None
            }

            # Download image if exists
            if message.media and isinstance(message.media, MessageMediaPhoto):
                msg_data["image_path"] = await download_image(client, message, channel_name)

            all_messages.append(msg_data)

    except errors.FloodWaitError as e:
        logging.warning(f"Rate limited for {e.seconds} seconds on channel {channel_name}")
        await asyncio.sleep(e.seconds)
    except Exception as e:
        logging.error(f"Error scraping channel {channel_name}: {e}")

    # Save messages to JSON in data lake
    if all_messages:
        today = datetime.now().strftime("%Y-%m-%d")
        folder_path = os.path.join(DATA_DIR, "telegram_messages", today)
        os.makedirs(folder_path, exist_ok=True)
        json_file = os.path.join(folder_path, f"{channel_name}.json")
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(all_messages, f, ensure_ascii=False, indent=4)
        logging.info(f"Saved {len(all_messages)} messages for {channel_name} to {json_file}")

# =========================
# MAIN FUNCTION
# =========================

async def main():
    async with TelegramClient("session_name", API_ID, API_HASH) as client:
        tasks = [scrape_channel(client, channel) for channel in CHANNELS]
        await asyncio.gather(*tasks)
    logging.info("Scraping completed for all channels.")

# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    asyncio.run(main())
