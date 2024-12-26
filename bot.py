#!/usr/bin/env python3
import os
import asyncio
import logging
from google.cloud import storage
from pyrogram import Client

# Basic logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Telegram credentials
API_ID = "27783825"
API_HASH = "22995583ea31fed17fa5e92b8d33c1c6"
BOT_TOKEN = "7834378427:AAE88n5PzOFCQK-Py44YV4kvM_FYeqd6P8I"
TARGET_CHANNEL = "pregnantpetite"

# Google Cloud Storage settings
BUCKET_NAME = "tglyon"
FOLDERS = [
    "source/allierivers/",
    "source/emarusova/",
    "source/konekoshinji/",
    "source/koreanspecial/",
    "source/pregnantprincessxx/",
    "source/vickyaisha/",
]

class Uploader:
    def __init__(self):
        self.app = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(BUCKET_NAME)

    async def upload_file(self, blob):
        """Upload a single file to Telegram."""
        temp_file = f"/tmp/{blob.name.replace('/', '_')}"
        try:
            # Download from GCS
            blob.download_to_filename(temp_file)
            logger.info(f"Downloaded: {blob.name}")

            # Get folder name for hashtag
            folder_name = blob.name.split('/')[1]
            caption = f"#{folder_name}"

            # Upload to Telegram
            ext = os.path.splitext(blob.name.lower())[1]
            if ext in ['.jpg', '.jpeg', '.png']:
                await self.app.send_photo(TARGET_CHANNEL, photo=temp_file, caption=caption)
            elif ext in ['.mp4', '.mov', '.avi', '.mkv']:
                await self.app.send_video(TARGET_CHANNEL, video=temp_file, caption=caption)
            else:
                await self.app.send_document(TARGET_CHANNEL, document=temp_file, caption=caption)

            logger.info(f"Uploaded: {blob.name}")
            return True

        except Exception as e:
            logger.error(f"Error uploading {blob.name}: {e}")
            return False
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)

    async def run(self):
        """Main upload loop."""
        async with self.app:
            for folder in FOLDERS:
                try:
                    # List files in folder
                    blobs = list(self.bucket.list_blobs(prefix=folder))
                    logger.info(f"Found {len(blobs)} files in {folder}")

                    # Upload each file
                    for blob in blobs:
                        if not blob.name.endswith('.DS_Store'):
                            await self.upload_file(blob)
                            await asyncio.sleep(60)  # Wait 1 minute between uploads

                except Exception as e:
                    logger.error(f"Error processing folder {folder}: {e}")
                    continue

async def main():
    uploader = Uploader()
    await uploader.run()

if __name__ == "__main__":
    asyncio.run(main()) 