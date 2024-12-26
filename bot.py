#!/usr/bin/env python3
import os
import json
import asyncio
import logging
from google.cloud import storage
from google.oauth2 import service_account
from pyrogram import Client

# Basic logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Telegram credentials - from environment variables
API_ID = os.getenv("API_ID", "27783825")
API_HASH = os.getenv("API_HASH", "22995583ea31fed17fa5e92b8d33c1c6")
BOT_TOKEN = os.getenv("BOT_TOKEN", "7834378427:AAE88n5PzOFCQK-Py44YV4kvM_FYeqd6P8I")
TARGET_CHANNEL = os.getenv("TARGET_CHANNEL", "pregnantpetite")

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
        # Initialize Telegram client
        self.app = Client(
            "bot",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            in_memory=True
        )
        
        # Initialize Google Cloud Storage client
        try:
            # Get credentials from environment variable
            creds_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            if creds_json:
                # Parse the JSON string into a dictionary
                creds_dict = json.loads(creds_json)
                # Create credentials object
                credentials = service_account.Credentials.from_service_account_info(creds_dict)
                # Create storage client with credentials
                self.storage_client = storage.Client(credentials=credentials)
            else:
                # Fallback to default credentials
                self.storage_client = storage.Client()
            
            self.bucket = self.storage_client.bucket(BUCKET_NAME)
            logger.info("Successfully connected to Google Cloud Storage")
        except Exception as e:
            logger.error(f"Failed to initialize Google Cloud Storage: {e}")
            raise

    async def upload_file(self, blob):
        """Upload a single file to Telegram."""
        # Create temp directory if it doesn't exist
        os.makedirs("/tmp", exist_ok=True)
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
            try:
                if ext in ['.jpg', '.jpeg', '.png']:
                    await self.app.send_photo(TARGET_CHANNEL, photo=temp_file, caption=caption)
                elif ext in ['.mp4', '.mov', '.avi', '.mkv']:
                    await self.app.send_video(TARGET_CHANNEL, video=temp_file, caption=caption)
                else:
                    await self.app.send_document(TARGET_CHANNEL, document=temp_file, caption=caption)
                
                logger.info(f"Successfully uploaded: {blob.name}")
                return True
            except Exception as e:
                logger.error(f"Failed to upload to Telegram: {e}")
                return False

        except Exception as e:
            logger.error(f"Error processing {blob.name}: {e}")
            return False
        finally:
            # Clean up temp file
            if os.path.exists(temp_file):
                os.remove(temp_file)
                logger.info(f"Cleaned up temporary file: {temp_file}")

    async def run(self):
        """Main upload loop."""
        logger.info("Starting upload process...")
        async with self.app:
            for folder in FOLDERS:
                try:
                    # List files in folder
                    blobs = list(self.bucket.list_blobs(prefix=folder))
                    logger.info(f"Found {len(blobs)} files in {folder}")

                    # Upload each file
                    for blob in blobs:
                        if not blob.name.endswith('.DS_Store'):
                            success = await self.upload_file(blob)
                            if success:
                                logger.info(f"Waiting 60 seconds before next upload...")
                                await asyncio.sleep(60)  # Wait 1 minute between uploads

                except Exception as e:
                    logger.error(f"Error processing folder {folder}: {e}")
                    continue

async def main():
    try:
        logger.info("Initializing uploader...")
        uploader = Uploader()
        await uploader.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 