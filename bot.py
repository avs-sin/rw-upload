#!/usr/bin/env python3
import os
import json
import logging
import asyncio
from google.cloud import storage
from pyrogram import Client
from datetime import datetime
import pytz

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
TARGET_CHANNEL = os.getenv("TARGET_CHANNEL")
BUCKET_NAME = "tglyon"

# Category mapping for hashtags
CATEGORY_MAPPING = {
    'allierivers': '#pregnant',
    'emarusova': '#petite',
    'konekoshinji': '#asian',
    'koreanspecial': '#korean',
    'pregnantprincessxx': '#pregnant',
    'vickyaisha': '#petite'
}

class TelegramUploader:
    def __init__(self):
        # Initialize Pyrogram client
        self.app = Client("uploader_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
        
        # Initialize Google Cloud Storage client
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(BUCKET_NAME)

    def get_caption(self, blob_name: str) -> str:
        """
        Constructs the caption for a media blob by reading the corresponding text file.
        Format: "(content from .txt file) #source_name #category"
        """
        # Extract folder name from blob path
        parts = blob_name.split('/')
        try:
            folder_name = parts[0]  # First part of the path is the folder name
        except (IndexError):
            logger.warning(f"Unable to extract folder name from path: {blob_name}")
            folder_name = "content"

        # Get category based on folder name
        category = CATEGORY_MAPPING.get(folder_name)

        try:
            # Find and read the corresponding text file
            txt_blob_name = os.path.splitext(blob_name)[0] + '.txt'
            txt_blob = self.bucket.blob(txt_blob_name)
            
            if txt_blob.exists():
                content = txt_blob.download_as_text().strip()
                # Clean up spacing around emojis
                content = content.replace(" 🔥", "🔥")
                content = content.replace("🔥 ", "🔥")
                content = content.replace("🔥", " 🔥")
                
                if content:
                    # Limit total caption length, prioritizing content
                    if category:
                        caption = f"{content[:100]} #{folder_name} {category}"
                    else:
                        caption = f"{content[:100]} #{folder_name}"
                    return caption.strip()

        except Exception as e:
            logger.warning(f"Could not fetch caption for {blob_name}: {e}")
        
        # Fallback caption if text file is missing or empty
        if category:
            logger.warning(f"No caption content found for {blob_name}. Using default hashtags.")
            return f"#{folder_name} {category}"
        else:
            logger.warning(f"No caption content found and no category for {blob_name}. Using folder hashtag only.")
            return f"#{folder_name}"

    async def upload_media(self, blob_name: str) -> bool:
        """Upload a single media file from GCS to Telegram"""
        try:
            # Get the blob
            blob = self.bucket.blob(blob_name)
            if not blob.exists():
                logger.error(f"Blob does not exist: {blob_name}")
                return False

            # Download to temporary file
            file_extension = os.path.splitext(blob_name)[1].lower()
            temp_path = f"temp_{datetime.now().strftime('%Y%m%d_%H%M%S')}{file_extension}"
            
            logger.info(f"Downloading {blob_name} to {temp_path}")
            blob.download_to_filename(temp_path)

            # Get caption
            caption = self.get_caption(blob_name)
            logger.info(f"Caption: {caption}")

            try:
                # Upload to Telegram based on file type
                if file_extension in ['.jpg', '.jpeg', '.png']:
                    await self.app.send_photo(
                        chat_id=TARGET_CHANNEL,
                        photo=temp_path,
                        caption=caption
                    )
                elif file_extension in ['.mp4', '.mov']:
                    await self.app.send_video(
                        chat_id=TARGET_CHANNEL,
                        video=temp_path,
                        caption=caption,
                        supports_streaming=True
                    )
                logger.info(f"Successfully uploaded {blob_name}")
                return True

            finally:
                # Clean up temporary file
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                    logger.info(f"Cleaned up temporary file: {temp_path}")

        except Exception as e:
            logger.error(f"Error uploading {blob_name}: {e}")
            return False

    async def process_uploads(self):
        """Process uploads from GCS to Telegram"""
        try:
            # List all blobs in bucket
            blobs = list(self.bucket.list_blobs())
            media_blobs = [blob.name for blob in blobs 
                         if blob.name.lower().endswith(('.mp4', '.mov', '.jpg', '.jpeg', '.png'))
                         and not blob.name.startswith('sent/')]

            if not media_blobs:
                logger.info("No media files found in bucket")
                return

            logger.info(f"Found {len(media_blobs)} media files to process")

            # Process each media file
            for blob_name in media_blobs:
                success = await self.upload_media(blob_name)
                if success:
                    # Move to sent folder
                    source_blob = self.bucket.blob(blob_name)
                    new_name = f"sent/{blob_name}"
                    
                    # Move the media file
                    new_blob = self.bucket.copy_blob(source_blob, self.bucket, new_name)
                    source_blob.delete()
                    
                    # Move the caption file if it exists
                    txt_name = os.path.splitext(blob_name)[0] + '.txt'
                    txt_blob = self.bucket.blob(txt_name)
                    if txt_blob.exists():
                        new_txt_name = f"sent/{txt_name}"
                        new_txt_blob = self.bucket.copy_blob(txt_blob, self.bucket, new_txt_name)
                        txt_blob.delete()
                    
                    logger.info(f"Moved {blob_name} to sent folder")
                    
                    # Wait between uploads
                    await asyncio.sleep(180)  # 3 minutes between uploads

        except Exception as e:
            logger.error(f"Error in process_uploads: {e}")

async def main():
    """Main function to run the uploader"""
    if not all([API_ID, API_HASH, BOT_TOKEN, TARGET_CHANNEL]):
        logger.error("Missing required environment variables")
        return

    uploader = TelegramUploader()
    
    try:
        async with uploader.app:
            logger.info("Starting upload process")
            await uploader.process_uploads()
            logger.info("Upload process completed")
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    asyncio.run(main()) 