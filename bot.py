#!/usr/bin/env python3
import os
import random
import asyncio
import logging
import shutil
import time
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List

# Logging Configuration first, so we can log any import errors
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Check for Google Cloud credentials
if not os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
    logger.error("GOOGLE_APPLICATION_CREDENTIALS environment variable not set!")
    try:
        # Try to parse from environment variable as JSON
        creds_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
        if creds_json:
            creds_dict = json.loads(creds_json)
            temp_creds_file = "/tmp/google_creds.json"
            with open(temp_creds_file, 'w') as f:
                json.dump(creds_dict, f)
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_creds_file
            logger.info("Successfully loaded Google credentials from environment JSON")
    except Exception as e:
        logger.error(f"Failed to parse Google credentials: {e}")
        raise

try:
    from google.cloud import storage
except ImportError:
    raise ImportError("The google-cloud-storage package is required. Install it using 'pip install google-cloud-storage'.")

try:
    from pyrogram import Client
except ImportError:
    raise ImportError("The pyrogram package is required. Install it using 'pip install pyrogram'.")

import ffmpeg
import pytz

# ------------------------------ Configuration ------------------------------

# API credentials for Telegram
API_ID = os.getenv("API_ID", "27783825")
API_HASH = os.getenv("API_HASH", "22995583ea31fed17fa5e92b8d33c1c6")
BOT_TOKEN = os.getenv("BOT_TOKEN", "7834378427:AAE88n5PzOFCQK-Py44YV4kvM_FYeqd6P8I")
TARGET_CHANNEL = os.getenv("TARGET_CHANNEL", "pregnantpetite")

logger.info("Loaded environment variables:")
logger.info(f"API_ID: {'Set' if API_ID else 'Not Set'}")
logger.info(f"API_HASH: {'Set' if API_HASH else 'Not Set'}")
logger.info(f"BOT_TOKEN: {'Set' if BOT_TOKEN else 'Not Set'}")
logger.info(f"TARGET_CHANNEL: {TARGET_CHANNEL}")

# GCS Bucket and folder information
BUCKET_NAME = "tglyon"
FOLDERS = [
    "source/allierivers/",
    "source/emarusova/",
    "source/konekoshinji/",
    "source/koreanspecial/",
    "source/pregnantprincessxx/",
    "source/vickyaisha/",
]

# Constants
TEMP_DIR = "/tmp"
os.makedirs(TEMP_DIR, exist_ok=True)  # Ensure temp directory exists
SENT_FILES_RECORD = os.path.join(TEMP_DIR, "sent_files.txt")
MAX_DURATION = 300  # Maximum video duration in seconds (5 minutes)
MAX_FILE_SIZE = 2000 * 1024 * 1024  # 2GB max file size for Telegram
MAX_RETRIES = 3

# Cycle Configuration
UPLOADS_PER_CYCLE = 2
INTER_UPLOAD_DELAY = 60
CYCLE_DELAY = 300
TOTAL_CYCLES = 3

# PST Timezone
PST_TZ = ZoneInfo("America/Los_Angeles")

# ------------------------------ Helper Functions ------------------------------

def download_with_retry(blob, temp_file, retries=3, backoff_factor=2) -> bool:
    """Downloads a file from GCS with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            blob.download_to_filename(temp_file)
            logger.info(f"Downloaded {blob.name} to {temp_file}")
            return True
        except Exception as e:
            logger.error(f"Download failed for {blob.name} (Attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                sleep_time = backoff_factor ** attempt
                logger.info(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
    logger.error(f"Failed to download {blob.name} after {retries} attempts.")
    return False

def get_caption(media_blob):
    """
    Constructs the caption for a media blob by reading the corresponding text file.
    Format: "(content from .txt file) #foldername"
    """
    folder_name = media_blob.name.split('/')[1]

    try:
        blobs_in_folder = list(storage.Client().bucket(BUCKET_NAME).list_blobs(prefix=f"source/{folder_name}/"))
    except Exception as e:
        logger.error(f"Error fetching blobs for caption: {e}")
        return f"#{folder_name}"

    text_blob = next((b for b in blobs_in_folder if b.name.endswith('.txt')), None)

    if text_blob:
        temp_text_file = os.path.join(TEMP_DIR, text_blob.name.replace('/', '_'))
        if download_with_retry(text_blob, temp_text_file):
            try:
                with open(temp_text_file, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
            except Exception as e:
                logger.error(f"Error reading caption from {text_blob.name}: {e}")
                content = ""
            finally:
                if os.path.exists(temp_text_file):
                    os.remove(temp_text_file)

            if content:
                return f"{content[:150]} #{folder_name}"

    logger.warning(f"No caption content found for {media_blob.name}. Using default caption.")
    return f"#{folder_name}"

def load_sent_files(record_path: str) -> set:
    """Loads the list of sent files from the record file."""
    if os.path.exists(record_path):
        try:
            with open(record_path, "r") as f:
                return set(f.read().splitlines())
        except Exception as e:
            logger.error(f"Error loading sent files record: {e}")
    return set()

def add_sent_file(record_path: str, file_path: str):
    """Adds a file to the sent files record."""
    try:
        with open(record_path, "a") as f:
            f.write(file_path + "\n")
        logger.info(f"Marked {file_path} as sent.")
    except Exception as e:
        logger.error(f"Error updating sent files record: {e}")

# ------------------------------ Main Class ------------------------------

class TelegramUploader:
    def __init__(self):
        session_name = f"uploader_bot_{int(time.time())}"
        self.app = Client(
            session_name,
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            in_memory=True
        )
        logger.info(f"Initializing Telegram client with session: {session_name}")
        
        try:
            self.storage_client = storage.Client()
            self.bucket = self.storage_client.bucket(BUCKET_NAME)
            logger.info(f"Successfully connected to Google Cloud Storage bucket: {BUCKET_NAME}")
        except Exception as e:
            logger.error(f"Failed to initialize Google Cloud Storage: {e}")
            raise
            
        self.sent_files = load_sent_files(SENT_FILES_RECORD)
        logger.info(f"Loaded {len(self.sent_files)} sent files from record")

    def get_files_from_gcs(self, prefix: str) -> List[storage.Blob]:
        """Fetches files from a GCS bucket with a given prefix."""
        try:
            blobs = list(self.bucket.list_blobs(prefix=prefix))
            blobs = [blob for blob in blobs if not blob.name.endswith('.DS_Store')]
            logger.info(f"Fetched {len(blobs)} files from {prefix}")
            return blobs
        except Exception as e:
            logger.error(f"Error fetching files from {prefix}: {e}")
            return []

    async def upload_file(self, blob: storage.Blob) -> bool:
        """Downloads and uploads a file to Telegram."""
        if blob.size > MAX_FILE_SIZE:
            logger.warning(f"File {blob.name} exceeds maximum size limit of {MAX_FILE_SIZE/(1024*1024)}MB")
            return False

        temp_file = os.path.join(TEMP_DIR, f"{int(time.time())}_{blob.name.replace('/', '_')}")
        try:
            if not download_with_retry(blob, temp_file):
                return False

            caption = get_caption(blob)
            logger.info(f"Using caption: {caption}")

            for attempt in range(MAX_RETRIES):
                try:
                    _, ext = os.path.splitext(blob.name.lower())
                    if ext in ['.jpg', '.jpeg', '.png']:
                        await self.app.send_photo(TARGET_CHANNEL, photo=temp_file, caption=caption)
                    elif ext in ['.mp4', '.mov', '.avi', '.mkv']:
                        await self.app.send_video(TARGET_CHANNEL, video=temp_file, caption=caption)
                    else:
                        await self.app.send_document(TARGET_CHANNEL, document=temp_file, caption=caption)

                    logger.info(f"[SUCCESS] Uploaded: {blob.name}")
                    add_sent_file(SENT_FILES_RECORD, blob.name)
                    return True
                except Exception as e:
                    if attempt < MAX_RETRIES - 1:
                        logger.warning(f"Upload attempt {attempt + 1} failed for {blob.name}: {e}. Retrying...")
                        await asyncio.sleep(2 ** attempt)
                    else:
                        logger.error(f"[ERROR] Upload failed after {MAX_RETRIES} attempts for {blob.name}: {e}")
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
                logger.debug(f"Cleaned up temporary file: {temp_file}")
        
        return False

    async def run_cycle(self, cycle_number: int):
        """Performs a single upload cycle."""
        random.shuffle(FOLDERS)
        uploads_done = 0

        logger.info(f"--- Starting Cycle {cycle_number + 1} ---")

        for folder in FOLDERS:
            if uploads_done >= UPLOADS_PER_CYCLE:
                break
            blobs = self.get_files_from_gcs(folder)
            for blob in blobs:
                if blob.name in self.sent_files:
                    continue

                success = await self.upload_file(blob)
                if success:
                    uploads_done += 1
                    logger.info(f"Uploaded {blob.name} from folder {folder}")
                    if uploads_done >= UPLOADS_PER_CYCLE:
                        break
                    logger.info(f"Waiting {INTER_UPLOAD_DELAY} seconds before next upload...")
                    await asyncio.sleep(INTER_UPLOAD_DELAY)

        if uploads_done == 0:
            logger.warning(f"No suitable files were uploaded in cycle {cycle_number + 1}.")
        else:
            logger.info(f"--- Cycle {cycle_number + 1} Complete: {uploads_done} uploads done ---")

    async def run(self):
        """Runs the uploader for a fixed number of cycles."""
        logger.info("Starting Telegram Uploader...")
        async with self.app:
            for cycle in range(TOTAL_CYCLES):
                await self.run_cycle(cycle)
                if cycle < TOTAL_CYCLES - 1:
                    logger.info(f"Waiting {CYCLE_DELAY / 60} minutes before next cycle.")
                    await asyncio.sleep(CYCLE_DELAY)
            logger.info("All cycles completed. Exiting uploader.")

# ------------------------------ Execution ------------------------------

async def main():
    logger.info("[START] Starting Telegram Uploader for 3 cycles...")
    try:
        uploader = TelegramUploader()
        await uploader.run()
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main()) 