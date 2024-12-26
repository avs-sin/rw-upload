#!/usr/bin/env python3
import os
import random
import asyncio
import logging
import shutil
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List

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
API_ID = "27783825"
API_HASH = "22995583ea31fed17fa5e92b8d33c1c6"
BOT_TOKEN = "7834378427:AAE88n5PzOFCQK-Py44YV4kvM_FYeqd6P8I"
TARGET_CHANNEL = "pregnantpetite"

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

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# ------------------------------ Helper Functions ------------------------------

def download_with_retry(blob, temp_file, retries=3, backoff_factor=2) -> bool:
    """Downloads a file from GCS with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            blob.download_to_filename(temp_file)
            logging.info(f"Downloaded {blob.name} to {temp_file}")
            return True
        except Exception as e:
            logging.error(f"Download failed for {blob.name} (Attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                sleep_time = backoff_factor ** attempt
                logging.info(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
    logging.error(f"Failed to download {blob.name} after {retries} attempts.")
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
        logging.error(f"Error fetching blobs for caption: {e}")
        return f"#{folder_name}"

    text_blob = next((b for b in blobs_in_folder if b.name.endswith('.txt')), None)

    if text_blob:
        temp_text_file = os.path.join(TEMP_DIR, text_blob.name.replace('/', '_'))
        if download_with_retry(text_blob, temp_text_file):
            try:
                with open(temp_text_file, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
            except Exception as e:
                logging.error(f"Error reading caption from {text_blob.name}: {e}")
                content = ""
            finally:
                os.remove(temp_text_file)

            if content:
                return f"{content[:150]} #{folder_name}"

    logging.warning(f"No caption content found for {media_blob.name}. Using default caption.")
    return f"#{folder_name}"

def load_sent_files(record_path: str) -> set:
    """Loads the list of sent files from the record file."""
    if os.path.exists(record_path):
        try:
            with open(record_path, "r") as f:
                return set(f.read().splitlines())
        except Exception as e:
            logging.error(f"Error loading sent files record: {e}")
    return set()

def add_sent_file(record_path: str, file_path: str):
    """Adds a file to the sent files record."""
    try:
        with open(record_path, "a") as f:
            f.write(file_path + "\n")
        logging.info(f"Marked {file_path} as sent.")
    except Exception as e:
        logging.error(f"Error updating sent files record: {e}")

# ------------------------------ Main Class ------------------------------

class TelegramUploader:
    def __init__(self):
        self.app = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(BUCKET_NAME)
        self.sent_files = load_sent_files(SENT_FILES_RECORD)

    def get_files_from_gcs(self, prefix: str) -> List[storage.Blob]:
        """Fetches files from a GCS bucket with a given prefix."""
        try:
            blobs = list(self.bucket.list_blobs(prefix=prefix))
            blobs = [blob for blob in blobs if not blob.name.endswith('.DS_Store')]
            logging.info(f"Fetched {len(blobs)} files from {prefix}")
            return blobs
        except Exception as e:
            logging.error(f"Error fetching files from {prefix}: {e}")
            return []

    async def upload_file(self, blob: storage.Blob) -> bool:
        """Downloads and uploads a file to Telegram."""
        if blob.size > MAX_FILE_SIZE:
            logging.warning(f"File {blob.name} exceeds maximum size limit of {MAX_FILE_SIZE/(1024*1024)}MB")
            return False

        temp_file = os.path.join(TEMP_DIR, f"{int(time.time())}_{blob.name.replace('/', '_')}")
        if not download_with_retry(blob, temp_file):
            return False

        caption = get_caption(blob)
        logging.info(f"Using caption: {caption}")

        for attempt in range(MAX_RETRIES):
            try:
                _, ext = os.path.splitext(blob.name.lower())
                if ext in ['.jpg', '.jpeg', '.png']:
                    await self.app.send_photo(chat_id=TARGET_CHANNEL, photo=temp_file, caption=caption)
                elif ext in ['.mp4', '.mov', '.avi', '.mkv']:
                    await self.app.send_video(chat_id=TARGET_CHANNEL, video=temp_file, caption=caption)
                else:
                    logging.warning(f"Unsupported file extension {ext} for file {blob.name}. Skipping upload.")
                    os.remove(temp_file)
                    return False

                logging.info(f"[SUCCESS] Uploaded: {blob.name}")
                add_sent_file(SENT_FILES_RECORD, blob.name)
                os.remove(temp_file)
                return True
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    logging.warning(f"Upload attempt {attempt + 1} failed for {blob.name}: {e}. Retrying...")
                    await asyncio.sleep(2 ** attempt)
                else:
                    logging.error(f"[ERROR] Upload failed after {MAX_RETRIES} attempts for {blob.name}: {e}")

        if os.path.exists(temp_file):
            os.remove(temp_file)
        return False

    async def run_cycle(self, cycle_number: int):
        """Performs a single upload cycle."""
        random.shuffle(FOLDERS)
        uploads_done = 0

        logging.info(f"--- Starting Cycle {cycle_number + 1} ---")

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
                    logging.info(f"Uploaded {blob.name} from folder {folder}")
                    if uploads_done >= UPLOADS_PER_CYCLE:
                        break
                    await asyncio.sleep(INTER_UPLOAD_DELAY)

        if uploads_done == 0:
            logging.warning(f"No suitable files were uploaded in cycle {cycle_number + 1}.")
        else:
            logging.info(f"--- Cycle {cycle_number + 1} Complete: {uploads_done} uploads done ---")

    async def run(self):
        """Runs the uploader for a fixed number of cycles."""
        async with self.app:
            for cycle in range(TOTAL_CYCLES):
                await self.run_cycle(cycle)
                if cycle < TOTAL_CYCLES - 1:
                    logging.info(f"Waiting for {CYCLE_DELAY / 60} minutes before next cycle.")
                    await asyncio.sleep(CYCLE_DELAY)
            logging.info("All cycles completed. Exiting uploader.")

# ------------------------------ Execution ------------------------------

async def main():
    logging.info("[START] Starting Telegram Uploader for 3 cycles...")
    uploader = TelegramUploader()

    try:
        await uploader.run()
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(main()) 