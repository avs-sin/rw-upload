# Telegram Media Uploader Bot

A Telegram bot that automatically uploads media files (photos, videos, documents) to a specified channel.

## Features

- Uploads photos, videos, and documents to a specified Telegram channel
- Preserves captions when uploading media
- Easy to deploy on Railway
- Simple and user-friendly interface

## Prerequisites

- Python 3.7 or higher
- A Telegram Bot Token (get it from [@BotFather](https://t.me/botfather))
- Channel ID where media will be uploaded
- The bot must be an admin in the target channel

## Environment Variables

Create a `.env` file with the following variables:
```
BOT_TOKEN=your_bot_token_here
CHANNEL_ID=@your_channel_username_or_id
```

## Local Development

1. Clone this repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Create `.env` file with required environment variables
4. Run the bot:
   ```
   python bot.py
   ```

## Deployment on Railway

1. Create a new project on [Railway](https://railway.app/)
2. Connect your GitHub repository
3. Add the required environment variables in Railway dashboard:
   - `BOT_TOKEN`
   - `CHANNEL_ID`
4. Deploy the project

Railway will automatically install dependencies and start the bot.

## Usage

1. Start a chat with your bot
2. Send any media file (photo, video, or document)
3. The bot will automatically upload it to the specified channel

## Commands

- `/start` - Start the bot
- `/help` - Show help message 