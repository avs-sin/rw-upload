import os
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Command handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        'Hello! I am your media uploader bot. Send me any media file and I will upload it to the channel.'
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        'Send me any media file (photo, video, document) and I will upload it to the channel.\n'
        'Available commands:\n'
        '/start - Start the bot\n'
        '/help - Show this help message'
    )

async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        channel_id = os.getenv('CHANNEL_ID')
        
        if update.message.photo:
            # Get the largest photo size
            photo = update.message.photo[-1]
            await context.bot.send_photo(
                chat_id=channel_id,
                photo=photo.file_id,
                caption=update.message.caption
            )
        elif update.message.video:
            await context.bot.send_video(
                chat_id=channel_id,
                video=update.message.video.file_id,
                caption=update.message.caption
            )
        elif update.message.document:
            await context.bot.send_document(
                chat_id=channel_id,
                document=update.message.document.file_id,
                caption=update.message.caption
            )
        
        await update.message.reply_text("Media successfully uploaded to the channel! ✅")
    
    except Exception as e:
        logger.error(f"Error uploading media: {str(e)}")
        await update.message.reply_text("Sorry, there was an error uploading your media. Please try again.")

def main():
    # Get the bot token from environment variable
    token = os.getenv('BOT_TOKEN')
    if not token:
        logger.error("No bot token provided!")
        return

    # Create the Application
    application = Application.builder().token(token).build()

    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(MessageHandler(
        filters.PHOTO | filters.VIDEO | filters.DOCUMENT,
        handle_media
    ))

    # Start the bot
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main() 