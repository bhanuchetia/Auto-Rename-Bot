import os
import re
import time
import shutil
import asyncio
import logging
from datetime import datetime
from PIL import Image
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import (InputMediaDocument, Message, 
                           InlineKeyboardMarkup, InlineKeyboardButton)
from hachoir.metadata import extractMetadata
from hachoir.parser import createParser
from plugins.antinsfw import check_anti_nsfw
from helper.utils import progress_for_pyrogram, humanbytes, convert
from helper.database import codeflixbots
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global dictionary to track ongoing operations
renaming_operations = {}

# Enhanced regex patterns for season and episode extraction
SEASON_EPISODE_PATTERNS = [
    (re.compile(r'S(\d+)(?:E|EP)(\d+)'),  # S01E02, S01EP02
    (re.compile(r'S(\d+)[\s-]*(?:E|EP)(\d+)'),  # S01 E02, S01-EP02
    (re.compile(r'Season\s*(\d+)\s*Episode\s*(\d+)', re.IGNORECASE),  # Season 1 Episode 2
    (re.compile(r'\[S(\d+)\]\[E(\d+)\]'),  # [S01][E02]
    (re.compile(r'S(\d+)[^\d]*(\d+)'),  # S01 13
    (re.compile(r'(?:E|EP|Episode)\s*(\d+)', re.IGNORECASE), (None, 'episode')),
    (re.compile(r'\b(\d+)\b'), (None, 'episode'))  # Standalone number
]

# Quality detection patterns
QUALITY_PATTERNS = [
    (re.compile(r'\b(\d{3,4}[pi])\b', re.IGNORECASE), lambda m: m.group(1)),  # 1080p, 720p
    (re.compile(r'\b(4k|2160p)\b', re.IGNORECASE), lambda m: "4k"),
    (re.compile(r'\b(2k|1440p)\b', re.IGNORECASE), lambda m: "2k"),
    (re.compile(r'\b(HDRip|HDTV)\b', re.IGNORECASE), lambda m: m.group(1)),
    (re.compile(r'\b(4kX264|4kx265)\b', re.IGNORECASE), lambda m: m.group(1)),
    (re.compile(r'\[(\d{3,4}[pi])\]', re.IGNORECASE), lambda m: m.group(1))  # [1080p]
]

# Audio language patterns
AUDIO_PATTERNS = [
    (re.compile(r'\b(Multi|Dual)[-\s]?Audio\b', re.IGNORECASE), lambda m: "Multi"),
    (re.compile(r'\b(Dual)[-\s]?(Audio|Track)\b', re.IGNORECASE), lambda m: "Dual"),
    (re.compile(r'\b(Sub(bed)?)\b', re.IGNORECASE), lambda m: "Sub"),
    (re.compile(r'\b(Dub(bed)?)\b', re.IGNORECASE), lambda m: "Dub"),
    (re.compile(r'\[(Sub|Dub)\]'), lambda m: f"{m.group(1)}bed"),
    (re.compile(r'\((Sub|Dub)\)'), lambda m: f"{m.group(1)}bed"),
    (re.compile(r'\b(Eng(lish)?\s*/\s*(Jap|Kor|Chi))\b', re.IGNORECASE), lambda m: "Dual"),
    (re.compile(r'\b(TrueHD|DTS[- ]?HD|Atmos)\b'), lambda m: m.group(1)),
    (re.compile(r'\[(Unknown)\]'), lambda m: m.group(1))  # Added for [Unknown] tags
]

def extract_season_episode(text):
    """Extract season and episode numbers from text"""
    for pattern in SEASON_EPISODE_PATTERNS:
        match = pattern.search(text)
        if match:
            groups = match.groups()
            season = groups[0] if len(groups) > 0 else None
            episode = groups[1] if len(groups) > 1 else (groups[0] if len(groups) > 0 else None)
            logger.info(f"Extracted season: {season}, episode: {episode} from {text}")
            return season, episode
    logger.warning(f"No season/episode pattern matched for {text}")
    return None, None

def extract_quality(text):
    """Extract quality information from text"""
    for pattern, extractor in QUALITY_PATTERNS:
        match = pattern.search(text)
        if match:
            quality = extractor(match)
            logger.info(f"Extracted quality: {quality} from {text}")
            return quality
    logger.warning(f"No quality pattern matched for {text}")
    return "Unknown"

def extract_audio_info(text):
    """Extract audio/language information from text"""
    for pattern, extractor in AUDIO_PATTERNS:
        match = pattern.search(text)
        if match:
            audio_info = extractor(match)
            logger.info(f"Extracted audio info: {audio_info} from {text}")
            return audio_info
    logger.info(f"No audio pattern matched for {text}")
    return None

async def cleanup_files(*paths):
    """Safely remove files if they exist"""
    for path in paths:
        try:
            if path and os.path.exists(path):
                if os.path.isfile(path):
                    os.remove(path)
                elif os.path.isdir(path):
                    shutil.rmtree(path)
        except Exception as e:
            logger.error(f"Error removing {path}: {e}")

async def process_thumbnail(thumb_path):
    """Process and resize thumbnail image"""
    if not thumb_path or not os.path.exists(thumb_path):
        return None
    
    try:
        with Image.open(thumb_path) as img:
            img = img.convert("RGB").resize((320, 320))
            processed_path = f"{thumb_path}_processed.jpg"
            img.save(processed_path, "JPEG")
        return processed_path
    except Exception as e:
        logger.error(f"Thumbnail processing failed: {e}")
        await cleanup_files(thumb_path)
        return None

async def add_metadata(input_path, output_path, user_id):
    """Add metadata to media file using ffmpeg"""
    ffmpeg = shutil.which('ffmpeg')
    if not ffmpeg:
        raise RuntimeError("FFmpeg not found in PATH")
    
    metadata = {
        'title': await codeflixbots.get_title(user_id) or "",
        'artist': await codeflixbots.get_artist(user_id) or "",
        'author': await codeflixbots.get_author(user_id) or "",
        'video_title': await codeflixbots.get_video(user_id) or "",
        'audio_title': await codeflixbots.get_audio(user_id) or "",
        'subtitle': await codeflixbots.get_subtitle(user_id) or ""
    }
    
    cmd = [
        ffmpeg,
        '-i', input_path,
        '-metadata', f'title={metadata["title"]}',
        '-metadata', f'artist={metadata["artist"]}',
        '-metadata', f'author={metadata["author"]}',
        '-metadata:s:v', f'title={metadata["video_title"]}',
        '-metadata:s:a', f'title={metadata["audio_title"]}',
        '-metadata:s:s', f'title={metadata["subtitle"]}',
        '-map', '0',
        '-c', 'copy',
        '-loglevel', 'error',
        output_path
    ]
    
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await asyncio.wait_for(process.communicate(), timeout=300)
        
        if process.returncode != 0:
            raise RuntimeError(f"FFmpeg error: {stderr.decode()}")
    except asyncio.TimeoutError:
        process.kill()
        raise RuntimeError("FFmpeg processing timed out")

@Client.on_message(filters.command(["renamesource"]) & filters.private)
async def set_file_source(client, message):
    """Set whether to extract patterns from filename or caption"""
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Filename", callback_data="source_filename")],
        [InlineKeyboardButton("Caption", callback_data="source_caption")]
    ])
    await message.reply_text(
        "Select where to extract metadata patterns from:",
        reply_markup=keyboard
    )

@Client.on_callback_query(filters.regex(r"^source_(filename|caption)$"))
async def file_source_callback(client, callback_query):
    """Handle file source selection"""
    source_type = callback_query.data.split("_")[1]
    user_id = callback_query.from_user.id
    
    try:
        await codeflixbots.update_file_source(user_id, source_type)
        await callback_query.answer(f"Patterns will now be extracted from {source_type}")
        await callback_query.message.edit_text(f"✅ source set to: {source_type.upper()}")
    except Exception as e:
        logger.error(f"Error updating file source: {e}")
        await callback_query.answer("Failed to update source preference")

@Client.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def auto_rename_files(client, message):
    """Main handler for auto-renaming files"""
    user_id = message.from_user.id
    try:
        format_template = await codeflixbots.get_format_template(user_id)
        source_type = await codeflixbots.get_file_source(user_id) or "filename"
    except Exception as e:
        logger.error(f"Database error: {e}")
        return await message.reply_text("Error accessing database. Please try again later.")
    
    if not format_template:
        return await message.reply_text("Please set a rename format using /autorename")

    # Get file information
    try:
        if message.document:
            file_id = message.document.file_id
            file_name = message.document.file_name
            file_size = message.document.file_size
            media_type = "document"
        elif message.video:
            file_id = message.video.file_id
            file_name = message.video.file_name or "video"
            file_size = message.video.file_size
            media_type = "video"
        elif message.audio:
            file_id = message.audio.file_id
            file_name = message.audio.file_name or "audio"
            file_size = message.audio.file_size
            media_type = "audio"
        else:
            return await message.reply_text("Unsupported file type")
    except Exception as e:
        logger.error(f"Error getting file info: {e}")
        return await message.reply_text("Error processing file information")

    # NSFW check
    try:
        if await check_anti_nsfw(file_name, message):
            return await message.reply_text("NSFW content detected")
    except Exception as e:
        logger.error(f"NSFW check failed: {e}")
        return await message.reply_text("Error during content check")

    # Prevent duplicate processing
    current_time = datetime.now()
    if file_id in renaming_operations:
        if (current_time - renaming_operations[file_id]).seconds < 10:
            return
    renaming_operations[file_id] = current_time

    download_path = None
    metadata_path = None
    thumb_path = None
    msg = None

    try:
        # Determine text to parse based on source type
        text_to_parse = message.caption if source_type == "caption" and message.caption else file_name
        
        # Extract metadata from selected source
        season, episode = extract_season_episode(text_to_parse)
        quality = extract_quality(text_to_parse)
        audio_info = extract_audio_info(text_to_parse)
        
        # Replace placeholders in template
        replacements = {
            '{season}': season or 'XX',
            '{episode}': episode or 'XX',
            '{quality}': quality,
            '{audio}': audio_info or 'Unknown',
            'Season': season or 'XX',
            'Episode': episode or 'XX',
            'QUALITY': quality,
            'AUDIO': audio_info or 'Unknown'
        }
        
        # Handle all case variations of placeholders
        for placeholder, value in replacements.items():
            format_template = re.sub(
                re.escape(placeholder),
                value,
                format_template,
                flags=re.IGNORECASE
            )

        # Prepare file paths
        ext = os.path.splitext(file_name)[1] or ('.mp4' if media_type == 'video' else '.mp3')
        new_filename = f"{format_template}{ext}"
        download_path = os.path.join("downloads", new_filename)
        metadata_path = os.path.join("metadata", new_filename)
        
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        os.makedirs(os.path.dirname(metadata_path), exist_ok=True)

        # Download file
        msg = await message.reply_text("**Downloading...**")
        try:
            file_path = await client.download_media(
                message,
                file_name=download_path,
                progress=progress_for_pyrogram,
                progress_args=("Downloading...", msg, time.time())
            )
        except FloodWait as e:
            await asyncio.sleep(e.value)
            file_path = await client.download_media(
                message,
                file_name=download_path,
                progress=progress_for_pyrogram,
                progress_args=("Downloading...", msg, time.time())
            )
        except Exception as e:
            await msg.edit(f"Download failed: {e}")
            raise

        # Process metadata
        await msg.edit("**Processing metadata...**")
        try:
            await add_metadata(file_path, metadata_path, user_id)
            file_path = metadata_path
        except Exception as e:
            await msg.edit(f"Metadata processing failed: {e}")
            raise

        # Prepare for upload
        await msg.edit("**Preparing upload...**")
        try:
            caption = await codeflixbots.get_caption(message.chat.id) or f"**{new_filename}**"
            thumb = await codeflixbots.get_thumbnail(message.chat.id)
            thumb_path = None

            # Handle thumbnail
            if thumb:
                thumb_path = await client.download_media(thumb)
            elif media_type == "video" and message.video.thumbs:
                thumb_path = await client.download_media(message.video.thumbs[0].file_id)
            
            thumb_path = await process_thumbnail(thumb_path)

            # Upload file
            await msg.edit("**Uploading...**")
            upload_params = {
                'chat_id': message.chat.id,
                'caption': caption,
                'thumb': thumb_path,
                'progress': progress_for_pyrogram,
                'progress_args': ("Uploading...", msg, time.time())
            }

            try:
                if media_type == "document":
                    await client.send_document(document=file_path, **upload_params)
                elif media_type == "video":
                    await client.send_video(video=file_path, **upload_params)
                elif media_type == "audio":
                    await client.send_audio(audio=file_path, **upload_params)
            except FloodWait as e:
                await asyncio.sleep(e.value)
                if media_type == "document":
                    await client.send_document(document=file_path, **upload_params)
                elif media_type == "video":
                    await client.send_video(video=file_path, **upload_params)
                elif media_type == "audio":
                    await client.send_audio(audio=file_path, **upload_params)

            if msg:
                await msg.delete()
        except Exception as e:
            if msg:
                await msg.edit(f"Upload failed: {e}")
            raise

    except Exception as e:
        logger.error(f"Processing error: {e}", exc_info=True)
        if msg:
            await msg.edit(f"Error: {str(e)}")
        else:
            await message.reply_text(f"Error: {str(e)}")
    finally:
        # Clean up files
        await cleanup_files(download_path, metadata_path, thumb_path)
        if file_id in renaming_operations:
            renaming_operations.pop(file_id)
