import motor.motor_asyncio
import datetime
import pytz
import logging
from config import Config
from .utils import send_log


class Database:
    def __init__(self, uri, database_name):
        try:
            self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
            self._client.server_info()  # Test connection
            logging.info("Successfully connected to MongoDB")
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            raise
        self.db = self._client[database_name]
        self.users = self.db.users

    def new_user(self, id):
        return {
            "_id": int(id),
            "join_date": datetime.datetime.now(pytz.utc).isoformat(),
            "file_id": None,
            "caption": None,
            "metadata": True,
            "metadata_code": "Telegram : @Codeflix_Bots",
            "format_template": None,
            "file_source": "filename",  # Default to filename extraction
            "ban_status": {
                "is_banned": False,
                "ban_duration": 0,
                "banned_on": datetime.datetime.max.isoformat(),
                "ban_reason": ""
            },
            "title": "Encoded by @Animes_Cruise",
            "author": "@Animes_Cruise",
            "artist": "@Animes_Cruise",
            "audio": "By @Animes_Cruise",
            "subtitle": "By @Animes_Cruise",
            "video": "Encoded By @Animes_Cruise"
        }

    async def add_user(self, bot, message):
        user = message.from_user
        if not await self.is_user_exist(user.id):
            new_user = self.new_user(user.id)
            try:
                await self.users.insert_one(new_user)
                await send_log(bot, user)
            except Exception as e:
                logging.error(f"Error adding user {user.id}: {e}")

    async def is_user_exist(self, id):
        try:
            return bool(await self.users.find_one({"_id": int(id)}))
        except Exception as e:
            logging.error(f"Error checking user {id}: {e}")
            return False

    async def total_users_count(self):
        try:
            return await self.users.count_documents({})
        except Exception as e:
            logging.error(f"Error counting users: {e}")
            return 0

    async def get_all_users(self):
        try:
            return self.users.find({})
        except Exception as e:
            logging.error(f"Error getting users: {e}")
            return None

    async def delete_user(self, user_id):
        try:
            await self.users.delete_one({"_id": int(user_id)})
        except Exception as e:
            logging.error(f"Error deleting user {user_id}: {e}")

    # Thumbnail methods
    async def set_thumbnail(self, id, file_id):
        try:
            await self.users.update_one(
                {"_id": int(id)},
                {"$set": {"file_id": file_id}}
            )
        except Exception as e:
            logging.error(f"Error setting thumbnail: {e}")

    async def get_thumbnail(self, id):
        try:
            user = await self.users.find_one({"_id": int(id)})
            return user.get("file_id")
        except Exception as e:
            logging.error(f"Error getting thumbnail: {e}")
            return None

    # Caption methods
    async def set_caption(self, id, caption):
        try:
            await self.users.update_one(
                {"_id": int(id)},
                {"$set": {"caption": caption}}
            )
        except Exception as e:
            logging.error(f"Error setting caption: {e}")

    async def get_caption(self, id):
        try:
            user = await self.users.find_one({"_id": int(id)})
            return user.get("caption")
        except Exception as e:
            logging.error(f"Error getting caption: {e}")
            return None

    # Format template methods
    async def set_format_template(self, id, template):
        try:
            await self.users.update_one(
                {"_id": int(id)},
                {"$set": {"format_template": template}}
            )
        except Exception as e:
            logging.error(f"Error setting format template: {e}")

    async def get_format_template(self, id):
        try:
            user = await self.users.find_one({"_id": int(id)})
            return user.get("format_template")
        except Exception as e:
            logging.error(f"Error getting format template: {e}")
            return None

    # File source methods (for /file_source command)
    async def set_file_source(self, id, source_type):
        try:
            await self.users.update_one(
                {"_id": int(id)},
                {"$set": {"file_source": source_type}}
            )
        except Exception as e:
            logging.error(f"Error setting file source: {e}")

    async def get_file_source(self, id):
        try:
            user = await self.users.find_one({"_id": int(id)})
            return user.get("file_source", "filename")  # Default to filename
        except Exception as e:
            logging.error(f"Error getting file source: {e}")
            return "filename"

    # Metadata methods
    async def get_metadata(self, id):
        try:
            user = await self.users.find_one({"_id": int(id)})
            return user.get("metadata", True)
        except Exception as e:
            logging.error(f"Error getting metadata status: {e}")
            return True

    async def set_metadata(self, id, status):
        try:
            await self.users.update_one(
                {"_id": int(id)},
                {"$set": {"metadata": status}}
            )
        except Exception as e:
            logging.error(f"Error setting metadata status: {e}")

    # Metadata fields methods
    async def get_title(self, id):
        try:
            user = await self.users.find_one({"_id": int(id)})
            return user.get("title", "Encoded by @Animes_Cruise")
        except Exception as e:
            logging.error(f"Error getting title: {e}")
            return "Encoded by @Animes_Cruise"

    async def set_title(self, id, title):
        try:
            await self.users.update_one(
                {"_id": int(id)},
                {"$set": {"title": title}}
            )
        except Exception as e:
            logging.error(f"Error setting title: {e}")

    async def get_author(self, id):
        try:
            user = await self.users.find_one({"_id": int(id)})
            return user.get("author", "@Animes_Cruise")
        except Exception as e:
            logging.error(f"Error getting author: {e}")
            return "@Animes_Cruise"

    async def set_author(self, id, author):
        try:
            await self.users.update_one(
                {"_id": int(id)},
                {"$set": {"author": author}}
            )
        except Exception as e:
            logging.error(f"Error setting author: {e}")

    async def get_artist(self, id):
        try:
            user = await self.users.find_one({"_id": int(id)})
            return user.get("artist", "@Animes_Cruise")
        except Exception as e:
            logging.error(f"Error getting artist: {e}")
            return "@Animes_Cruise"

    async def set_artist(self, id, artist):
        try:
            await self.users.update_one(
                {"_id": int(id)},
                {"$set": {"artist": artist}}
            )
        except Exception as e:
            logging.error(f"Error setting artist: {e}")

    async def get_audio(self, id):
        try:
            user = await self.users.find_one({"_id": int(id)})
            return user.get("audio", "By @Animes_Cruise")
        except Exception as e:
            logging.error(f"Error getting audio: {e}")
            return "By @Animes_Cruise"

    async def set_audio(self, id, audio):
        try:
            await self.users.update_one(
                {"_id": int(id)},
                {"$set": {"audio": audio}}
            )
        except Exception as e:
            logging.error(f"Error setting audio: {e}")

    async def get_subtitle(self, id):
        try:
            user = await self.users.find_one({"_id": int(id)})
            return user.get("subtitle", "By @Animes_Cruise")
        except Exception as e:
            logging.error(f"Error getting subtitle: {e}")
            return "By @Animes_Cruise"

    async def set_subtitle(self, id, subtitle):
        try:
            await self.users.update_one(
                {"_id": int(id)},
                {"$set": {"subtitle": subtitle}}
            )
        except Exception as e:
            logging.error(f"Error setting subtitle: {e}")

    async def get_video(self, id):
        try:
            user = await self.users.find_one({"_id": int(id)})
            return user.get("video", "Encoded By @Animes_Cruise")
        except Exception as e:
            logging.error(f"Error getting video: {e}")
            return "Encoded By @Animes_Cruise"

    async def set_video(self, id, video):
        try:
            await self.users.update_one(
                {"_id": int(id)},
                {"$set": {"video": video}}
            )
        except Exception as e:
            logging.error(f"Error setting video: {e}")


# Initialize database connection
codeflixbots = Database(Config.DB_URL, Config.DB_NAME)
