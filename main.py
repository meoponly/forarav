import os
import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError

# ----------------- CONFIGURATION -----------------
API_ID = int(os.environ.get("API_ID", "36500123"))
API_HASH = os.environ.get("API_HASH", "c3838a1772b0c67eadc6a46a8cfcf98a")

# Sanitize session string: strip any surrounding quotes, newlines, or whitespace
raw_session = os.environ.get("SESSION_STRING", "")
SESSION_STRING = raw_session.strip().strip('"').strip("'")

SOURCE_CHAT = os.environ.get("SOURCE_CHAT", "khaomaakasam")
TARGET_CHAT = os.environ.get("TARGET_CHAT", "dkdkslodd")

PROGRESS_FILE = "last_forwarded_id.txt"
COOLDOWN_DELAY = 1.5  # Safe delay (in seconds) between sends to prevent flood limits
# --------------------------------------------------

def clean_chat_identifier(chat_input):
    """Normalize t.me URLs, @ handles, or numeric IDs."""
    if isinstance(chat_input, str):
        chat_input = chat_input.strip()
        for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
            if chat_input.startswith(prefix):
                chat_input = chat_input[len(prefix):]
        chat_input = chat_input.lstrip("@").rstrip("/")
        if chat_input.lstrip("-").isdigit():
            return int(chat_input)
    return chat_input

def get_last_processed_id():
    """Retrieve last checkpoint message ID to resume if Railway restarts."""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                content = f.read().strip()
                if content.isdigit():
                    return int(content)
        except Exception as e:
            print(f"Warning reading tracker file: {e}")
    return 0

def save_last_processed_id(message_id):
    """Save progress state to local file."""
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(message_id))

async def send_clean_copy(client, target, message):
    """Sends a copy of the message/media without any forward header."""
    while True:
        try:
            if message.media:
                # Reuses Telegram's cloud file directly without re-uploading
                await client.send_file(
                    entity=target,
                    file=message.media,
                    caption=message.text or "",
                    formatting_entities=message.entities
                )
            elif message.text:
                await client.send_message(
                    entity=target,
                    message=message.text,
                    formatting_entities=message.entities,
                    link_preview=bool(message.web_preview)
                )

            save_last_processed_id(message.id)
            print(f"[COPIED] Message ID: {message.id}")
            await asyncio.sleep(COOLDOWN_DELAY)
            break

        except FloodWaitError as e:
            wait_time = e.seconds + 5
            print(f"[FLOOD WAIT] Rate limit reached. Pausing execution for {wait_time}s...")
            await asyncio.sleep(wait_time)
        except Exception as e:
            print(f"[SKIP] Could not copy message {message.id}: {e}")
            save_last_processed_id(message.id)
            break

async def main():
    cleaned_source = clean_chat_identifier(SOURCE_CHAT)
    cleaned_target = clean_chat_identifier(TARGET_CHAT)

    print("Connecting to user account using StringSession...")
    async with TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH) as client:
        print("Connected successfully.")

        source = await client.get_entity(cleaned_source)
        target = await client.get_entity(cleaned_target)

        last_id = get_last_processed_id()
        print(f"Starting chronological forward (clean copy) from Message ID: {last_id}...")

        total_copied = 0

        # reverse=True traverses from oldest to newest (Lecture 1 -> 2300)
        async for message in client.iter_messages(source, min_id=last_id, reverse=True):
            if not message.action and (message.message or message.media):
                await send_clean_copy(client, target, message)
                total_copied += 1

        print(f"All done! Total messages forwarded cleanly: {total_copied}")

if __name__ == "__main__":
    asyncio.run(main())
