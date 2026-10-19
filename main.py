import os
import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError

# ----------------- CONFIGURATION -----------------
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
SESSION_STRING = os.environ.get("SESSION_STRING")

SOURCE_CHAT = os.environ.get("SOURCE_CHAT", "khaomaakasam")
TARGET_CHAT = os.environ.get("TARGET_CHAT", "dkdkslodd")

PROGRESS_FILE = "last_forwarded_id.txt"
COOLDOWN_DELAY = 1.5  # Seconds between sends to avoid rate limits
# --------------------------------------------------

def clean_chat_identifier(chat_input):
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
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                content = f.read().strip()
                if content.isdigit():
                    return int(content)
        except Exception as e:
            print(f"Warning reading tracker: {e}")
    return 0

def save_last_processed_id(message_id):
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(message_id))

async def send_clean_copy(client, target, message):
    """Sends a copy of the message/media without any forward header."""
    while True:
        try:
            if message.media:
                # Reuses Telegram's cloud file ID directly without downloading/uploading
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
            print(f"[FLOOD WAIT] Telegram hit limit. Sleeping for {wait_time}s...")
            await asyncio.sleep(wait_time)
        except Exception as e:
            print(f"[SKIP] Error copying message {message.id}: {e}")
            save_last_processed_id(message.id)
            break

async def main():
    cleaned_source = clean_chat_identifier(SOURCE_CHAT)
    cleaned_target = clean_chat_identifier(TARGET_CHAT)

    print("Connecting to user account...")
    async with TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH) as client:
        print("Connected.")

        source = await client.get_entity(cleaned_source)
        target = await client.get_entity(cleaned_target)

        last_id = get_last_processed_id()
        print(f"Starting chronological forward (clean, no forward tag) from ID {last_id}...")

        total_copied = 0

        # reverse=True ensures chronological order (Lecture 1 -> 2300)
        async for message in client.iter_messages(source, min_id=last_id, reverse=True):
            if not message.action and (message.message or message.media):
                await send_clean_copy(client, target, message)
                total_copied += 1

        print(f"Finished! Total messages copied: {total_copied}")

if __name__ == "__main__":
    asyncio.run(main())
