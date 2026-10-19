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
BATCH_SIZE = 100        # Telegram maximum batch size
COOLDOWN_DELAY = 2.5    # Pacing delay between batches to stay under rate limits
# --------------------------------------------------

def clean_chat_identifier(chat_input):
    """Normalize URLs or handles into clean usernames or numeric IDs."""
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
    """Retrieve last checkpoint ID to resume seamlessly if container restarts."""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                content = f.read().strip()
                if content.isdigit():
                    return int(content)
        except Exception as e:
            print(f"Warning reading progress tracker: {e}")
    return 0

def save_last_processed_id(message_id):
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(message_id))

async def forward_batch(client, target, batch):
    """Forward messages natively to preserve original forward headers."""
    message_ids = [msg.id for msg in batch]
    source_entity = batch[0].peer_id

    while True:
        try:
            await client.forward_messages(
                entity=target,
                messages=message_ids,
                from_peer=source_entity
            )
            latest_id = message_ids[-1]
            save_last_processed_id(latest_id)
            print(f"[SUCCESS] Forwarded batch up to Message ID {latest_id} ({len(batch)} items).")
            await asyncio.sleep(COOLDOWN_DELAY)
            break
        except FloodWaitError as e:
            wait_time = e.seconds + 5
            print(f"[FLOOD WAIT] Telegram triggered rate limit. Sleeping for {wait_time}s...")
            await asyncio.sleep(wait_time)

async def main():
    cleaned_source = clean_chat_identifier(SOURCE_CHAT)
    cleaned_target = clean_chat_identifier(TARGET_CHAT)

    print("Connecting to Telegram user account...")
    async with TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH) as client:
        print("Connected.")

        source = await client.get_entity(cleaned_source)
        target = await client.get_entity(cleaned_target)

        last_id = get_last_processed_id()
        print(f"Starting chronological forward from Message ID: {last_id}...")

        batch = []
        total_forwarded = 0

        # reverse=True traverses from oldest to newest (Lecture 1 -> 2300)
        async for message in client.iter_messages(source, min_id=last_id, reverse=True):
            if not message.action and (message.message or message.media):
                batch.append(message)

            if len(batch) >= BATCH_SIZE:
                await forward_batch(client, target, batch)
                total_forwarded += len(batch)
                batch = []

        if batch:
            await forward_batch(client, target, batch)
            total_forwarded += len(batch)

        print(f"Done! Forwarded {total_forwarded} messages.")

if __name__ == "__main__":
    asyncio.run(main())
