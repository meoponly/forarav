import os
import asyncio
from typing import Optional
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.tl.types import MessageMediaDocument

API_ID = int(os.environ.get("API_ID", "36500123"))
API_HASH = os.environ.get("API_HASH", "c3838a1772b0c67eadc6a46a8cfcf98a")
raw_session = os.environ.get("SESSION_STRING", "")
SESSION_STRING = raw_session.strip().strip('"').strip("'")

app = FastAPI(title="Telegram Lecture Forwarder")
templates = Jinja2Templates(directory="templates")

# Shared client instance (initialized on startup)
client: Optional[TelegramClient] = None

# Global state tracker for the web dashboard
state = {
    "is_running": False,
    "current_status": "Idle",
    "total_processed": 0,
    "last_message_id": 0,
    "logs": []
}

def log(msg: str):
    state["logs"].append(msg)
    if len(state["logs"]) > 100:
        state["logs"].pop(0)
    print(msg)

def clean_chat_identifier(chat_input: str):
    """Normalize t.me URLs, handles, or numeric IDs."""
    if not chat_input:
        return ""
    chat_input = chat_input.strip()
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if chat_input.startswith(prefix):
            chat_input = chat_input[len(prefix):]
    chat_input = chat_input.lstrip("@").rstrip("/")
    if chat_input.lstrip("-").isdigit():
        return int(chat_input)
    return chat_input

def get_media_attributes(message):
    if not message.media or not isinstance(message.media, MessageMediaDocument):
        return None
    document = message.media.document
    if not document or not hasattr(document, "attributes"):
        return None
    return list(document.attributes)

@app.on_event("startup")
async def startup_event():
    global client
    if SESSION_STRING:
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        await client.connect()
        if await client.is_user_authorized():
            log("[Auth] Successfully connected Telethon user session.")
        else:
            log("[Auth Error] Session string unauthorized or expired.")

@app.on_event("shutdown")
async def shutdown_event():
    global client
    if client and client.is_connected():
        await client.disconnect()

async def run_forward_task(source_raw: str, target_raw: str, source_topic: Optional[int], target_topic: Optional[int], clean_copy: bool, filter_keyword: Optional[str]):
    global state, client
    state["is_running"] = True
    state["current_status"] = "Running"
    state["total_processed"] = 0

    try:
        source_id = clean_chat_identifier(source_raw)
        target_id = clean_chat_identifier(target_raw)

        log(f"Resolving source: {source_id} & target: {target_id}")
        source = await client.get_entity(source_id)
        target = await client.get_entity(target_id)

        log("Starting chronological transfer...")
        
        # reverse=True fetches from oldest to newest
        async for message in client.iter_messages(source, reverse=True, reply_to=source_topic if source_topic else None):
            if not state["is_running"]:
                log("Job manually stopped.")
                break

            # Skip empty service actions
            if message.action or (not message.message and not message.media):
                continue

            # Optional keyword filter
            if filter_keyword:
                text_content = (message.text or "") + (message.file.name or "" if message.file else "")
                if filter_keyword.lower() not in text_content.lower():
                    continue

            # Send/forward message
            while True:
                try:
                    if clean_copy:
                        # Direct copy without "Forwarded from" tag
                        if message.media:
                            attrs = get_media_attributes(message)
                            await client.send_file(
                                entity=target,
                                file=message.media,
                                caption=message.text or "",
                                formatting_entities=message.entities,
                                attributes=attrs,
                                reply_to=target_topic if target_topic else None,
                                supports_streaming=True
                            )
                        elif message.text:
                            await client.send_message(
                                entity=target,
                                message=message.text,
                                formatting_entities=message.entities,
                                reply_to=target_topic if target_topic else None,
                                link_preview=bool(message.web_preview)
                            )
                    else:
                        # Forward preserving "Forwarded from" tag
                        await client.forward_messages(
                            entity=target,
                            messages=[message.id],
                            from_peer=source,
                            reply_to=target_topic if target_topic else None
                        )

                    state["total_processed"] += 1
                    state["last_message_id"] = message.id
                    log(f"Transferred Message ID #{message.id} (Total: {state['total_processed']})")
                    await asyncio.sleep(1.5)
                    break

                except FloodWaitError as e:
                    wait_sec = e.seconds + 5
                    log(f"[FloodWait] Telegram paused task for {wait_sec} seconds...")
                    await asyncio.sleep(wait_sec)
                except Exception as e:
                    log(f"[Error] Failed Message #{message.id}: {str(e)}")
                    break

        log(f"Task finished! Successfully transferred {state['total_processed']} items.")
    except Exception as e:
        log(f"[Fatal Task Error]: {str(e)}")
    finally:
        state["is_running"] = False
        state["current_status"] = "Finished / Idle"

@app.get("/", response_class=HTMLResponse)
async def get_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/status")
async def get_status():
    return JSONResponse(state)

@app.post("/api/start")
async def start_job(request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    if state["is_running"]:
        return JSONResponse({"error": "A job is already running!"}, status_code=400)

    source = data.get("source")
    target = data.get("target")
    source_topic = int(data["source_topic"]) if data.get("source_topic") else None
    target_topic = int(data["target_topic"]) if data.get("target_topic") else None
    clean_copy = bool(data.get("clean_copy", True))
    filter_keyword = data.get("filter_keyword", "").strip() or None

    if not source or not target:
        return JSONResponse({"error": "Source and Target are required."}, status_code=400)

    background_tasks.add_task(
        run_forward_task,
        source,
        target,
        source_topic,
        target_topic,
        clean_copy,
        filter_keyword
    )
    return JSONResponse({"status": "Started successfully."})

@app.post("/api/stop")
async def stop_job():
    if state["is_running"]:
        state["is_running"] = False
        log("Stop command sent...")
        return JSONResponse({"status": "Stopping task..."})
    return JSONResponse({"status": "No active task."})
