import os
import re
import asyncio
from typing import Optional
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, UserAlreadyParticipantError
from telethon.tl.types import MessageMediaDocument, MessageMediaPoll
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

API_ID = int(os.environ.get("API_ID", "36500123"))
API_HASH = os.environ.get("API_HASH", "c3838a1772b0c67eadc6a46a8cfcf98a")

raw_session = os.environ.get("SESSION_STRING", "")
SESSION_STRING = raw_session.strip().strip('"').strip("'")

app = FastAPI(title="Universal Telegram Forwarder")

client: Optional[TelegramClient] = None

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

async def ensure_joined(chat_input: str):
    """Automatically joins the channel/group if not already a member."""
    if not chat_input:
        return None

    raw = str(chat_input).strip()

    # Case 1: Private invite link format (e.g., https://t.me/+ABCDEF or t.me/joinchat/ABCDEF)
    invite_hash_match = re.search(r"(?:\+|joinchat/)([a-zA-Z0-9_-]+)", raw)
    if invite_hash_match:
        invite_hash = invite_hash_match.group(1)
        try:
            log(f"Attempting to join private chat via invite hash: {invite_hash}")
            updates = await client(ImportChatInviteRequest(invite_hash))
            log("Successfully joined private chat.")
            return updates.chats[0]
        except UserAlreadyParticipantError:
            log("Already a participant of the private chat.")
        except Exception as e:
            log(f"Notice on joinchat: {e}")

    # Case 2: Public handle, username, or clean ID
    cleaned = clean_chat_identifier(raw)
    try:
        entity = await client.get_entity(cleaned)
        return entity
    except Exception:
        # If resolving entity fails because account is not in the channel
        try:
            log(f"Not joined yet. Attempting to join public channel/group: {cleaned}")
            await client(JoinChannelRequest(cleaned))
            log(f"Successfully joined {cleaned}!")
            return await client.get_entity(cleaned)
        except Exception as e:
            log(f"[Join Error] Could not join {cleaned}: {e}")
            raise e

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
        try:
            client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
            await client.connect()
            if await client.is_user_authorized():
                log("[Auth] Successfully connected Telethon session.")
            else:
                log("[Auth Error] Session string unauthorized or expired.")
        except Exception as e:
            log(f"[Auth Error] {str(e)}")

@app.on_event("shutdown")
async def shutdown_event():
    global client
    if client and client.is_connected():
        await client.disconnect()

async def forward_item(source, target, message, clean_copy: bool, target_topic: Optional[int]):
    while True:
        try:
            if clean_copy:
                if isinstance(message.media, MessageMediaPoll):
                    await client.send_message(
                        entity=target,
                        file=message.media.poll,
                        reply_to=target_topic if target_topic else None
                    )
                elif message.media:
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
                await client.forward_messages(
                    entity=target,
                    messages=[message.id],
                    from_peer=source,
                    reply_to=target_topic if target_topic else None
                )

            state["total_processed"] += 1
            state["last_message_id"] = message.id
            log(f"Transferred Message #{message.id} (Total: {state['total_processed']})")
            await asyncio.sleep(1.5)
            break

        except FloodWaitError as e:
            wait_sec = e.seconds + 5
            log(f"[FloodWait] Telegram rate limited. Pausing for {wait_sec}s...")
            await asyncio.sleep(wait_sec)
        except Exception as e:
            log(f"[Skip] Error copying Message #{message.id}: {str(e)}")
            break

async def run_forward_task(source_raw: str, target_raw: str, source_topic: Optional[int], target_topic: Optional[int], clean_copy: bool, filter_keyword: Optional[str]):
    global state, client
    state["is_running"] = True
    state["current_status"] = "Running"
    state["total_processed"] = 0

    try:
        # Step 1: Ensure account is joined to source and target
        log(f"Checking membership & resolving source: {source_raw}")
        source = await ensure_joined(source_raw)

        log(f"Checking membership & resolving target: {target_raw}")
        target = await ensure_joined(target_raw)

        log("Starting transfer of all content (videos, files, photos, texts, polls) chronologically...")

        # reverse=True fetches from oldest to newest
        async for message in client.iter_messages(source, reverse=True, reply_to=source_topic if source_topic else None):
            if not state["is_running"]:
                log("Process stopped by user.")
                break

            if message.action or (not message.message and not message.media):
                continue

            if filter_keyword:
                text_content = (message.text or "")
                if hasattr(message, "file") and message.file and message.file.name:
                    text_content += " " + message.file.name
                if filter_keyword.lower() not in text_content.lower():
                    continue

            await forward_item(source, target, message, clean_copy, target_topic)

        log(f"Finished! Successfully transferred {state['total_processed']} items.")
    except Exception as e:
        log(f"[Fatal Task Error] {str(e)}")
    finally:
        state["is_running"] = False
        state["current_status"] = "Finished / Idle"

HTML_CONTENT = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Universal Telegram Content Forwarder</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen py-8 px-4 font-sans">
  <div class="max-w-4xl mx-auto space-y-6">

    <div class="flex items-center justify-between border-b border-slate-800 pb-5">
      <div>
        <h1 class="text-2xl font-bold tracking-tight text-white">Universal Telegram Forwarder</h1>
        <p class="text-sm text-slate-400">Auto-joins channels/groups and transfers all content cleanly in chronological order.</p>
      </div>
      <div id="statusBadge" class="px-3 py-1 text-xs font-semibold rounded-full bg-slate-800 text-slate-300">
        Status: Idle
      </div>
    </div>

    <div class="grid grid-cols-1 md:grid-cols-2 gap-6 bg-slate-900 border border-slate-800 p-6 rounded-2xl shadow-xl">
      <div class="space-y-4">
        <h2 class="text-sm font-bold text-sky-400 uppercase tracking-wider">Source (Auto Joins If Needed)</h2>
        <div>
          <label class="block text-xs font-medium text-slate-300 mb-1">Source Chat / Link</label>
          <input type="text" id="source" value="khaomaakasam" placeholder="username, link, or private invite (+xyz)" 
            class="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-sky-500">
        </div>
        <div>
          <label class="block text-xs font-medium text-slate-300 mb-1">Source Topic ID (Optional)</label>
          <input type="number" id="source_topic" placeholder="Leave blank if not a topic" 
            class="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-sky-500">
        </div>
      </div>

      <div class="space-y-4">
        <h2 class="text-sm font-bold text-emerald-400 uppercase tracking-wider">Destination</h2>
        <div>
          <label class="block text-xs font-medium text-slate-300 mb-1">Target Chat / Link</label>
          <input type="text" id="target" value="dkdkslodd" placeholder="username or link" 
            class="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-emerald-500">
        </div>
        <div>
          <label class="block text-xs font-medium text-slate-300 mb-1">Target Topic ID (Optional)</label>
          <input type="number" id="target_topic" placeholder="Leave empty for main chat" 
            class="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-emerald-500">
        </div>
      </div>

      <div class="md:col-span-2 pt-2 border-t border-slate-800 grid grid-cols-1 md:grid-cols-2 gap-4">
        <div>
          <label class="block text-xs font-medium text-slate-300 mb-1">Keyword Filter (Optional)</label>
          <input type="text" id="filter_keyword" placeholder="Leave blank to transfer everything" 
            class="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-sky-500">
        </div>
        <div class="flex items-center pt-5">
          <label class="relative flex items-center cursor-pointer select-none">
            <input type="checkbox" id="clean_copy" checked class="sr-only peer">
            <div class="w-10 h-6 bg-slate-700 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-slate-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-sky-500"></div>
            <span class="ml-3 text-sm font-medium text-slate-300">Clean copy (No "Forwarded from" tag)</span>
          </label>
        </div>
      </div>

      <div class="md:col-span-2 flex items-center justify-end space-x-3 pt-2">
        <button id="stopBtn" onclick="stopJob()" disabled 
          class="px-5 py-2.5 rounded-lg text-sm font-semibold bg-rose-600/20 text-rose-300 border border-rose-500/30 hover:bg-rose-600/30 disabled:opacity-40 disabled:cursor-not-allowed">
          Stop Process
        </button>
        <button id="startBtn" onclick="startJob()" 
          class="px-6 py-2.5 rounded-lg text-sm font-semibold bg-sky-500 hover:bg-sky-400 text-slate-950 font-bold transition disabled:opacity-40 disabled:cursor-not-allowed">
          Start Forwarding All Content
        </button>
      </div>
    </div>

    <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-4">
      <div class="flex items-center justify-between">
        <h3 class="text-sm font-semibold text-slate-300">Live Execution Log</h3>
        <span id="counter" class="text-xs font-mono text-sky-400 font-bold">Processed: 0 messages</span>
      </div>
      <div id="console" class="h-64 overflow-y-auto bg-slate-950 rounded-xl p-4 font-mono text-xs text-slate-300 space-y-1 border border-slate-800">
        <div class="text-slate-600">Ready. Paste your channels and click start.</div>
      </div>
    </div>

  </div>

  <script>
    async function startJob() {
      const payload = {
        source: document.getElementById('source').value,
        source_topic: document.getElementById('source_topic').value,
        target: document.getElementById('target').value,
        target_topic: document.getElementById('target_topic').value,
        clean_copy: document.getElementById('clean_copy').checked,
        filter_keyword: document.getElementById('filter_keyword').value
      };

      try {
        const res = await fetch('/api/start', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(payload)
        });
        const data = await res.json();
        if (data.error) alert(data.error);
      } catch (err) {
        alert("Failed to start job: " + err);
      }
    }

    async function stopJob() {
      await fetch('/api/stop', { method: 'POST' });
    }

    async function pollStatus() {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();

        const badge = document.getElementById('statusBadge');
        const startBtn = document.getElementById('startBtn');
        const stopBtn = document.getElementById('stopBtn');

        if (data.is_running) {
          badge.className = "px-3 py-1 text-xs font-semibold rounded-full bg-emerald-500/20 text-emerald-400 border border-emerald-500/30";
          badge.innerText = "Status: Running";
          startBtn.disabled = true;
          stopBtn.disabled = false;
        } else {
          badge.className = "px-3 py-1 text-xs font-semibold rounded-full bg-slate-800 text-slate-300";
          badge.innerText = `Status: ${data.current_status}`;
          startBtn.disabled = false;
          stopBtn.disabled = true;
        }

        document.getElementById('counter').innerText = `Processed: ${data.total_processed} messages`;

        const consoleEl = document.getElementById('console');
        if (data.logs.length > 0) {
          consoleEl.innerHTML = data.logs.map(l => `<div>&gt; ${l}</div>`).join('');
          consoleEl.scrollTop = consoleEl.scrollHeight;
        }
      } catch (e) {}
    }

    setInterval(pollStatus, 2000);
    pollStatus();
  </script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def get_index():
    return HTMLResponse(content=HTML_CONTENT)

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
