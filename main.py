import os
import re
import asyncio
from typing import Optional, Tuple
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

app = FastAPI(title="Telegram Smart Content Transfer")

client: Optional[TelegramClient] = None

state = {
    "is_running": False,
    "current_status": "Idle",
    "total_processed": 0,
    "logs": []
}

def log(msg: str):
    state["logs"].append(msg)
    if len(state["logs"]) > 100:
        state["logs"].pop(0)
    print(msg)

def parse_telegram_input(chat_input: str) -> Tuple[object, Optional[int]]:
    """
    Parses any input string into (chat_identifier, topic_id).
    Supports:
      - @username/41 or https://t.me/username/41
      - https://t.me/c/123456789/41
      - https://t.me/+ABCDEF (Private invite)
      - -100xxxxxxx or raw @username
    """
    if not chat_input:
        return "", None

    raw = str(chat_input).strip()
    topic_id = None

    # Check for private invite links (+ or joinchat/)
    invite_match = re.search(r"(?:\+|joinchat/)([a-zA-Z0-9_-]+)", raw)
    if invite_match:
        return f"+{invite_match.group(1)}", None

    # Remove standard t.me protocols
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if raw.startswith(prefix):
            raw = raw[len(prefix):]
            break

    raw = raw.lstrip("@").rstrip("/")

    # Check for topic link: c/123456789/41 or username/41
    if "/" in raw:
        parts = raw.split("/")
        if len(parts) >= 2:
            if parts[0] == "c":
                # Private supergroup link format: c/<channel_id>/<topic_id>
                chat_id_part = parts[1]
                topic_part = parts[2] if len(parts) > 2 else parts[1]
                if chat_id_part.isdigit():
                    raw = int(f"-100{chat_id_part}")
                if topic_part.isdigit():
                    topic_id = int(topic_part)
            else:
                # Public topic link: <username>/<topic_id>
                raw = parts[0]
                if parts[1].isdigit():
                    topic_id = int(parts[1])

    # Convert numeric strings to int
    if isinstance(raw, str) and raw.lstrip("-").isdigit():
        raw = int(raw)

    return raw, topic_id

async def ensure_joined(identifier: object):
    """Auto-joins channel/group if not already a member."""
    # Handle private invites (+hash)
    if isinstance(identifier, str) and identifier.startswith("+"):
        invite_hash = identifier[1:]
        try:
            log(f"Joining private chat via invite hash: {invite_hash}...")
            updates = await client(ImportChatInviteRequest(invite_hash))
            return updates.chats[0]
        except UserAlreadyParticipantError:
            pass
        except Exception as e:
            log(f"Notice on joining private chat: {e}")

    try:
        return await client.get_entity(identifier)
    except Exception:
        try:
            log(f"Joining public chat: {identifier}...")
            await client(JoinChannelRequest(identifier))
            return await client.get_entity(identifier)
        except Exception as e:
            log(f"Auto-join failed for {identifier}: {e}")
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
                log("[Auth] Telethon user session connected.")
            else:
                log("[Auth Error] Session invalid or revoked.")
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
            log(f"Transferred Message #{message.id} (Total: {state['total_processed']})")
            await asyncio.sleep(1.5)
            break

        except FloodWaitError as e:
            wait_sec = e.seconds + 5
            log(f"[FloodWait] Telegram paused task for {wait_sec}s...")
            await asyncio.sleep(wait_sec)
        except Exception as e:
            log(f"[Skip] Error copying message #{message.id}: {str(e)}")
            break

async def run_forward_task(source_raw: str, target_raw: str, clean_copy: bool):
    global state, client
    state["is_running"] = True
    state["current_status"] = "Running"
    state["total_processed"] = 0

    try:
        source_id, source_topic = parse_telegram_input(source_raw)
        target_id, target_topic = parse_telegram_input(target_raw)

        log(f"Parsed Source: {source_id} (Topic: {source_topic or 'None'})")
        log(f"Parsed Target: {target_id} (Topic: {target_topic or 'None'})")

        source = await ensure_joined(source_id)
        target = await ensure_joined(target_id)

        log("Starting transfer in chronological order...")

        # reverse=True ensures oldest to newest
        async for message in client.iter_messages(source, reverse=True, reply_to=source_topic if source_topic else None):
            if not state["is_running"]:
                log("Stopped by user.")
                break

            if message.action or (not message.message and not message.media):
                continue

            await forward_item(source, target, message, clean_copy, target_topic)

        log(f"Done! Successfully transferred {state['total_processed']} items.")
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
  <title>Telegram Forwarder</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen flex flex-col items-center justify-center p-4">
  <div class="w-full max-w-xl space-y-4">

    <!-- Card -->
    <div class="bg-slate-900 border border-slate-800 p-6 rounded-2xl shadow-2xl space-y-5">
      <div class="flex items-center justify-between pb-3 border-b border-slate-800">
        <div>
          <h1 class="text-xl font-bold text-white tracking-tight">Telegram Forwarder</h1>
          <p class="text-xs text-slate-400">Paste any link, username, or topic URL</p>
        </div>
        <span id="statusBadge" class="px-2.5 py-1 text-xs font-semibold rounded-full bg-slate-800 text-slate-300">
          Idle
        </span>
      </div>

      <div class="space-y-4">
        <!-- From Input -->
        <div>
          <label class="block text-xs font-semibold uppercase tracking-wider text-sky-400 mb-1.5">From (Source)</label>
          <input type="text" id="source" placeholder="e.g. https://t.me/mygroup/41 or @channel" 
            class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3.5 py-2.5 text-sm focus:outline-none focus:border-sky-500 font-mono">
          <p class="text-[11px] text-slate-500 mt-1">Accepts channel link, invite link, or topic link (e.g. <code>@group/41</code>).</p>
        </div>

        <!-- To Input -->
        <div>
          <label class="block text-xs font-semibold uppercase tracking-wider text-emerald-400 mb-1.5">To (Destination)</label>
          <input type="text" id="target" placeholder="e.g. https://t.me/targetchannel or @targetgroup/12" 
            class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3.5 py-2.5 text-sm focus:outline-none focus:border-emerald-500 font-mono">
          <p class="text-[11px] text-slate-500 mt-1">Make sure you have posting/admin permissions.</p>
        </div>

        <!-- Clean Copy Toggle -->
        <div class="flex items-center pt-1">
          <label class="relative flex items-center cursor-pointer select-none">
            <input type="checkbox" id="clean_copy" checked class="sr-only peer">
            <div class="w-9 h-5 bg-slate-700 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-slate-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-sky-500"></div>
            <span class="ml-3 text-xs text-slate-300">Clean copy (Hide "Forwarded from" tag)</span>
          </label>
        </div>
      </div>

      <!-- Action Buttons -->
      <div class="flex items-center space-x-3 pt-2">
        <button id="startBtn" onclick="startJob()" 
          class="flex-1 py-2.5 rounded-xl text-sm font-bold bg-sky-500 hover:bg-sky-400 text-slate-950 transition disabled:opacity-40">
          Start Forwarding
        </button>
        <button id="stopBtn" onclick="stopJob()" disabled 
          class="px-5 py-2.5 rounded-xl text-sm font-semibold bg-rose-600/20 text-rose-300 border border-rose-500/30 hover:bg-rose-600/30 transition disabled:opacity-40">
          Stop
        </button>
      </div>
    </div>

    <!-- Live Logs -->
    <div class="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-xl space-y-2">
      <div class="flex items-center justify-between text-xs text-slate-400">
        <span class="font-semibold">Live Log</span>
        <span id="counter" class="font-mono text-sky-400">Processed: 0</span>
      </div>
      <div id="console" class="h-44 overflow-y-auto bg-slate-950 rounded-xl p-3 font-mono text-xs text-slate-300 space-y-1 border border-slate-800">
        <div class="text-slate-600">Ready. Paste links and click Start.</div>
      </div>
    </div>

  </div>

  <script>
    async function startJob() {
      const source = document.getElementById('source').value.trim();
      const target = document.getElementById('target').value.trim();
      const clean_copy = document.getElementById('clean_copy').checked;

      if (!source || !target) {
        alert("Please enter both source and destination links!");
        return;
      }

      try {
        const res = await fetch('/api/start', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ source, target, clean_copy })
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
          badge.className = "px-2.5 py-1 text-xs font-semibold rounded-full bg-emerald-500/20 text-emerald-400 border border-emerald-500/30";
          badge.innerText = "Running";
          startBtn.disabled = true;
          stopBtn.disabled = false;
        } else {
          badge.className = "px-2.5 py-1 text-xs font-semibold rounded-full bg-slate-800 text-slate-300";
          badge.innerText = data.current_status;
          startBtn.disabled = false;
          stopBtn.disabled = true;
        }

        document.getElementById('counter').innerText = `Processed: ${data.total_processed}`;

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
    clean_copy = bool(data.get("clean_copy", True))

    if not source or not target:
        return JSONResponse({"error": "Source and Target links are required."}, status_code=400)

    background_tasks.add_task(run_forward_task, source, target, clean_copy)
    return JSONResponse({"status": "Started successfully."})

@app.post("/api/stop")
async def stop_job():
    if state["is_running"]:
        state["is_running"] = False
        log("Stop command sent...")
        return JSONResponse({"status": "Stopping..."})
    return JSONResponse({"status": "No active task."})
