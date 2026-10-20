import os
import re
import asyncio
from typing import Optional, Tuple
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError,
    UserAlreadyParticipantError,
    InviteHashExpiredError,
    InviteHashInvalidError
)
from telethon.tl.types import (
    MessageMediaDocument,
    MessageMediaPoll,
    ChatInviteAlready,
    ChatInvite,
    Updates,
    UpdatesCombined
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest

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
    Parses any input:
      - Private invite link: https://t.me/+76FBRC9V4aA2N2J1 or t.me/joinchat/...
      - Internal private link: https://t.me/c/123456789/41
      - Public username: https://t.me/jajshdhg/15 or @jajshdhg/15
      - Raw IDs: -100xxxxxx
    """
    if not chat_input:
        return "", None

    raw = str(chat_input).strip()
    topic_id = None

    # 1. Private invite link: (+ or joinchat/)
    invite_match = re.search(r"(?:\+|joinchat/)([a-zA-Z0-9_-]+)", raw)
    if invite_match:
        # Check if a topic was appended like +hash/15
        after_hash = raw.split(invite_match.group(1))[-1]
        topic_match = re.search(r"/(\d+)", after_hash)
        if topic_match:
            topic_id = int(topic_match.group(1))
        return f"+{invite_match.group(1)}", topic_id

    # Strip prefixes
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if raw.startswith(prefix):
            raw = raw[len(prefix):]
            break

    raw = raw.lstrip("@").rstrip("/")

    # 2. Private direct message/topic link: c/<channel_id>/<msg_or_topic_id>
    if raw.startswith("c/"):
        parts = raw.split("/")
        if len(parts) >= 2:
            clean_digits = parts[1]
            if clean_digits.isdigit():
                raw = int(f"-100{clean_digits}")
            if len(parts) >= 3 and parts[2].isdigit():
                topic_id = int(parts[2])
        return raw, topic_id

    # 3. Public handle with topic: username/15
    if "/" in raw:
        parts = raw.split("/")
        raw = parts[0]
        if len(parts) >= 2 and parts[1].isdigit():
            topic_id = int(parts[1])

    # 4. Pure numeric ID
    if isinstance(raw, str) and raw.lstrip("-").isdigit():
        raw = int(raw)

    return raw, topic_id

async def resolve_entity_safely(identifier: object):
    """Accurately resolves public handles, private invite hashes, and numeric IDs."""
    
    # CASE 1: Private Invite Link (+hash)
    if isinstance(identifier, str) and identifier.startswith("+"):
        invite_hash = identifier[1:]
        log(f"Resolving private invite hash: {invite_hash}...")
        
        # Step A: Check the invite first (safest way, won't error if already joined)
        try:
            check_res = await client(CheckChatInviteRequest(invite_hash))
            
            # If already a participant, Telegram returns ChatInviteAlready containing the chat
            if isinstance(check_res, ChatInviteAlready):
                log(f"Already a member of private chat: '{check_res.chat.title}'")
                return check_res.chat

            # If not yet joined, import the invite to join
            if isinstance(check_res, ChatInvite):
                log(f"Joining private chat '{check_res.title}' via invite...")
                imported = await client(ImportChatInviteRequest(invite_hash))
                if hasattr(imported, 'chats') and imported.chats:
                    return imported.chats[0]
        except UserAlreadyParticipantError:
            log("Already a participant of this private chat.")
        except (InviteHashExpiredError, InviteHashInvalidError) as e:
            log(f"[Error] The invite link has expired or is invalid: {e}")
            raise e
        except Exception as e:
            log(f"Notice during invite inspection: {e}")

        # Step B: Fallback - Scan active dialogs to find the chat entity
        log("Searching loaded dialogs for private chat entity...")
        async for dialog in client.iter_dialogs(limit=250):
            # Dialogs populate Telethon's internal entity cache
            pass

        # Try importing directly if not already caught
        try:
            imported = await client(ImportChatInviteRequest(invite_hash))
            if hasattr(imported, 'chats') and imported.chats:
                return imported.chats[0]
        except UserAlreadyParticipantError:
            # Re-check dialogs after cache sync
            async for dialog in client.iter_dialogs(limit=100):
                if dialog.is_channel or dialog.is_group:
                    return dialog.entity
        except Exception as e:
            log(f"Direct import note: {e}")

        raise ValueError(f"Could not resolve private invite link: +{invite_hash}. Please make sure your account is in the chat or the link is active.")

    # CASE 2: Numeric IDs (-100xxxx or int)
    if isinstance(identifier, int):
        try:
            return await client.get_entity(identifier)
        except Exception:
            # Sync dialog cache first
            log(f"Syncing dialogs to find ID: {identifier}...")
            async for dialog in client.iter_dialogs(limit=250):
                if dialog.id == identifier:
                    return dialog.entity
            return await client.get_entity(identifier)

    # CASE 3: Public Username / Handle
    try:
        return await client.get_entity(identifier)
    except Exception:
        log(f"Attempting to join public channel: {identifier}...")
        try:
            await client(JoinChannelRequest(identifier))
            return await client.get_entity(identifier)
        except Exception as e:
            log(f"Could not resolve entity '{identifier}': {e}")
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

async def run_forward_task(source_raw: str, target_raw: str, clean_copy: bool, min_msg_id: Optional[int], max_msg_id: Optional[int]):
    global state, client
    state["is_running"] = True
    state["current_status"] = "Running"
    state["total_processed"] = 0

    try:
        source_id, source_topic = parse_telegram_input(source_raw)
        target_id, target_topic = parse_telegram_input(target_raw)

        log(f"Source Identifier: {source_id} (Topic: {source_topic or 'None'})")
        log(f"Target Identifier: {target_id} (Topic: {target_topic or 'None'})")

        source = await resolve_entity_safely(source_id)
        target = await resolve_entity_safely(target_id)

        actual_min_id = (min_msg_id - 1) if (min_msg_id and min_msg_id > 0) else None
        actual_max_id = (max_msg_id + 1) if max_msg_id else None

        range_desc = []
        if min_msg_id:
            range_desc.append(f"from #{min_msg_id}")
        if max_msg_id:
            range_desc.append(f"to #{max_msg_id}")
        range_str = f" ({', '.join(range_desc)})" if range_desc else " (Full History)"

        log(f"Starting chronological transfer{range_str}...")

        async for message in client.iter_messages(
            source, 
            reverse=True, 
            reply_to=source_topic if source_topic else None,
            min_id=actual_min_id or 0,
            max_id=actual_max_id or 0
        ):
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

    <div class="bg-slate-900 border border-slate-800 p-6 rounded-2xl shadow-2xl space-y-5">
      <div class="flex items-center justify-between pb-3 border-b border-slate-800">
        <div>
          <h1 class="text-xl font-bold text-white tracking-tight">Telegram Forwarder</h1>
          <p class="text-xs text-slate-400">Supports Public, Private (+links), and Private Topics</p>
        </div>
        <span id="statusBadge" class="px-2.5 py-1 text-xs font-semibold rounded-full bg-slate-800 text-slate-300">
          Idle
        </span>
      </div>

      <div class="space-y-4">
        <div>
          <label class="block text-xs font-semibold uppercase tracking-wider text-sky-400 mb-1.5">From (Source)</label>
          <input type="text" id="source" placeholder="e.g. https://t.me/+76FBRC9V4aA2N2J1 or https://t.me/c/123456789/41" 
            class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3.5 py-2.5 text-sm focus:outline-none focus:border-sky-500 font-mono">
          
          <div class="grid grid-cols-2 gap-3 mt-2.5">
            <div>
              <label class="block text-[11px] font-medium text-slate-400 mb-1">Start Message ID (Optional)</label>
              <input type="number" id="min_msg_id" placeholder="e.g. 1" 
                class="w-full bg-slate-950/80 border border-slate-800 rounded-lg px-3 py-1.5 text-xs text-slate-200 focus:outline-none focus:border-sky-500 font-mono">
            </div>
            <div>
              <label class="block text-[11px] font-medium text-slate-400 mb-1">End Message ID (Optional)</label>
              <input type="number" id="max_msg_id" placeholder="e.g. 2300" 
                class="w-full bg-slate-950/80 border border-slate-800 rounded-lg px-3 py-1.5 text-xs text-slate-200 focus:outline-none focus:border-sky-500 font-mono">
            </div>
          </div>
          <p class="text-[11px] text-slate-500 mt-1">Accepts private links (<code>t.me/+...</code>), internal links (<code>t.me/c/...</code>), or public handles.</p>
        </div>

        <div>
          <label class="block text-xs font-semibold uppercase tracking-wider text-emerald-400 mb-1.5">To (Destination)</label>
          <input type="text" id="target" placeholder="e.g. https://t.me/jajshdhg/15 or -100xxxxxxxxxx" 
            class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3.5 py-2.5 text-sm focus:outline-none focus:border-emerald-500 font-mono">
          <p class="text-[11px] text-slate-500 mt-1">Make sure you have posting permissions in destination.</p>
        </div>

        <div class="flex items-center pt-1">
          <label class="relative flex items-center cursor-pointer select-none">
            <input type="checkbox" id="clean_copy" checked class="sr-only peer">
            <div class="w-9 h-5 bg-slate-700 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-slate-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-sky-500"></div>
            <span class="ml-3 text-xs text-slate-300">Clean copy (Hide "Forwarded from" tag)</span>
          </label>
        </div>
      </div>

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
      const min_msg_id = document.getElementById('min_msg_id').value;
      const max_msg_id = document.getElementById('max_msg_id').value;

      if (!source || !target) {
        alert("Please enter both source and destination links!");
        return;
      }

      try {
        const res = await fetch('/api/start', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ 
            source, 
            target, 
            clean_copy,
            min_msg_id: min_msg_id ? parseInt(min_msg_id) : null,
            max_msg_id: max_msg_id ? parseInt(max_msg_id) : null
          })
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
    min_msg_id = data.get("min_msg_id")
    max_msg_id = data.get("max_msg_id")

    if not source or not target:
        return JSONResponse({"error": "Source and Target links are required."}, status_code=400)

    background_tasks.add_task(run_forward_task, source, target, clean_copy, min_msg_id, max_msg_id)
    return JSONResponse({"status": "Started successfully."})

@app.post("/api/stop")
async def stop_job():
    if state["is_running"]:
        state["is_running"] = False
        log("Stop command sent...")
        return JSONResponse({"status": "Stopping..."})
    return JSONResponse({"status": "No active task."})
