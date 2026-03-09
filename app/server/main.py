"""FastAPI server for SAP Finance Intelligence Agent."""

import asyncio
import base64
import json
import logging
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from server.agent import run_agent
from server.db import list_conversations, get_conversation, save_message, delete_conversation

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="SAP Finance Intelligence")

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


def _get_user_email(request: Request) -> str:
    """Extract user email from the x-forwarded-access-token JWT (set by Databricks App proxy)."""
    token = request.headers.get("x-forwarded-access-token", "")
    if not token:
        return "anonymous"
    try:
        # JWT payload is the second segment, base64url-encoded
        payload = token.split(".")[1]
        # Add padding if needed
        payload += "=" * (4 - len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
        return claims.get("sub", claims.get("email", "anonymous"))
    except Exception:
        return "anonymous"


class ChatRequest(BaseModel):
    message: str
    session_id: str
    history: list[dict] = []


def _try_save(session_id, role, content, **kwargs):
    """Best-effort save to Lakebase — don't break chat if DB is unavailable."""
    try:
        save_message(session_id, role, content, **kwargs)
    except Exception as e:
        logger.warning(f"DB save failed (non-fatal): {e}")


@app.post("/api/chat")
async def chat(req: ChatRequest, request: Request):
    try:
        user_email = _get_user_email(request)
        title = req.message[:80] if not req.history else None

        _try_save(req.session_id, "user", req.message, title=title, user_email=user_email)

        result = await asyncio.to_thread(run_agent, req.message, req.history)

        _try_save(req.session_id, "assistant", result["response"], sources=result.get("sources"), user_email=user_email, steps=result.get("steps"))

        return JSONResponse(result)
    except Exception as e:
        logger.error(f"Agent error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/chat/stream")
async def chat_stream(req: ChatRequest, request: Request):
    """SSE endpoint — streams step events in real-time, then the final result."""
    user_email = _get_user_email(request)
    queue: asyncio.Queue = asyncio.Queue()

    def on_step(step: dict):
        """Called from sync thread — push step onto async queue."""
        queue.put_nowait(step)

    async def run_in_thread():
        """Run agent in background thread, return result."""
        return await asyncio.to_thread(run_agent, req.message, req.history, on_step)

    async def event_stream():
        task = asyncio.create_task(run_in_thread())
        try:
            while not task.done():
                try:
                    step = await asyncio.wait_for(queue.get(), timeout=0.3)
                    yield f"event: step\ndata: {json.dumps(step, default=str)}\n\n"
                except asyncio.TimeoutError:
                    continue
            # Drain remaining steps
            while not queue.empty():
                step = queue.get_nowait()
                yield f"event: step\ndata: {json.dumps(step, default=str)}\n\n"

            result = await task
            # Save to Lakebase (best-effort)
            title = req.message[:80] if not req.history else None
            _try_save(req.session_id, "user", req.message, title=title, user_email=user_email)
            _try_save(req.session_id, "assistant", result["response"], sources=result.get("sources"), user_email=user_email, steps=result.get("steps"))

            yield f"event: done\ndata: {json.dumps(result, default=str)}\n\n"
        except Exception as e:
            logger.error(f"Stream error: {e}", exc_info=True)
            yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/api/conversations")
async def api_list_conversations(request: Request):
    try:
        user_email = _get_user_email(request)
        convs = list_conversations(user_email)
        return JSONResponse([{
            "session_id": c["session_id"],
            "title": c["title"],
            "created_at": c["created_at"].isoformat() if c.get("created_at") else None,
            "updated_at": c["updated_at"].isoformat() if c.get("updated_at") else None,
        } for c in convs])
    except Exception as e:
        logger.error(f"List conversations error: {e}", exc_info=True)
        return JSONResponse([], status_code=200)


@app.get("/api/conversations/{session_id}")
async def api_get_conversation(session_id: str, request: Request):
    try:
        user_email = _get_user_email(request)
        conv = get_conversation(session_id, user_email)
        if not conv:
            return JSONResponse({"error": "Not found"}, status_code=404)
        for m in conv.get("messages", []):
            if m.get("created_at"):
                m["created_at"] = m["created_at"].isoformat()
        if conv.get("created_at"):
            conv["created_at"] = conv["created_at"].isoformat()
        return JSONResponse(conv)
    except Exception as e:
        logger.error(f"Get conversation error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.delete("/api/conversations/{session_id}")
async def api_delete_conversation(session_id: str, request: Request):
    try:
        user_email = _get_user_email(request)
        delete_conversation(session_id, user_email)
        return JSONResponse({"ok": True})
    except Exception as e:
        logger.error(f"Delete conversation error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/me")
async def api_me(request: Request):
    return {"email": _get_user_email(request)}


@app.get("/api/health")
async def health():
    return {"status": "ok"}


app.mount("/static", StaticFiles(directory=FRONTEND_DIR / "static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def index():
    return (FRONTEND_DIR / "index.html").read_text()
