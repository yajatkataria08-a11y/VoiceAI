"""
app/api/dashboard.py
Auth-gated HTML admin dashboard with emotion sparklines.
"""
from __future__ import annotations
import base64
import html
import json
import secrets

from fastapi import APIRouter
from fastapi.requests import Request
from fastapi.responses import HTMLResponse

from app.core.config import DASHBOARD_TOKEN
from app.database.db import db_pool

router = APIRouter()


def _emotion_sparkline(emotions_json_str: str) -> str:
    try:
        data = json.loads(emotions_json_str or "[]")
    except Exception:
        data = []
    if not data:
        return "<svg width='80' height='20'></svg>"
    colors = {
        "urgent": "#ef4444", "angry": "#f97316", "frustrated": "#eab308",
        "sad": "#6366f1", "confused": "#8b5cf6", "happy": "#22c55e", "neutral": "#94a3b8",
    }
    w, h = 80, 20
    bw   = max(2, w // max(len(data), 1))
    bars = "".join(
        f'<rect x="{i*bw}" y="{h - max(2, int(h * float(t.get("confidence", 0.5))))} "'
        f' width="{bw-1}" height="{max(2, int(h * float(t.get("confidence", 0.5))))}"'
        f' fill="{colors.get(t.get("emotion", "neutral"), "#94a3b8")}"/>'
        for i, t in enumerate(data)
    )
    return f'<svg width="{w}" height="{h}" style="vertical-align:middle">{bars}</svg>'


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    if DASHBOARD_TOKEN:
        auth = request.headers.get("Authorization", "")
        authed = False
        if auth.startswith("Bearer "):
            authed = secrets.compare_digest(auth[7:], DASHBOARD_TOKEN)
        elif auth.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth[6:]).decode()
                _, pwd  = decoded.split(":", 1)
                authed  = secrets.compare_digest(pwd, DASHBOARD_TOKEN)
            except Exception:
                authed = False
        if not authed:
            return HTMLResponse(
                "<h2>401 Unauthorized</h2>",
                status_code=401,
                headers={"WWW-Authenticate": 'Basic realm="VoiceAI Dashboard"'},
            )

    if db_pool is None:
        return HTMLResponse("<h2>Database not configured.</h2>", status_code=503)

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT id, caller, start_time, duration_seconds, language,
                      dominant_emotion, topics, flagged, ab_bucket, emotions, satisfied
               FROM calls ORDER BY start_time DESC LIMIT 50"""
        )

    rows_html = ""
    for r in rows:
        topics_str = ", ".join(json.loads(r["topics"] or "[]"))
        flag_icon  = "🚨" if r["flagged"] else ""
        satisfied  = "👍" if r.get("satisfied") is True else ("👎" if r.get("satisfied") is False else "")
        sparkline  = _emotion_sparkline(r["emotions"] or "[]")
        bg         = "background:#fff1f2" if r["flagged"] else ""
        rows_html += f"""
        <tr style="{bg}">
          <td>{r['id']}</td>
          <td>{html.escape(r['caller'] or '')}</td>
          <td>{r['start_time'].strftime('%Y-%m-%d %H:%M') if r['start_time'] else ''}</td>
          <td>{r['duration_seconds'] or 0}s</td>
          <td>{html.escape(r['language'] or '')}</td>
          <td>{html.escape(r['dominant_emotion'] or '')}</td>
          <td>{sparkline}</td>
          <td>{html.escape(topics_str)}</td>
          <td>{html.escape(r['ab_bucket'] or '')}</td>
          <td>{satisfied}</td>
          <td>{flag_icon}</td>
        </tr>"""

    return HTMLResponse(f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <title>VoiceAI Dashboard</title>
  <style>
    body{{font-family:system-ui,sans-serif;margin:0;background:#f8fafc;color:#1e293b}}
    header{{background:#1e293b;color:#f8fafc;padding:1rem 2rem;display:flex;align-items:center;gap:1rem}}
    header h1{{margin:0;font-size:1.4rem}}
    .badge{{background:#3b82f6;color:#fff;border-radius:999px;padding:2px 10px;font-size:.75rem;font-weight:700}}
    main{{padding:1.5rem 2rem}}
    table{{border-collapse:collapse;width:100%;font-size:.85rem;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 1px 4px #0001}}
    th{{background:#1e293b;color:#f8fafc;padding:.6rem .8rem;text-align:left}}
    td{{padding:.5rem .8rem;border-bottom:1px solid #e2e8f0}}
    tr:last-child td{{border-bottom:none}}
    tr:hover td{{background:#f1f5f9}}
  </style>
</head>
<body>
  <header>
    <h1>📞 VoiceAI Admin Dashboard</h1>
    <span class="badge">Last 50 calls</span>
  </header>
  <main>
    <table>
      <thead><tr>
        <th>#</th><th>Caller</th><th>Time</th><th>Duration</th>
        <th>Lang</th><th>Dominant Emotion</th><th>Arc</th>
        <th>Topics</th><th>A/B</th><th>Rating</th><th>Flag</th>
      </tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
  </main>
</body>
</html>""")
