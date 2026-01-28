from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Clawdbot Server", version="1.0")

class ChatRequest(BaseModel):
    message: str
    user_id: str = None
    source: str = "web"
    country: str = "MX"

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/api/chat")
def chat(req: ChatRequest):
    msg = (req.message or "").strip()
    if "america" in msg.lower():
        return {"reply": "⚽ (demo clawdbot) América: conexión OK. Aquí irán canales/streaming reales."}
    return {"reply": f"✅ (demo clawdbot) Recibí: {req.message}"}
