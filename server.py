
from fastapi import FastAPI, Header
from pydantic import BaseModel

app = FastAPI(title="Clawdbot Server", version="1.0")

class ChatRequest(BaseModel):
    app: str = "cotizabot"
    message: str
    user_id: str = None
    source: str = "web"
    country: str = "MX"


@app.get("/health")
def health():
    return {"ok": True}

@app.get("/api/health")
def api_health():
    return {"ok": True}


@app.post("/api/chat")
def chat(req: ChatRequest, authorization: str = Header(default="")):


    app_id = (req.app or "cotizabot").lower().strip()
    msg = (req.message or "").lower().strip()



    # --- CotizaBot ---
    if app_id == "cotizabot":
        quote_kw = [
            "cotiza", "cotización", "cotizacion", "precio", "cuánto", "cuanto",
            "costo", "m2", "metros", "tablaroca", "durock", "pijas", "panel", "perfil"
        ]
        if any(k in msg for k in quote_kw):
            return {"reply": "📦 *CotizaBot*: Dime 1) ciudad 2) producto y cantidades (o m²) 3) ¿con IVA?"}
        return {"reply": "📦 *CotizaBot*: ¿Qué quieres cotizar? (ej: 'tablaroca 20 hojas en MTY con IVA')"}

    # --- DóndeVer ---
    if app_id == "dondever":
        sports_kw = [
            "america", "américa", "chivas", "tigres", "rayados",
            "liga mx", "champions", "nba", "nfl", "donde ver", "canal", "stream"
        ]
        if any(k in msg for k in sports_kw):
            return {"reply": "⚽ *DóndeVer*: Dime el partido y el país (MX/USA) y te digo canales/plataformas."}
        return {"reply": "⚽ *DóndeVer*: ¿Qué partido buscas?"}

    # --- EntiendeUSA ---
    if app_id == "entiendeusa":
        if not msg:
            return {"reply": "🇺🇸 *EntiendeUSA*: mándame el texto a traducir o explicar."}
        return {"reply": f"🇺🇸 *EntiendeUSA* (demo): recibí '{req.message}'."}

    return {"reply": f"App '{app_id}' no existe. Usa: cotizabot | dondever | entiendeusa"}
