"""
whatsapp_api.py — WhatsApp Cloud API helpers for CotizaExpress.

Thin wrappers around the Meta Graph API for sending messages,
downloading media, and extracting text from images via OpenAI Vision.
"""

import io
import logging
import os

import requests

log = logging.getLogger("cotizaexpress.whatsapp")

WA_API_BASE = "https://graph.facebook.com/v19.0"


# ── Sending messages ─────────────────────────────────────────────────────

def send_whatsapp_text(wa_api_key: str, phone_number_id: str, to: str, text: str):
    url = f"{WA_API_BASE}/{phone_number_id}/messages"
    headers = {"Authorization": f"Bearer {wa_api_key}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": to, "type": "text", "text": {"body": text}}
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    if r.status_code >= 300:
        raise RuntimeError(f"WhatsApp send failed {r.status_code}: {r.text[:400]}")


def send_whatsapp_list(wa_api_key: str, phone_number_id: str, to: str,
                       body_text: str, options: list, button_label: str = "Ver opciones"):
    rows = [{"id": f"spec_{i}", "title": opt[:24]} for i, opt in enumerate(options[:10])]
    payload = {
        "messaging_product": "whatsapp", "to": to, "type": "interactive",
        "interactive": {
            "type": "list",
            "body": {"text": body_text},
            "action": {"button": button_label, "sections": [{"title": "Opciones", "rows": rows}]},
        },
    }
    url = f"{WA_API_BASE}/{phone_number_id}/messages"
    headers = {"Authorization": f"Bearer {wa_api_key}", "Content-Type": "application/json"}
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    if r.status_code >= 300:
        raise RuntimeError(f"WA list failed {r.status_code}: {r.text[:400]}")


def send_whatsapp_list_sections(wa_api_key: str, phone_number_id: str, to: str,
                                body_text: str, sections: list, button_label: str = "Ver opciones"):
    payload = {
        "messaging_product": "whatsapp", "to": to, "type": "interactive",
        "interactive": {
            "type": "list",
            "body": {"text": body_text},
            "action": {"button": button_label, "sections": sections},
        },
    }
    url = f"{WA_API_BASE}/{phone_number_id}/messages"
    headers = {"Authorization": f"Bearer {wa_api_key}", "Content-Type": "application/json"}
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    if r.status_code >= 300:
        raise RuntimeError(f"WA list sections failed {r.status_code}: {r.text[:400]}")


# ── Media ────────────────────────────────────────────────────────────────

def download_whatsapp_media(image_id: str, wa_api_key: str) -> bytes:
    url_resp = requests.get(
        f"{WA_API_BASE}/{image_id}",
        headers={"Authorization": f"Bearer {wa_api_key}"},
        timeout=10,
    )
    url_resp.raise_for_status()
    media_url = url_resp.json()["url"]
    img_resp = requests.get(media_url, headers={"Authorization": f"Bearer {wa_api_key}"}, timeout=15)
    img_resp.raise_for_status()
    return img_resp.content


def extract_text_from_image(image_bytes: bytes) -> str | None:
    """Use OpenAI Vision to extract product lists from images."""
    try:
        from openai import OpenAI
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return None
        client = OpenAI(api_key=api_key)
        import base64
        b64 = base64.b64encode(image_bytes).decode()
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}", "detail": "high"}},
                    {"type": "text", "text": (
                        "Eres asistente de ferretería mexicana. Esta imagen contiene una lista "
                        "de materiales. Puede ser una nota manuscrita, una tabla digital, una "
                        "captura de pantalla, o una foto de un pedido. "
                        "Extrae TODOS los productos con sus cantidades, sin omitir ninguno. "
                        "Formato estricto: CANTIDAD PRODUCTO, un item por línea. "
                        "La cantidad siempre debe ser un número entero (sin decimales). "
                        "Si ves '175.00', escribe '175'. Si ves '20.00', escribe '20'. "
                        "Si la imagen es una tabla con columnas (ej: Conceptos | Cantidad), "
                        "lee CADA fila y pon la cantidad antes del nombre del producto. "
                        "Productos comunes: poste, tablaroca, cemento, varilla, block, "
                        "malla, perfacinta, redimix, canal, tornillo, clavo, tubo, pija, "
                        "durock, basecoat, ángulo, canaleta, reborde, taquete. "
                        "Conserva medidas y especificaciones tal cual aparecen (ej: 'Cal 26', '1/2\"', '6 x 1', '10 x 1 1/2'). "
                        "Si una palabra parece un producto con error ortográfico, corrígela. "
                        "Ignora encabezados, totales, fechas, logos y textos que no sean productos. "
                        "Si un renglón existe pero no puedes leerlo, escribe: 1 ???. "
                        "NO agregues productos que no estén en la imagen. "
                        "Ejemplo de salida:\n20 tablaroca ultralight USG\n50 ángulo amarre cal 26\n1200 pija 6 x 1\n"
                        "Si no hay lista de productos en absoluto, responde exactamente: NO_LIST"
                    )}
                ]
            }],
            max_tokens=800, temperature=0.1,
        )
        result = (resp.choices[0].message.content or "").strip()
        return None if result == "NO_LIST" else result
    except Exception as e:
        log.error("VISION ERROR: %s", repr(e))
        return None


# ── WhatsApp Business Profile ───────────────────────────────────────────

_META_APP_ID = "1461694011992339"


def update_wa_profile_photo(wa_api_key: str, phone_number_id: str,
                            img_bytes: bytes, mime_type: str = "image/png") -> dict:
    """Upload an image and set it as the WhatsApp Business profile photo.

    Uses the Resumable Upload API to get a file handle, then sets it
    as profile_picture_handle on the WhatsApp Business Profile.

    Returns dict with keys: ok (bool), step, error (optional).
    """
    headers = {"Authorization": f"Bearer {wa_api_key}"}
    file_size = len(img_bytes)

    # Step 1: Create a resumable upload session
    try:
        session_resp = requests.post(
            f"{WA_API_BASE}/{_META_APP_ID}/uploads",
            headers=headers,
            params={
                "file_length": file_size,
                "file_type": mime_type,
                "access_token": wa_api_key,
            },
            timeout=30,
        )
    except Exception as e:
        log.error("WA PROFILE: upload session error: %s", repr(e))
        return {"ok": False, "step": "create_session", "error": str(e)}

    if session_resp.status_code != 200:
        log.error("WA PROFILE: upload session failed %s: %s",
                  session_resp.status_code, session_resp.text[:500])
        return {"ok": False, "step": "create_session",
                "error": session_resp.text[:500]}

    upload_session_id = session_resp.json().get("id")
    log.info("WA PROFILE: upload session created: %s", upload_session_id)

    # Step 2: Upload the file data to the session
    try:
        upload_resp = requests.post(
            f"{WA_API_BASE}/{upload_session_id}",
            headers={
                "Authorization": f"OAuth {wa_api_key}",
                "file_offset": "0",
                "Content-Type": mime_type,
            },
            data=img_bytes,
            timeout=30,
        )
    except Exception as e:
        log.error("WA PROFILE: file upload error: %s", repr(e))
        return {"ok": False, "step": "upload_file", "error": str(e)}

    if upload_resp.status_code != 200:
        log.error("WA PROFILE: file upload failed %s: %s",
                  upload_resp.status_code, upload_resp.text[:500])
        return {"ok": False, "step": "upload_file",
                "error": upload_resp.text[:500]}

    file_handle = upload_resp.json().get("h")
    log.info("WA PROFILE: file handle obtained: %s", file_handle[:50] if file_handle else "None")

    if not file_handle:
        return {"ok": False, "step": "upload_file",
                "error": "No file handle returned"}

    # Step 3: Set as profile picture using the handle
    try:
        profile_resp = requests.post(
            f"{WA_API_BASE}/{phone_number_id}/whatsapp_business_profile",
            headers={**headers, "Content-Type": "application/json"},
            json={"messaging_product": "whatsapp",
                  "profile_picture_handle": file_handle},
            timeout=30,
        )
    except Exception as e:
        log.error("WA PROFILE: profile update error: %s", repr(e))
        return {"ok": False, "step": "profile_update", "error": str(e)}

    ok = profile_resp.status_code == 200
    log.info("WA PROFILE: profile update %s — %s",
             "OK" if ok else "FAILED", profile_resp.text[:300])
    return {"ok": ok, "step": "profile_update",
            "response": profile_resp.text[:500]}


# ── Phone normalization ──────────────────────────────────────────────────

def normalize_mx_phone(phone: str) -> str:
    """Normalize a Mexican phone number to include country code 52."""
    p = (phone or "").replace("+", "").replace(" ", "").replace("-", "").replace("whatsapp:", "").strip()
    if len(p) == 10 and p.isdigit():
        p = "52" + p
    return p


# ── Owner notifications ──────────────────────────────────────────────────

def notify_owner_escalation(wa_api_key: str, phone_number_id: str, owner_phone: str,
                            client_phone: str, reason: str, state: dict):
    owner_phone_clean = normalize_mx_phone(owner_phone)
    cart = (state or {}).get("cart") or []
    cart_txt = ""
    if cart:
        lines = [f"• {it['qty']}x {it['name']} — ${float(it.get('price',0))*int(it.get('qty',0)):,.2f}" for it in cart]
        cart_txt = "\n" + "\n".join(lines)
    msg = (
        f"⚠️ *Cliente necesita un asesor*\n"
        f"📱 {client_phone}\n"
        f"❓ Motivo: {reason}\n"
        f"🛒 Carrito actual:{cart_txt if cart_txt else ' (vacío)'}\n\n"
        f"Responde directo a ese número."
    )
    send_whatsapp_text(wa_api_key, phone_number_id, owner_phone_clean, msg)


def notify_owner_comprobante(wa_api_key: str, phone_number_id: str, owner_phone: str,
                             client_phone: str, state: dict):
    owner_phone_clean = normalize_mx_phone(owner_phone)
    cart = (state or {}).get("cart") or []
    cart_txt = ""
    if cart:
        lines = [f"• {it['qty']}x {it['name']} — ${float(it.get('price',0))*int(it.get('qty',0)):,.2f}" for it in cart]
        cart_txt = "\n" + "\n".join(lines)
    msg = (
        f"💰 *Comprobante de pago recibido*\n"
        f"📱 Cliente: {client_phone}\n"
        f"🛒 Cotización:{cart_txt if cart_txt else ' (sin carrito)'}\n\n"
        f"Revisa tu WhatsApp — el cliente acaba de mandar el comprobante."
    )
    send_whatsapp_text(wa_api_key, phone_number_id, owner_phone_clean, msg)
