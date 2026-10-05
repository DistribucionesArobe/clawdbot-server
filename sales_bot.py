"""
sales_bot.py — Bot de ventas de CotizaExpress.

Atiende mensajes entrantes al WhatsApp de ventas de CotizaExpress.
Responde preguntas sobre el producto, explica planes/precios,
y dirige a registro o agendar llamada.
"""

import logging
import os
from openai import OpenAI

log = logging.getLogger("cotizaexpress.sales_bot")

# ── Config ──────────────────────────────────────────────────────────────────

CX_SALES_PHONE_NUMBER_ID = (os.getenv("CX_SALES_PHONE_NUMBER_ID") or "").strip()
CX_SALES_WA_API_KEY = (
    os.getenv("CX_SALES_WA_API_KEY")
    or os.getenv("WHATSAPP_ACCESS_TOKEN")
    or os.getenv("WA_API_KEY")
    or ""
).strip()

REGISTRO_URL = "https://cotizaexpress.com/registro"
AGENDAR_URL = "https://calendly.com/cotizaexpress"  # update if different
WHATSAPP_VENTAS = "+52 81 3085 0381"

SYSTEM_PROMPT = """Eres el asistente de ventas de CotizaExpress por WhatsApp. Tu trabajo es atender a personas interesadas en contratar CotizaExpress para su negocio.

## Qué es CotizaExpress
CotizaExpress es un bot de inteligencia artificial que automatiza cotizaciones por WhatsApp para negocios mayoristas (ferreterías, distribuidoras, refaccionarias, materiales de construcción, etc.).

Cuando un cliente del negocio envía un mensaje de WhatsApp pidiendo precios, el bot:
- Interpreta el pedido con IA (texto, fotos, PDFs)
- Busca los productos en el catálogo del negocio
- Responde en menos de 30 segundos con una cotización profesional
- Genera un PDF descargable de la cotización
- Maneja descuentos por volumen
- Funciona 24/7 sin intervención humana

## Planes y precios
- **Plan CotizaBot**: $1,000 MXN/mes (neto)
  - Bot IA en WhatsApp
  - Cotizaciones automáticas ilimitadas
  - Panel de control con estadísticas
  - Catálogo de productos ilimitado

- **Plan Pro**: $2,000 MXN/mes (neto)
  - Todo lo de CotizaBot +
  - Link de pago Mercado Pago
  - Datos SPEI automáticos
  - Notificaciones de pago
  - Configuración de CLABE

## Proceso de activación
1. Se registran en cotizaexpress.com/registro
2. Suben su lista de productos (Excel, copiar/pegar, o carga rápida)
3. Conectan su WhatsApp Business
4. ¡Listo! El bot comienza a responder cotizaciones

Nuestro equipo ayuda con la primera carga de productos. La activación depende de la conexión de WhatsApp; no prometas un plazo fijo ni que conservará la app y sus chats hasta verificar la modalidad de conexión.

## Reglas de conversación
- Sé amable, profesional y entusiasta pero no empalagoso
- Responde en español mexicano natural
- Sé breve — máximo 2-3 párrafos por mensaje
- Si preguntan algo técnico que no sabes, ofrece solicitar ayuda al equipo, sin afirmar que ya lo notificaste
- Si piden una persona, solicita nombre y horario preferido, sin prometer una llamada confirmada
- Nunca indiques eliminar la cuenta de WhatsApp
- No inventes disponibilidad, promociones, confirmaciones de pago ni citas
- Siempre busca llevarlos a registrarse o agendar una llamada
- Si preguntan por el programa de afiliados, dirígelos a cotizaexpress.com/afiliados
- NO inventes funciones que no existen
- NO des precios diferentes a los listados
- Usa emojis con moderación (1-2 por mensaje máximo)
- Si dicen "hola" o es su primer mensaje, preséntate y pregunta en qué les puedes ayudar
"""


# ── Conversation memory (in-process, resets on deploy) ─────────────────────

_conversations: dict[str, list[dict]] = {}
_MAX_HISTORY = 20  # messages per conversation


def _get_history(phone: str) -> list[dict]:
    if phone not in _conversations:
        _conversations[phone] = []
    return _conversations[phone]


def _add_message(phone: str, role: str, content: str):
    hist = _get_history(phone)
    hist.append({"role": role, "content": content})
    # Trim to keep memory bounded
    if len(hist) > _MAX_HISTORY:
        _conversations[phone] = hist[-_MAX_HISTORY:]


# ── Main reply function ────────────────────────────────────────────────────

def build_sales_reply(from_phone: str, user_text: str) -> str | dict:
    """Generate a sales bot reply for an incoming message."""
    if not user_text.strip():
        return ""

    _add_message(from_phone, "user", user_text)

    try:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            log.error("SALES BOT: No OPENAI_API_KEY")
            return _fallback_reply(user_text)

        client = OpenAI(api_key=api_key)

        messages = [{"role": "system", "content": SYSTEM_PROMPT}]
        messages.extend(_get_history(from_phone))

        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            max_tokens=500,
            temperature=0.7,
        )

        reply_text = (resp.choices[0].message.content or "").strip()
        if not reply_text:
            reply_text = _fallback_reply(user_text)

        _add_message(from_phone, "assistant", reply_text)

        # Check if we should add action buttons
        text_lower = reply_text.lower()
        if any(w in text_lower for w in ["registr", "prueb", "activ", "empez", "comenz"]):
            return {
                "type": "text_then_buttons",
                "text": reply_text,
                "body": "¿Qué te gustaría hacer?",
                "buttons": ["📝 Registrarme", "📞 Agendar llamada", "💰 Ver precios"],
            }

        return reply_text

    except Exception as e:
        log.error("SALES BOT GPT ERROR: %s", repr(e))
        return _fallback_reply(user_text)


def handle_sales_button(button_title: str) -> str | dict:
    """Handle button clicks from the sales bot."""
    title_lower = button_title.lower().strip()

    if "registr" in title_lower:
        return (
            f"¡Excelente! Puedes empezar tu registro aquí:\n\n"
            f"👉 {REGISTRO_URL}\n\n"
            f"Si necesitas ayuda con la configuración, escríbenos y te guiamos paso a paso."
        )
    elif "llamada" in title_lower or "agendar" in title_lower:
        return (
            f"Con gusto te agendamos una llamada para mostrarte cómo funciona.\n\n"
            "Dime tu nombre y horario preferido para solicitar atención del equipo.\n\n"
            "La llamada queda pendiente de confirmación por una persona."
        )
    elif "precio" in title_lower:
        return {
            "type": "text_then_buttons",
            "text": (
                "💰 *Planes CotizaExpress*\n\n"
                "*CotizaBot* — $1,000 MXN/mes\n"
                "• Bot IA en WhatsApp\n"
                "• Cotizaciones ilimitadas\n"
                "• Panel de control\n"
                "• Catálogo ilimitado\n\n"
                "*Pro* — $2,000 MXN/mes\n"
                "• Todo lo anterior +\n"
                "• Links de pago Mercado Pago\n"
                "• Datos SPEI automáticos\n"
                "• Notificaciones de pago\n\n"
                "Sin contratos. Cancela cuando quieras."
            ),
            "body": "¿Listo para empezar?",
            "buttons": ["📝 Registrarme", "📞 Agendar llamada"],
        }

    return build_sales_reply("unknown", button_title)


def _fallback_reply(user_text: str) -> str:
    """Simple fallback when GPT is unavailable."""
    return (
        "¡Hola! Gracias por tu interés en CotizaExpress 😊\n\n"
        "Somos un bot de IA que automatiza cotizaciones por WhatsApp para negocios mayoristas.\n\n"
        "• Plan CotizaBot: $1,000 MXN/mes\n"
        "• Plan Pro: $2,000 MXN/mes\n\n"
        f"Regístrate aquí: {REGISTRO_URL}\n"
        "Si prefieres atención de una persona, deja aquí tu nombre y horario preferido."
    )


def is_sales_number(phone_number_id: str) -> bool:
    """Check if the incoming webhook is for the CotizaExpress sales number."""
    return bool(CX_SALES_PHONE_NUMBER_ID and phone_number_id == CX_SALES_PHONE_NUMBER_ID)
