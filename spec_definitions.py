# spec_definitions.py

SPEC_STEPS = {
    "varilla": [
        {
            "key": "diametro",
            "question": "¿Qué diámetro de varilla necesitas?",
            "options": ['3/8"', '1/2"', '5/8"', '1"'],
        }
    ],
    "cable": [
        {
            "key": "calibre",
            "question": "¿Qué calibre de cable eléctrico necesitas?",
            "options": ["Cal 8", "Cal 10", "Cal 12", "Cal 14"],
        }
    ],
    "tubo pvc": [
        {
            "key": "diametro",
            "question": "¿Qué diámetro de tubo PVC necesitas?",
            "options": ['1/2"', '3/4"', '1"', '2"'],
        },
        {
            "key": "largo",
            "question": "¿Y qué largo necesitas?",
            "options": ["3m", "6m"],
        },
    ],
    "poste": [
        {
            "key": "medida",
            "question": "¿Qué medida de poste necesitas?",
            "options": ["3.05", "4.10", "6.35"],
        },
        {
            "key": "calibre",
            "question": "¿Y qué calibre?",
            "options": ["Cal 22", "Cal 26"],
        },
    ],
    "canal liston": [
        {
            "key": "medida",
            "question": "¿Qué medida de canal listón necesitas?",
            "options": ["3.05", "4.10", "6.35"],
        },
        {
            "key": "calibre",
            "question": "¿Y qué calibre?",
            "options": ["Cal 22", "Cal 26"],
        },
    ],
    "canal carga": [
        {
            "key": "medida",
            "question": "¿Qué medida de canal de carga necesitas?",
            "options": ["3.05", "4.10", "6.35"],
        },
        {
            "key": "calibre",
            "question": "¿Y qué calibre?",
            "options": ["Cal 22", "Cal 26"],
        },
    ],
    "canal": [
        {
            "key": "tipo",
            "question": "¿Qué tipo de canal necesitas?",
            "options": ["Canal listón", "Canal de carga"],
        },
        {
            "key": "medida",
            "question": "¿Qué medida?",
            "options": ["3.05", "4.10", "6.35"],
        },
        {
            "key": "calibre",
            "question": "¿Y qué calibre?",
            "options": ["Cal 22", "Cal 26"],
        },
    ],
    "angulo": [
        {
            "key": "tipo",
            "question": "¿Qué tipo de ángulo necesitas?",
            "options": ["Ángulo de amarre", "Ángulo esquinero"],
        },
    ],
}


def get_spec_steps(product_raw: str) -> list:
    """Retorna pasos de spec si el producto los requiere, [] si no."""
    n = (product_raw or "").lower()
    # Try longer keys first to avoid "canal" matching before "canal liston"
    sorted_keys = sorted(SPEC_STEPS.keys(), key=len, reverse=True)
    for key in sorted_keys:
        if key in n:
            return SPEC_STEPS[key]
    return []


def already_has_specs(product_raw: str, steps: list) -> bool:
    n = (product_raw or "").lower()
    for step in steps:
        if not any(opt.lower().replace('"', '').replace("'", '') in n for opt in step["options"]):
            return False
    return True


def build_spec_query(raw: str, resolved: dict) -> str:
    parts = [raw.strip()]
    for v in resolved.values():
        clean = v.replace('"', '').replace("'", '').strip()
        # Solo agregar si no está ya en el raw
        if clean.lower() not in raw.lower():
            parts.append(clean)
    return " ".join(parts)
