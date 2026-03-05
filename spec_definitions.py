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
}


def get_spec_steps(product_raw: str) -> list:
    """Retorna pasos de spec si el producto los requiere, [] si no."""
    n = (product_raw or "").lower()
    for key, steps in SPEC_STEPS.items():
        if key in n:
            return steps
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
