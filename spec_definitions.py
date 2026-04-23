# spec_definitions.py
import re

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

# Products that should NOT trigger spec steps when they already have
# enough detail (calibre, measurements, fractions, or rejacero context)
_SKIP_SPEC_IF_HAS_DETAIL = {"poste", "canal", "canal liston", "canal carga", "angulo"}

# Words that indicate the product is NOT a tablaroca/construction product
# (e.g., rejacero postes are different from tablaroca postes)
_REJACERO_CONTEXT_WORDS = {"rejacero", "reja", "malla", "cerca", "cerco", "abrazadera",
                           "base para poste", "bases para poste", "deacero", "clasica",
                           "clásica", "ciclonica", "ciclónica"}


def get_spec_steps(product_raw: str) -> list:
    """Retorna pasos de spec si el producto los requiere, [] si no."""
    n = (product_raw or "").lower()

    # Try longer keys first to avoid "canal" matching before "canal liston"
    sorted_keys = sorted(SPEC_STEPS.keys(), key=len, reverse=True)
    for key in sorted_keys:
        if key in n:
            # For products that have multiple domains (e.g., poste tablaroca vs poste rejacero),
            # skip spec steps if the product already has specific measurements, calibre,
            # fractions, or rejacero context — let smart_search handle it
            if key in _SKIP_SPEC_IF_HAS_DETAIL:
                # Has a calibre mentioned (cal 16, cal 20, calibre 22, etc.)
                if re.search(r'\bcal(?:ibre)?\s*\d+', n):
                    return []
                # Has fraction measurements (2 1/4", 1/2", etc.)
                if re.search(r'\d+\s*/\s*\d+', n):
                    return []
                # Has decimal measurements (4.10, 6.35, 3.05, etc.)
                if re.search(r'\d+\.\d+', n):
                    return []
                # Has rejacero context words
                if any(w in n for w in _REJACERO_CONTEXT_WORDS):
                    return []
                # Has "altura" or height spec (1.00 m de altura)
                if re.search(r'\baltura\b', n):
                    return []
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
