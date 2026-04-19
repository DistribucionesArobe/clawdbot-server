"""
calculators.py — Construction calculators for CotizaExpress.

Contains material quantity formulas for drywall (tablaroca/durock)
walls and ceilings, plus configuration data.
"""

import math
import re

# ── Tipos de construcción disponibles ────────────────────────────────────

CONSTRUCCION_TIPOS = {
    "muro tablaroca": {
        "label": "Muro Tablaroca",
        "inputs": ["alto_muro", "largo_muro"],
        "preguntas": {
            "alto_muro": "📐 ¿Cuántos metros de *alto* tiene el muro? (ej: 2.44)",
            "largo_muro": "📏 ¿Cuántos metros de *largo* tiene el muro? (ej: 10)",
        },
    },
    "muro durock": {
        "label": "Muro Durock",
        "inputs": ["alto_muro", "largo_muro"],
        "preguntas": {
            "alto_muro": "📐 ¿Cuántos metros de *alto* tiene el muro? (ej: 2.44)",
            "largo_muro": "📏 ¿Cuántos metros de *largo* tiene el muro? (ej: 10)",
        },
    },
    "plafon tablaroca": {
        "label": "Plafón Tablaroca",
        "inputs": ["largo", "ancho"],
        "preguntas": {
            "largo": "📏 ¿Cuántos metros de *largo* tiene el plafón? (ej: 12)",
            "ancho": "📐 ¿Cuántos metros de *ancho* tiene el plafón? (ej: 8)",
        },
    },
    "plafon reticulado": {
        "label": "Plafón Reticulado",
        "inputs": ["largo", "ancho"],
        "preguntas": {
            "largo": "📏 ¿Cuántos metros de *largo* tiene el plafón? (ej: 12)",
            "ancho": "📐 ¿Cuántos metros de *ancho* tiene el plafón? (ej: 5)",
        },
    },
}

CONSTRUCCION_PRODUCTOS = {
    "muro tablaroca": [
        "Tablaroca ultralight usg",
        "Poste 6.35 x 3.05 cal 26",
        "Canal 6.35 x 3.05 cal 26",
        "Redimix 21.8 kg usg",
        "Pija 6 x 1",
        "Pija framer",
        "Perfacinta",
    ],
    "muro durock": [
        "Durock usg",
        "Poste 6.35 x 3.05 cal 20",
        "Canal 6.35 x 3.05 cal 22",
        "Basecoat usg",
        "Pija para durock",
        "Pija framer",
        "Cinta fibra de vidrio",
    ],
    "plafon tablaroca": [
        "Tablaroca ultralight usg",
        "Canal listón cal 26",
        "Ángulo de amarre cal 26",
        "Canaleta de carga cal 24",
        "Redimix 21.8 kg usg",
        "Pija 6 x 1",
        "Pija framer",
        "Perfacinta",
        "Alambre galvanizado liso cal 12.5",
    ],
    "plafon reticulado": [
        "Plafón radar 61 x 61",
        "Tee principal",
        "Tee 1.22",
        "Tee 61",
        "Ángulo perimetral",
        "Alambre galvanizado liso cal 12.5",
    ],
}


# ── Cálculo puro de cantidades ───────────────────────────────────────────

def _ceil_hundreds(n: int) -> int:
    return math.ceil(n / 100) * 100


def calc_muro_tablaroca(alto: float, largo: float) -> list:
    m2 = alto * largo
    tablaroca = math.ceil(math.ceil(m2 / (1.22 * 2.44) * 2 * 1.03) * 1.03)
    pijas = _ceil_hundreds(math.ceil(tablaroca * 30))
    return [
        ("Tablaroca ultralight usg",          tablaroca),
        ("Canal 6.35 x 3.05 cal 26",          math.ceil((largo / 3) * 2)),
        ("Poste 6.35 x 3.05 cal 26",          (math.ceil(largo / 0.61) + 1) * (math.ceil(alto / 3.05) + 1)),
        ("Pija 6 x 1",                        pijas),
        ("Pija framer",                       _ceil_hundreds(math.ceil(pijas / 2))),
        ("Perfacinta",                        math.ceil((m2 / 2.44) / 20)),
        ("Redimix 21.8 kg usg",               math.ceil(m2 / 14)),
    ]


def calc_muro_durock(alto: float, largo: float) -> list:
    m2 = alto * largo
    durock = math.ceil(math.ceil(m2 / (1.22 * 2.44) * 2 * 1.03) * 1.03)
    pijas = _ceil_hundreds(math.ceil(durock * 30))
    return [
        ("Durock usg",                        durock),
        ("Canal 6.35 x 3.05 cal 22",          math.ceil((largo / 3) * 2)),
        ("Poste 6.35 x 3.05 cal 20",          (math.ceil(largo / 0.406) + 1) * (math.ceil(alto / 3.05) + 1)),
        ("Pija para durock",                  pijas),
        ("Pija framer",                       _ceil_hundreds(math.ceil(pijas / 2))),
        ("Cinta fibra de vidrio",             math.ceil((m2 / 2.44) / 20)),
        ("Basecoat usg",                      math.ceil(m2 / 4)),
    ]


def calc_plafon_tablaroca(largo: float, ancho: float) -> list:
    m2 = largo * ancho
    tablaroca = math.ceil(m2 / 2.9768 * 1.07)
    pijas = _ceil_hundreds(math.ceil(tablaroca * 30))
    return [
        ("Tablaroca ultralight usg",          tablaroca),
        ("Canal listón cal 26",               math.ceil(((m2 / 0.61) * 1.05) / 3.05) + 2),
        ("Canaleta de carga cal 24",          math.ceil(((m2 / 1.22) * 1.05) / 3.05)),
        ("Ángulo de amarre cal 26",           math.ceil(((largo * 2) + (ancho * 2)) / 3.05)),
        ("Pija 6 x 1",                        pijas),
        ("Pija framer",                       _ceil_hundreds(math.ceil(pijas / 2))),
        ("Perfacinta",                        math.ceil((m2 * 0.8 * 1.05) / 75)),
        ("Redimix 21.8 kg usg",               math.ceil((m2 * 0.65 * 1.05) / 21.8)),
        ("Alambre galvanizado liso cal 12.5", math.ceil(m2 / 20)),
    ]


def calc_plafon_reticulado(largo: float, ancho: float) -> list:
    m2 = largo * ancho
    return [
        ("Plafón radar 61 x 61",              math.ceil(m2 / 0.36 * 1.03)),
        ("Tee principal",                     math.ceil(m2 * 0.29)),
        ("Tee 1.22",                          math.ceil(m2 * 1.4)),
        ("Tee 61",                            math.ceil(m2 * 1.4)),
        ("Ángulo perimetral",                 math.ceil(((largo * 2) + (ancho * 2)) / 3.05)),
        ("Alambre galvanizado liso cal 12.5", math.ceil(m2 / 20)),
    ]


# Map tipo_key → calculator function
CALC_FUNCTIONS = {
    "muro tablaroca": calc_muro_tablaroca,
    "muro durock": calc_muro_durock,
    "plafon tablaroca": calc_plafon_tablaroca,
    "plafon reticulado": calc_plafon_reticulado,
}


def is_construccion_trigger(text: str) -> bool:
    """Detect if user text is asking for a construction calculator."""
    t = text.strip().lower()
    # Strip accents for matching
    import unicodedata
    t = unicodedata.normalize("NFD", t)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")

    if t in {"cotizar materiales", "🔨 cotizar materiales"}:
        return False
    _other_calc_keywords = ["rejacero", "reja", "pintura", "imper", "impermeabilizante", "calculadoras", "📐"]
    if any(k in t for k in _other_calc_keywords):
        return False
    triggers = [
        "calcula", "calcular",
        "construccion", "construcion",
        "calcular material", "calcular materiales", "calcular m2", "calcular m", "🏗️ calcular m2",
        "muros y plafones", "🏗️ muros y plafones",
        "cuantos materiales", "cuantos materiales",
        "material para", "materiales para",
        "construccion ligera",
        "drywall", "tablaroca construccion",
        "muro tablaroca", "muro durock",
        "plafon tablaroca", "plafon reticulado",
        "cuanto material", "cuanto necesito",
        "m2 muro", "m2 plafon", "m2 tablaroca", "m2 durock",
        "metros muro", "metros plafon",
        "m2 de muro", "m2 de plafon",
        "metros de muro", "metros de plafon",
        "metros cuadrados",
    ]
    if any(tr in t for tr in triggers):
        return True
    if re.search(r"\d+\s*m2", t):
        if any(w in t for w in ["muro", "plafon", "tablaroca", "durock", "pared", "techo"]):
            return True
    return False
