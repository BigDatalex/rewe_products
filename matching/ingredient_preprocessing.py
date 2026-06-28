"""Vorverarbeitung von Rezeptzutaten.

Drei Operationen, in dieser Reihenfolge (sie bauen aufeinander auf):
  1. SPLIT     verknüpfte Zutaten trennen ("Salz und Pfeffer" -> zwei Einträge)
  2. DEDUP     gleiche Zutat pro Rezept zusammenfassen
  3. AGGREGAT  Mengen summieren -- aber nur, wenn eindeutig (gleiche Dimension)

Reine Logik, kein IO. Wird von preprocess_recipes.py auf die rohen
recipeIngredient-Listen angewandt; getestet in
tests/test_ingredient_preprocessing.py.

Konservativ by design: im Zweifel NICHT anfassen. Über-Splitten (Komma,
"/", "oder" als Trenner) oder Summieren über inkompatible Einheiten wäre
schlimmer als gar nichts zu tun.
"""
from __future__ import annotations

import re
from collections import OrderedDict

from quantity_matching import extract_ingredient_quantity, _VAGUE_UNITS

# normalize: identisch zur Pipeline (match_jsonl_cache2.py). Dient als
# Dedup-Schlüssel -- gleiche Zutat -> gleicher Key. Bei Änderungen dort hier
# mitziehen.
_UNITS = (
    r"\b(g|kg|mg|ml|l|el|tl|msp\.?|stück|stk\.?|prise[n]?|becher|dose[n]?|"
    r"pck\.?|packung(en)?|päckchen|zehe\/n|zehe[n]?|bund|scheibe[n]?|"
    r"m\.-große[snr]?|gr\.|kl\.|große[snr]?|kleine[snr]?|etwas|evtl\.?|"
    r"ca\.?|nach belieben)\b"
)


def normalize(text: str) -> str:
    s = str(text).lower()
    s = re.sub(r"^[\d\s.,/½¼¾-]+", "", s)
    s = re.sub(_UNITS, " ", s)
    s = re.sub(r"\(.*?\)", " ", s)
    s = re.sub(r"[^a-zäöüß\s-]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


# ---------------------------------------------------------------------------
# Form-Token: holt die EINE produktunterscheidende Form zurück, die normalize
# wegwirft (Konserve/TK/getrocknet/gemahlen). Ohne Marker = Default (frisch).
#
# Wichtig: jede Klasse fasst ihre Synonyme zusammen (dose/passiert/püriert ->
# "konserve"), damit "Dose Tomaten" und "passierte Tomaten" NICHT über-getrennt
# werden. Bei mehreren Klassen gewinnt die DOMINANTE (Listenreihenfolge), es
# wird also immer höchstens EIN Tag angehängt.
#
# "frisch" ist bewusst KEINE Klasse: es ist mehrdeutig ("frisch gemahlen" =
# Zubereitung, nicht Form) -- die Abwesenheit eines Tags steht für frisch.
_FORM_CLASSES = [
    ("konserve",   re.compile(r"\b(dose|dosen|konserve|konserviert|"
                              r"passiert|püriert)\w*", re.I)),
    ("tk",         re.compile(r"\b(tk|tiefgekühlt|tiefkühl|gefroren)\w*", re.I)),
    ("getrocknet", re.compile(r"\b(getrocknet|gedörrt)\w*", re.I)),
    ("gemahlen",   re.compile(r"\bgemahlen\w*", re.I)),
]


def form_token(raw: str) -> str:
    """Dominantes Form-Tag (oder '' = Default/frisch)."""
    low = str(raw).lower()
    for name, rx in _FORM_CLASSES:
        if rx.search(low):
            return name
    return ""


def dedup_key(raw: str) -> str:
    """Form-bewusster Dedup-/Cache-Schlüssel: normalize + '|' + Form-Tag.

    So kollabieren harmlose Varianten (nur Menge/Zubereitung) weiterhin, aber
    echt verschiedene Formen (frisch vs. Dose vs. getrocknet) bekommen eigene
    Schlüssel -> jede matcht mit ihrem eigenen Rohstring das richtige Produkt.
    """
    return normalize(raw) + "|" + form_token(raw)


# ---------------------------------------------------------------------------
# Schritt 1: verknüpfte Zutaten trennen
# ---------------------------------------------------------------------------

# NUR additive Trenner. Bewusst NICHT dabei:
#   ","     ist meist ein Qualifizierer ("Hackfleisch, gemischtes")
#   "/" / " oder "  sind Alternativen ("Sahne oder Cremefine", "Honig / Sirup")
_SPLIT_RE = re.compile(r"\s+und\s+|\s*\+\s*|\s*&\s*", re.IGNORECASE)

# Phrasen, in denen "und"/"&" NICHT additiv ist -> gar nicht splitten.
_BLOCK_PHRASES = ("halb und halb", "halb & halb", "süß und sauer",
                  "süss und sauer", "sauer und scharf", "kraut und rüben")


def _mask_parens(s: str) -> str:
    """Ersetzt Klammer-INHALT durch 'x', damit Trenner darin nicht greifen.
    Die Klammern selbst bleiben stehen (Positionen unverändert)."""
    out = list(s)
    depth = 0
    for i, ch in enumerate(s):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        elif depth > 0:
            out[i] = "x"
    return "".join(out)


def split_ingredient(raw: str) -> list[str]:
    """Trennt eine Zutatenzeile an additiven Verknüpfungen (und / + / &),
    aber NICHT innerhalb von Klammern. Gibt [raw] zurück, wenn nicht (sicher)
    splitbar."""
    low = raw.lower()
    if any(p in low for p in _BLOCK_PHRASES):
        return [raw]
    # Trennstellen auf der maskierten Kopie suchen, im Original schneiden.
    # Ein "-" direkt vor dem Trenner ist eine Komposita-Ellipse
    # ("Quiche- und Tarteteig") -> dort NICHT trennen.
    spans = [m.span() for m in _SPLIT_RE.finditer(_mask_parens(raw))
             if not raw[:m.start()].rstrip().endswith("-")]
    if not spans:
        return [raw]
    parts, last = [], 0
    for a, b in spans:
        parts.append(raw[last:a])
        last = b
    parts.append(raw[last:])
    parts = [p.strip() for p in parts if p.strip()]
    if len(parts) <= 1:
        return [raw]
    # Guard: jeder Teil muss eine echte Zutat ergeben (normalisiert nicht leer),
    # sonst war "und" wohl kein Zutaten-Trenner -> lieber unverändert lassen.
    if any(not normalize(p) for p in parts):
        return [raw]
    return parts


# ---------------------------------------------------------------------------
# Schritt 2+3: deduplizieren + Mengen aggregieren
# ---------------------------------------------------------------------------

# Führende Menge+Einheit für die Namens-Rekonstruktion entfernen. Die Einheit
# darf NICHT Teil eines längeren Wortes sein (sonst frisst "l" das "L" von
# "Limette") -> negativer Lookahead; längere Einheiten zuerst.
_LEADING_QTY_RE = re.compile(
    r"^\s*\d+(?:[.,]\d+)?\s*(?:(?:liter|kg|mg|ml|cl|g|l)(?![a-zäöüß]))?\s*",
    re.IGNORECASE,
)


def _strip_leading_quantity(raw: str) -> str:
    return _LEADING_QTY_RE.sub("", raw, count=1).strip()


# Vage Einheit am Anfang: Zahl + bekanntes Mengenwort (EL/TL/Prise/Bund/...).
# Der Parser quantity_matching ignoriert diese (nicht in g/ml/Stück umrechenbar),
# wir können aber bei GLEICHER vager Einheit trotzdem die Zahlen summieren.
# Einheit = Wort, optional mit "/n"-Plural, optionalem "(n)/(en)/..."-Marker
# (z.B. "Prise(n)") und optionalem Punkt ("Msp.").
_VAGUE_QTY_RE = re.compile(
    r"^\s*(\d+(?:[.,]\d+)?)\s*"
    r"([a-zäöüß]+(?:/[a-zäöüß]+)?(?:\((?:n|en|e|s|er)\))?\.?)"
    r"\s*(.*)$", re.I
)


def _vague_quantity(raw: str) -> tuple[float, str, str, str] | None:
    """(zahl, einheit_key, einheit_anzeige, name) oder None.

    z.B. "1 TL Gemüsebrühepulver" -> (1.0, "tl", "TL", "Gemüsebrühepulver"),
    "1 Prise(n) Salz" -> (1.0, "prise", "Prise(n)", "Salz").
    """
    m = _VAGUE_QTY_RE.match(raw)
    if not m:
        return None
    unit_disp = m.group(2)
    unit_key = re.sub(r"\(.*?\)", "", unit_disp).lower().rstrip(".").split("/")[0]
    if unit_key not in _VAGUE_UNITS:
        return None
    return float(m.group(1).replace(",", ".")), unit_key, unit_disp, m.group(3).strip()


def _fmt(n: float) -> str:
    return str(int(n)) if float(n).is_integer() else f"{n:g}"


def _reconstruct(dim: str, amount: float, name: str) -> str:
    if dim == "weight":
        return f"{_fmt(amount)} g {name}"
    if dim == "volume":
        return f"{_fmt(amount)} ml {name}"
    return f"{_fmt(amount)} {name}"  # count


def _render_sig(sig: tuple, lst: list) -> str:
    """Summiert die Mengen einer Signatur-Gruppe zu einem Zutaten-String."""
    total = sum(a for a, _, _ in lst)
    name, info = lst[0][1], lst[0][2]
    if sig[0] == "dim":
        return _reconstruct(info, total, name)
    if sig[0] == "vague":
        return f"{_fmt(total)} {info} {name}".strip()
    return f"{_fmt(total)} {name}".strip()  # plain


def preprocess_ingredients(raws: list[str]) -> tuple[list[str], list[dict]]:
    """Split -> Dedup -> (eindeutige) Mengen-Aggregation.

    Gibt (neue_zutaten, ops) zurück. `ops` dokumentiert jede Veränderung
    (split / merge / dedup / skip_ambiguous) für die Provenienz.
    """
    ops: list[dict] = []

    # --- Schritt 1: splitten ---
    items: list[str] = []
    for raw in raws:
        parts = split_ingredient(raw)
        if len(parts) > 1:
            ops.append({"type": "split", "from": raw, "to": parts})
        items.extend(parts)

    # --- gruppieren nach form-bewusstem Schlüssel (Reihenfolge: erstes
    # Vorkommen). dedup_key statt normalize, damit frisch/Dose/getrocknet
    # NICHT fälschlich zu einer Zutat gemerged werden. ---
    groups: "OrderedDict[str, list[str]]" = OrderedDict()
    for it in items:
        groups.setdefault(dedup_key(it), []).append(it)

    out: list[str] = []
    for members in groups.values():
        if len(members) == 1:
            out.append(members[0])
            continue

        # Jedes Mitglied klassifizieren nach einer "Signatur":
        #   ("dim", weight|volume|count)  numerisch, kaufbar (g/ml/Stück)
        #   ("vague", el|tl|prise|...)    vage Einheit
        #   ("plain",)                    Zahl ohne erkannte Einheit
        #                                 (z.B. "2 Knoblauchzehe(n)")
        #   -> bare                       gar keine Menge ("Salz", "evtl. X")
        # Gleiche Signatur = Mengen summierbar. bare ist redundant, sobald es
        # eine quantifizierte Variante gibt -> wird absorbiert (verworfen).
        sig_groups: "OrderedDict[tuple, list]" = OrderedDict()
        bare: list[str] = []
        for m in members:
            q = extract_ingredient_quantity(m)
            if q:
                sig_groups.setdefault(("dim", q["dimension"]), []).append(
                    (q["amount"], _strip_leading_quantity(m), q["dimension"]))
                continue
            vq = _vague_quantity(m)
            if vq:
                num, key, disp, name = vq
                sig_groups.setdefault(("vague", key), []).append((num, name, disp))
                continue
            lm = re.match(r"^\s*(\d+(?:[.,]\d+)?)\s+(\S.*)$", m)
            if lm:
                sig_groups.setdefault(("plain",), []).append(
                    (float(lm.group(1).replace(",", ".")), lm.group(2).strip(), None))
                continue
            bare.append(m)

        if not sig_groups:
            # alles ohne Menge -> ein Eintrag (z.B. "Salz" + "Salz")
            out.append(bare[0])
            ops.append({"type": "dedup", "from": members, "to": bare[0]})
            continue

        purchasable = [(s, l) for s, l in sig_groups.items()
                       if s[0] in ("dim", "plain")]
        vague_sigs = [(s, l) for s, l in sig_groups.items() if s[0] == "vague"]

        if purchasable:
            # kaufbare Mengen (g/ml/Stück) bestimmen das Ergebnis; vage
            # Einheiten + bare sind dagegen vernachlässigbar -> absorbiert.
            # Mehrere kaufbare Dimensionen (g + Stück) bleiben getrennt.
            pieces = [_render_sig(s, l) for s, l in purchasable]
            summed = any(len(l) > 1 for _, l in purchasable)
        elif len(vague_sigs) == 1:
            # eine vage Einheit (1 TL + 2 TL) -> Zahlen summieren
            s, l = vague_sigs[0]
            pieces = [_render_sig(s, l)]
            summed = len(l) > 1
        else:
            # verschiedene vage Einheiten (Prise + TL Salz): nicht numerisch
            # addierbar, aber DIESELBE Zutat -> auf EIN Vorkommen zusammenfassen,
            # sonst wird dasselbe Produkt doppelt gematcht.
            pieces = [next((m for m in members if _vague_quantity(m)), members[0])]
            summed = False

        out.extend(pieces)
        to = pieces[0] if len(pieces) == 1 else pieces
        if len(pieces) == len(members):
            # nichts zusammengefasst (nur echte Mehrfach-Dimensionen, g + Stück)
            ops.append({"type": "skip_ambiguous", "members": members})
        elif summed:
            ops.append({"type": "merge", "from": members, "to": to})
        else:
            ops.append({"type": "dedup", "from": members, "to": to})

    return out, ops
