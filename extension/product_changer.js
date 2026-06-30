// =============================================================================
//  REWE Mapping – Produkte automatisch auf die eigenen Angebots-Matches tauschen
// =============================================================================
//  Ablauf pro Rezept:
//    1. Alle Zutaten-Zeilen der Mapping-Seite einsammeln.
//    2. Pro Zeile das aktuell von REWE gewählte Produkt auslesen.
//    3. Mit der gewünschten Produkt-ID (eigenes Angebots-Match) vergleichen.
//    4. Nur bei Abweichung das Produkt über den Modify-Dialog austauschen.
//
//  Die Zeilen werden NACHEINANDER (await) abgearbeitet, weil der Modify-Dialog
//  global ist und immer nur für eine Zutat gleichzeitig offen sein kann.
// =============================================================================


// In eine IIFE gekapselt, damit wiederholtes Ausführen/Injizieren auf derselben
// Seite nicht an bereits deklarierten Konstanten scheitert.
(() => {

// ─── DOM-Helfer ───────────────────────────────────────────────────────────────

// Klick simulieren (React reagiert auf echte MouseEvents)
function simulateClick(element) {
    const event = new MouseEvent('click', { view: window, bubbles: true, cancelable: true });
    element.dispatchEvent(event);
}

// React-spezifische Eingabe-Erzwingung ins Suchfeld
function setValueForReweInput(inputElement, text) {
    inputElement.focus();
    const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value").set;
    nativeInputValueSetter.call(inputElement, text);

    inputElement.dispatchEvent(new Event('input', { bubbles: true }));
    inputElement.dispatchEvent(new Event('change', { bubbles: true }));

    inputElement.blur();
    inputElement.focus();

    const enterEvent = new KeyboardEvent('keydown', {
        key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true, cancelable: true
    });
    inputElement.dispatchEvent(enterEvent);
}

// Wartet, bis ein Element im DOM auftaucht
function waitForElement(selector, timeout = 5000) {
    return new Promise((resolve, reject) => {
        const startTime = Date.now();
        (function check() {
            const el = document.querySelector(selector);
            if (el) return resolve(el);
            if (Date.now() - startTime > timeout) return reject(new Error("Timeout für Selektor: " + selector));
            setTimeout(check, 100);
        })();
    });
}

// Wartet, bis ein Element wieder verschwindet (z. B. Modify-Dialog nach dem Übernehmen)
function waitForElementGone(selector, timeout = 5000) {
    return new Promise((resolve, reject) => {
        const startTime = Date.now();
        (function check() {
            if (!document.querySelector(selector)) return resolve();
            if (Date.now() - startTime > timeout) return reject(new Error("Timeout: Element bleibt sichtbar: " + selector));
            setTimeout(check, 100);
        })();
    });
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// Schließt einen offenen Modify-Dialog wieder. WICHTIG: Der Dialog ist global –
// bleibt er nach einem fehlgeschlagenen Tausch offen, sieht der nächste Tausch
// das alte Suchfeld und tippt ins Leere bzw. in die falsche Zutat. Darum nach
// JEDEM Tausch (auch bei Fehlern) sicherstellen, dass er geschlossen ist.
//
// REWE reagiert weder auf Escape noch auf einen Klick außerhalb – die Maske
// lässt sich NUR über den Zurück-Pfeil schließen
// (button.imr-SearchTile__productSearchInputBackButton).
async function ensureDialogClosed() {
    const SEARCH = 'input.imr-SearchTile__productSearchInput';
    const BACK   = 'button.imr-SearchTile__productSearchInputBackButton';
    if (!document.querySelector(SEARCH)) return;

    for (let i = 0; i < 4 && document.querySelector(SEARCH); i++) {
        const backBtn = document.querySelector(BACK);
        if (backBtn) {
            simulateClick(backBtn);
        } else {
            console.warn('Zurück-Pfeil nicht gefunden – Dialog kann nicht geschlossen werden.');
            break;
        }
        await waitForElementGone(SEARCH, 1500).catch(() => {});
    }
}


// ─── Zutaten-Zeilen der Mapping-Seite auslesen ────────────────────────────────

// Liefert für jede Zutat: das stabile ID-Klassen-Kennzeichen, den Zutatentext
// und die aktuell gewählte Produkt-ID. Zeilen ohne Produkt/Modify-Button (z. B.
// "Salz & Pfeffer") werden übersprungen.
// Liest Preis + Angebotsstatus aus einer Tile: { cents, onOffer } oder null.
// REWE zeigt auf der Mapping-Seite ENTWEDER einen Angebotspreis
// (a-pt__discount-price__price) ODER den regulären Preis
// (a-pt__regular-price__price), nie beide -- der reguläre Originalpreis eines
// rabattierten Produkts ist hier also NICHT auslesbar. Hash-Suffixe wechseln ->
// per [class*=...] matchen. onOffer = Discount-Element vorhanden; cents = der
// effektiv angezeigte Preis (das, was man zahlt).
function readTilePrice(tile) {
    if (!tile) return null;
    const offerEl = tile.querySelector('[class*="discount-price__price"]');
    const el = offerEl || tile.querySelector('[class*="regular-price__price"]');
    if (!el) return null;
    const onOffer = !!offerEl;
    const m = el.textContent.replace(/ |\s/g, "").match(/(\d+)[.,](\d{2})/);
    return m ? { cents: parseInt(m[1], 10) * 100 + parseInt(m[2], 10), onOffer } : null;
}

// Preis + Angebotsstatus der Kachel einer Zeile (per idClass) frisch aus dem DOM
// lesen -- nach dem Tausch zeigt sie das NEUE Produkt, so bekommen wir REWEs
// echten Live-Preis und ob dieses Produkt im Angebot ist.
function readRowPrice(idClass) {
    const item = document.querySelector('.' + idClass);
    if (!item) return null;
    return readTilePrice(item.querySelector('.imr-IngredientsListItem__tile .imr-Tile'));
}

function collectIngredientRows() {
    const rows = [];
    document.querySelectorAll('.imr-IngredientsListItem').forEach(item => {
        const modifyBtn = item.querySelector('.imr-ModifyButton');
        if (!modifyBtn) return; // keine austauschbare Zutat

        // Stabiles Kennzeichen wie "imr-IngredientsListItem__id-2-1" – damit wir
        // die Zeile nach einem Re-Render der Seite wiederfinden.
        const idClass = [...item.classList].find(c => c.startsWith('imr-IngredientsListItem__id-'));

        const termEl = item.querySelector('.imr-IngredientsListItem__searchTerm__Content');
        const searchTerm = termEl ? termEl.textContent.trim() : null;

        const tile = item.querySelector('.imr-IngredientsListItem__tile .imr-Tile');
        const currentProductId = tile ? tile.id : null;

        // Von REWE angezeigter Preis + Angebotsstatus des aktuell gewählten
        // Produkts, VOR dem Tausch ausgelesen (Basis für den Preisvergleich).
        const cur = readTilePrice(tile);
        const currentPrice   = cur ? cur.cents : null;
        const currentOnOffer = cur ? cur.onOffer : false;

        // Aktuelle Packungszahl (REWE startet bei 1)
        const amountInput = item.querySelector('.imr-AmountHandler__Amount');
        const currentAmount = amountInput ? (parseInt(amountInput.value, 10) || 1) : 1;

        rows.push({ idClass, searchTerm, currentProductId, currentAmount,
                    currentPrice, currentOnOffer });
    });
    return rows;
}


// ─── Ein einzelnes Produkt austauschen (dein erprobter Workflow) ──────────────

// Wartet, bis die Produktsuche ein Ergebnis liefert, und gibt den passenden
// "Artikel übernehmen"-Button zurück. Bevorzugt die Ergebnis-Kachel mit der
// gewünschten Produkt-ID; nur wenn die nicht auffindbar ist, das erste Ergebnis.
// Wirft, wenn nach `timeout` ms gar kein Ergebnis erscheint (z. B. Produkt nicht
// (mehr) im Sortiment) – dann KEIN Button blind klicken.
async function waitForSubmitButton(productId, timeout = 6000) {
    const BTN = 'button.imr-AlternativeProductListItemCallToAction__Button';
    const start = Date.now();
    while (Date.now() - start < timeout) {
        const tile = document.getElementById(String(productId));
        if (tile) {
            const container = tile.closest('.imr-AlternativeProductListItem') || tile.parentElement;
            const btn = container && container.querySelector(BTN);
            if (btn) return btn;
        }
        const fallback = document.querySelector(BTN);
        if (fallback) return fallback;
        await sleep(100);
    }
    throw new Error(`Kein Suchergebnis/Übernehmen-Button für Produkt ${productId} (Sortiment?).`);
}

async function swapProductForRow(idClass, desired) {
    // Niemals einen zweiten Dialog über einem hängengebliebenen öffnen.
    await ensureDialogClosed();

    // Zeile frisch aus dem DOM holen (kann nach vorherigem Tausch neu gerendert sein)
    const item = document.querySelector('.' + idClass);
    if (!item) throw new Error(`Zutaten-Zeile ${idClass} nicht gefunden.`);

    const modifyBtn = item.querySelector('.imr-ModifyButton');
    if (!modifyBtn) throw new Error(`Kein Modify-Button in ${idClass}.`);

    // 1. Modify-Dialog öffnen
    simulateClick(modifyBtn);

    try {
        // 2. Auf das Suchfeld warten
        const searchInput = await waitForElement('input.imr-SearchTile__productSearchInput');

        // 3. Gewünschte Produkt-ID eintragen und Suche auslösen (zuverlässigste Auswahl)
        setValueForReweInput(searchInput, String(desired.productId));

        // 4. Auf das passende Suchergebnis warten und "Artikel übernehmen" klicken
        const submitBtn = await waitForSubmitButton(desired.productId);
        simulateClick(submitBtn);

        // 5. Warten, bis der Dialog geschlossen ist, bevor die nächste Zutat drankommt
        await waitForElementGone('input.imr-SearchTile__productSearchInput').catch(() => {});
        await sleep(300); // kleine Puffer-Pause für das Re-Render der Kachel
    } finally {
        // Egal ob erfolgreich oder Fehler: Dialog garantiert schließen, sonst
        // vergiftet er alle folgenden Zutaten.
        await ensureDialogClosed();
    }
}

// Setzt den angezeigten Zutaten-Titel zurück auf den ursprünglichen Zutatentext.
// Nach der ID-Suche steht dort sonst die ID. Achtung: rein kosmetisch – React
// kann den Text bei einem erneuten Re-Render der Zeile wieder überschreiben.
function restoreTitle(idClass, originalText) {
    const item = document.querySelector('.' + idClass);
    if (!item) return;
    const el = item.querySelector('.imr-IngredientsListItem__searchTerm__Content');
    if (el) el.textContent = originalText;
}


// ─── Anzahl der Packungen eines Produkts setzen ───────────────────────────────
//
//  Ist das gewählte Produkt kleiner als die benötigte Menge, braucht das Rezept
//  mehrere Packungen (package_info.packages_needed > 1). REWE startet bei 1; wir
//  klicken den Plus-/Minus-Button so oft, bis der Zähler den Zielwert erreicht.
//  Wir lesen nach jedem Klick den echten Input-Wert neu, statt blind zu zählen –
//  so kann das Re-Render uns nicht aus dem Tritt bringen.
async function setAmountForRow(idClass, targetAmount) {
    const target = Math.max(1, parseInt(targetAmount, 10) || 1);

    for (let guard = 0; guard < 30; guard++) {
        const item = document.querySelector('.' + idClass);
        if (!item) throw new Error(`Zutaten-Zeile ${idClass} nicht gefunden.`);

        const input = item.querySelector('.imr-AmountHandler__Amount');
        const current = input ? (parseInt(input.value, 10) || 1) : 1;
        if (current === target) return;

        const btn = current < target
            ? item.querySelector('.imr-AmountHandler__ButtonIncrease')
            : item.querySelector('.imr-AmountHandler__ButtonDecrease');
        if (!btn) throw new Error(`Mengen-Button (${current}→${target}) in ${idClass} nicht gefunden.`);

        simulateClick(btn);
        await sleep(250); // auf das Re-Render des Zählers warten
    }
    console.warn(`Mengenanpassung in ${idClass} nach 30 Schritten abgebrochen.`);
}


// ─── Gewünschte Produkte von GitHub Pages laden ───────────────────────────────
//
//  Zwei Modi, je eine JSON-Datei (nach Mapping-Hash indexiert = letztes
//  Pfadsegment der URL). Pro Rezept eine Zutatenliste mit Produkt + benötigter
//  Packungszahl. Wird je Lauf einmal geladen und nach Zutatennamen gematcht.
//    cheapest = günstigstes eigenes Match (Standard)
//    offers   = Angebots-Match (Datei evtl. noch nicht vorhanden → sauberer
//               Hinweis im Panel statt stillem Abbruch)
const BASE_URL = "https://bigdatalex.github.io/rewe_products/data";
const DATA_URLS = {
    cheapest: `${BASE_URL}/cheapest_mappings.json`,
    offers:   `${BASE_URL}/offer_mappings.json`,
};
const MODE_LABELS = { cheapest: "Günstigste", offers: "Angebote" };
let currentMode = "cheapest";

// Führende Menge + Einheit aus dem Zutatentext entfernen.
const INGREDIENT_UNITS = new Set([
    "g", "kg", "mg", "ml", "l", "cl", "el", "tl", "msp", "stk", "stück", "stueck",
    "prise", "prisen", "dose", "dosen", "packung", "pck", "pkg", "bund", "tasse",
    "tassen", "becher", "glas", "scheibe", "scheiben", "zehe", "zehen", "kopf",
    "liter", "gramm", "blatt", "blätter",
]);

function ingredientName(raw) {
    let s = (raw || "").trim().toLowerCase();
    s = s.replace(/^[\d.,/\s½¼¾⅓⅔⅛–-]+/, "");   // führende Zahlen/Brüche weg
    let parts = s.split(/\s+/);
    if (parts.length && INGREDIENT_UNITS.has(parts[0])) parts = parts.slice(1); // Einheit weg
    s = parts.join(" ").split(",")[0];           // Zusätze nach Komma weg
    return s.replace(/\s+/g, " ").trim();
}

// ── Fuzzy-Matching ────────────────────────────────────────────────────────────
// Chefkoch und REWE formatieren Zutaten unterschiedlich (z.B. "Aubergine(n)" vs
// "Auberginen", "m.-große" vs "mittelgroße"). Darum vergleichen wir nicht den
// vollen Text, sondern Mengen von gestemmten Inhalts-Tokens und nehmen pro
// REWE-Zeile den ähnlichsten Eintrag aus unseren Daten.
const MATCH_STOP = new Set([
    "und", "oder", "mit", "im", "in", "aus", "dem", "den", "der", "die", "das",
    "zum", "zur", "von", "je", "ca", "etwa", "etwas", "evtl", "nach", "ggf",
    "ein", "eine", "einer", "einem", "sowie", "bzw", "a",
]);
// gestemmte Füll-Adjektive (Größe/Zubereitung/Farbe) – fürs Matching irrelevant
const MATCH_STOP_STEM = new Set([
    "groß", "klein", "mittelgroß", "mittel", "frisch", "getrocknet", "gehackt",
    "gerieben", "gemahl", "gewürfelt", "gekocht", "roh", "reif", "bio", "jung",
    "gelb", "rot", "grün", "weiß", "braun", "schwarz",
]);

// grobes deutsches Plural-/Flexions-Stemming (symmetrisch für Singular/Plural)
function stemToken(t) {
    return t.replace(/(en|er|n|e|s)$/, "");
}

function tokenize(name) {
    const set = new Set();
    for (let w of (name || "").toLowerCase().split(/[^a-zäöüß]+/)) {
        if (!w || w.length < 2 || MATCH_STOP.has(w)) continue;
        w = stemToken(w);
        if (w.length >= 2 && !MATCH_STOP_STEM.has(w)) set.add(w);
    }
    return set;
}

function matchScore(a, b) {
    if (!a.size || !b.size) return { overlap: 0, jaccard: 0 };
    let inter = 0;
    for (const t of a) if (b.has(t)) inter++;
    return { overlap: inter / Math.min(a.size, b.size), jaccard: inter / (a.size + b.size - inter) };
}

const MATCH_THRESHOLD = 0.6;

// Liste der gewünschten Produkte des Rezepts (mit vorberechneten Tokens).
let DESIRED_LIST = [];

async function loadDesiredProducts(mode) {
    const url = DATA_URLS[mode] || DATA_URLS.cheapest;
    const hash = location.pathname.split("/").filter(Boolean).pop();

    let res;
    try {
        res = await fetch(url, { cache: "no-cache" });
    } catch (e) {
        throw new Error(`„${MODE_LABELS[mode]}“-Daten nicht erreichbar: ${e.message}`);
    }
    // Häufigster Fall für "Angebote": Datei gibt es noch nicht → klarer Hinweis.
    if (res.status === 404) {
        throw new Error(`Für „${MODE_LABELS[mode]}“ gibt es noch keine Daten (Datei fehlt).`);
    }
    if (!res.ok) throw new Error(`Daten nicht ladbar (HTTP ${res.status}).`);

    const all = await res.json();
    const entry = all[hash];
    if (!entry) throw new Error(`Kein Mapping-Eintrag für dieses Rezept (Hash ${hash}).`);

    const list = entry.ingredients.map(i => ({
        name: i.name,
        productId: String(i.product_id),
        amount: i.amount || 1,
        tokens: tokenize(i.name),
    }));
    console.log(`Geladen [${mode}]: "${entry.recipe_name}" – ${list.length} Zutaten.`);
    return { list, recipeName: entry.recipe_name || "" };
}

// Beste Übereinstimmung für einen REWE-searchTerm: höchster Overlap, bei
// Gleichstand höchster Jaccard (bestraft überflüssige Tokens → exakter Treffer).
function getDesiredFor(searchTerm) {
    const q = tokenize(ingredientName(searchTerm));
    if (!q.size) return null;

    let best = null, bestOverlap = 0, bestJaccard = 0;
    for (const cand of DESIRED_LIST) {
        const { overlap, jaccard } = matchScore(q, cand.tokens);
        if (overlap > bestOverlap || (overlap === bestOverlap && jaccard > bestJaccard)) {
            best = cand; bestOverlap = overlap; bestJaccard = jaccard;
        }
    }
    if (bestOverlap < MATCH_THRESHOLD) return null;
    if (bestOverlap < 1) {
        console.debug(`fuzzy: "${searchTerm}" → "${best.name}" (overlap ${bestOverlap.toFixed(2)})`);
    }
    return best;
}


// ─── Hauptablauf (mit Live-Statistik fürs Panel) ──────────────────────────────

// Führt den Tausch durch und meldet Fortschritt/Zahlen an die UI-Hooks.
// `ui` kapselt die Panel-Updates (setStatus/setCounts/setProgress) -- so bleibt
// die Tauschlogik von der Darstellung getrennt und auch headless testbar.
async function runWorkflow(mode, ui) {
    ui.reset();
    ui.setStatus(`Lade „${MODE_LABELS[mode]}“-Daten …`);

    let loaded;
    try {
        loaded = await loadDesiredProducts(mode);
    } catch (err) {
        console.error("Abbruch – Produktdaten:", err.message);
        ui.setStatus(err.message, "error");
        return;
    }
    DESIRED_LIST = loaded.list;

    const rows = collectIngredientRows();
    ui.setTotal(rows.length);
    ui.setStatus(`${loaded.recipeName || "Rezept"} · ${rows.length} Zutaten`);
    console.log(`Gefundene austauschbare Zutaten: ${rows.length}`);

    let changed = 0, skipped = 0, missing = 0, failed = 0, done = 0;
    let offers = 0, savingCents = 0;

    // Buchung für eine Zeile, die am Ende UNSER Produkt trägt (getauscht oder
    // schon korrekt). Preis UND Angebotsstatus kommen ausschließlich von der
    // REWE-Seite (`ours` = { cents, onOffer } aus der Kachel) -- KEIN JSON-
    // Fallback. reweKosten = REWE-Preis × REWE-Menge (vor dem Tausch gelesen),
    // unsereKosten = unser Kachelpreis × Zielmenge. Differenz = Ersparnis (Cent).
    // Ist der Preis nicht lesbar (ours == null), trägt die Zeile nichts bei.
    const account = (row, amount, ours) => {
        if (!ours) return;
        if (ours.onOffer) offers++;
        if (row.currentPrice != null) {
            const reweCost = row.currentPrice * (row.currentAmount || 1);
            const ourCost  = ours.cents * (amount || 1);
            savingCents += reweCost - ourCost;
        }
    };

    for (const row of rows) {
        // Sicherheitsnetz: vor jeder Zutat darf kein Dialog mehr offen sein.
        await ensureDialogClosed();

        const desired = getDesiredFor(row.searchTerm);

        if (!desired) {
            console.log(`– "${row.searchTerm}": kein eigenes Match hinterlegt → übersprungen`);
            missing++;
        } else {
            const targetAmount = desired.amount || 1;
            const needSwap   = String(row.currentProductId) !== String(desired.productId);
            const needAmount = (row.currentAmount || 1) !== targetAmount;

            if (!needSwap && !needAmount) {
                console.log(`✓ "${row.searchTerm}": bereits korrekt (${desired.productId} ×${targetAmount})`);
                skipped++;
                // Kachel zeigt bereits unser Produkt -> currentPrice/onOffer IST unseres.
                account(row, targetAmount,
                        { cents: row.currentPrice, onOffer: row.currentOnOffer });
            } else {
                try {
                    if (needSwap) {
                        console.log(`↻ "${row.searchTerm}": ${row.currentProductId} → ${desired.productId} …`);
                        await swapProductForRow(row.idClass, desired);
                    }
                    // Menge IMMER nach dem (möglichen) Tausch setzen: nach einem
                    // Tausch steht der Zähler wieder auf 1.
                    await setAmountForRow(row.idClass, targetAmount);
                    // Titel zurück auf den Zutatentext (nur nach Tausch nötig).
                    if (needSwap) restoreTitle(row.idClass, row.searchTerm);
                    changed++;
                    // Preis + Angebotsstatus des frisch getauschten Produkts von der
                    // Kachel lesen (rein seiten-basiert), dann Ersparnis buchen.
                    account(row, targetAmount, readRowPrice(row.idClass));
                    console.log(`  ✔ ${desired.productId} ×${targetAmount} gesetzt.`);
                } catch (err) {
                    console.error(`  ✘ Fehler bei "${row.searchTerm}":`, err.message);
                    failed++;  // z.B. Produkt nicht (mehr) im Sortiment
                }
            }
        }

        done++;
        ui.setCounts({ changed, skipped, missing, failed });
        ui.setSummary({ offers, savingCents });
        ui.setProgress(done, rows.length);
    }

    const parts = [`${changed} getauscht`, `${skipped} schon korrekt`, `${missing} ohne Match`];
    if (failed) parts.push(`${failed} fehlgeschlagen`);
    ui.setStatus("Fertig · " + parts.join(" · "), "done");
    console.log("Fertig. " + parts.join(", ") + `. ${offers} im Angebot, Ersparnis ${(savingCents/100).toFixed(2)} €.`);
}


// ─── Panel-UI (Shadow-DOM, damit REWE-CSS nicht reinfunkt) ────────────────────

const PANEL_ID = "rro-panel-host";

function mountPanel() {
    // Re-Injektion (erneuter Toolbar-Klick): vorhandenes Panel einfach behalten.
    if (document.getElementById(PANEL_ID)) return;

    const host = document.createElement("div");
    host.id = PANEL_ID;
    // Positionierung am Host; die Stil-Isolation nach innen macht :host{all:initial}
    // im Shadow-CSS. (KEIN all:initial hier – das würde position:fixed zurücksetzen.)
    host.style.cssText =
        "position:fixed;top:16px;right:16px;z-index:2147483647;";
    const root = host.attachShadow({ mode: "open" });
    root.innerHTML = `
      <style>
        :host { all: initial; }
        * { box-sizing: border-box; font-family: system-ui, "Segoe UI", Arial, sans-serif; }
        .panel { width: 280px; background:#fff; color:#1a1a1a;
                 border-radius:12px; box-shadow:0 8px 28px rgba(0,0,0,.25);
                 overflow:hidden; border:1px solid #e5e5e5; }
        .head { display:flex; align-items:center; justify-content:space-between;
                background:#cc071e; color:#fff; padding:10px 12px; }
        .head h1 { font-size:14px; font-weight:600; margin:0; }
        .close { cursor:pointer; background:none; border:none; color:#fff;
                 font-size:18px; line-height:1; padding:0 2px; }
        .body { padding:12px; }
        .seg { display:flex; background:#f0f0f0; border-radius:8px; padding:3px;
               margin-bottom:10px; }
        .seg button { flex:1; border:none; background:none; padding:7px 0;
                      font-size:12px; font-weight:600; color:#555; cursor:pointer;
                      border-radius:6px; }
        .seg button.active { background:#fff; color:#cc071e;
                             box-shadow:0 1px 3px rgba(0,0,0,.15); }
        .run { width:100%; border:none; background:#cc071e; color:#fff;
               font-size:13px; font-weight:600; padding:10px; border-radius:8px;
               cursor:pointer; margin-bottom:10px; }
        .run:disabled { opacity:.55; cursor:default; }
        .stats { display:grid; grid-template-columns:1fr 1fr 1fr; gap:8px; }
        .stats.has-failed { grid-template-columns:1fr 1fr 1fr 1fr; }
        .card { background:#f7f7f7; border-radius:8px; padding:8px 4px; text-align:center; }
        .card .n { font-size:20px; font-weight:700; line-height:1.1; }
        .card .l { font-size:10px; color:#666; margin-top:2px; }
        .card.changed .n { color:#1a7f37; }
        .card.skipped .n { color:#0969da; }
        .card.missing .n { color:#9a6700; }
        .card.failed  .n { color:#cf222e; }
        .card.failed  { display:none; }
        .stats.has-failed .card.failed { display:block; }
        .bar { height:6px; background:#eee; border-radius:3px; margin-top:10px; overflow:hidden; }
        .bar > i { display:block; height:100%; width:0; background:#cc071e; transition:width .2s; }
        .summary { display:flex; gap:8px; margin-top:10px; }
        .summary .pill { flex:1; background:#f7f7f7; border-radius:8px;
                         padding:7px 8px; font-size:11px; color:#333;
                         display:flex; align-items:center; gap:4px; white-space:nowrap; }
        .summary .pill b { font-weight:700; }
        .summary .save b { color:#1a7f37; }
        .summary .save.neg b { color:#cf222e; }
        .status { font-size:11px; color:#555; margin-top:8px; min-height:14px;
                  line-height:1.3; }
        .status.error { color:#cf222e; }
        .status.done  { color:#1a7f37; font-weight:600; }
      </style>
      <div class="panel">
        <div class="head">
          <h1>🛒 Rezept-Optimierer</h1>
          <button class="close" title="Schließen">×</button>
        </div>
        <div class="body">
          <div class="seg">
            <button data-mode="cheapest" class="active">Günstigste</button>
            <button data-mode="offers">Angebote</button>
          </div>
          <button class="run">Produkte tauschen</button>
          <div class="stats">
            <div class="card changed"><div class="n" data-c="changed">0</div><div class="l">getauscht</div></div>
            <div class="card skipped"><div class="n" data-c="skipped">0</div><div class="l">schon korrekt</div></div>
            <div class="card missing"><div class="n" data-c="missing">0</div><div class="l">ohne Match</div></div>
            <div class="card failed"><div class="n" data-c="failed">0</div><div class="l">fehlgeschl.</div></div>
          </div>
          <div class="bar"><i></i></div>
          <div class="summary">
            <div class="pill offer">🏷 <b data-s="offers">0</b>&nbsp;im Angebot</div>
            <div class="pill save">💰 <b data-s="saving">0,00&nbsp;€</b>&nbsp;<span data-s="savelabel">gespart</span></div>
          </div>
          <div class="status"></div>
        </div>
      </div>`;
    document.body.appendChild(host);

    const $ = (sel) => root.querySelector(sel);
    const statsEl  = $(".stats");
    const statusEl = $(".status");
    const barEl    = $(".bar > i");
    const runBtn   = $(".run");
    const savePill = $(".summary .save");
    const setN = (k, v) => { const el = root.querySelector(`[data-c="${k}"]`); if (el) el.textContent = v; };
    const setS = (k, v) => { const el = root.querySelector(`[data-s="${k}"]`); if (el) el.textContent = v; };
    const euro = (cents) => (Math.abs(cents) / 100).toFixed(2).replace(".", ",") + " €";

    // UI-Hooks, die runWorkflow() ansteuert.
    const ui = {
        reset() {
            ["changed", "skipped", "missing", "failed"].forEach(k => setN(k, 0));
            statsEl.classList.remove("has-failed");
            setS("offers", 0); setS("saving", "0,00 €"); setS("savelabel", "gespart");
            savePill.classList.remove("neg");
            barEl.style.width = "0%";
            statusEl.className = "status";
        },
        setStatus(msg, kind) { statusEl.textContent = msg; statusEl.className = "status" + (kind ? " " + kind : ""); },
        setCounts({ changed, skipped, missing, failed }) {
            setN("changed", changed); setN("skipped", skipped);
            setN("missing", missing); setN("failed", failed);
            statsEl.classList.toggle("has-failed", failed > 0);
        },
        setSummary({ offers, savingCents }) {
            setS("offers", offers);
            setS("saving", euro(savingCents));
            setS("savelabel", savingCents < 0 ? "teurer" : "gespart");
            savePill.classList.toggle("neg", savingCents < 0);
        },
        setTotal() {},
        setProgress(done, total) {
            barEl.style.width = total ? `${Math.round((done / total) * 100)}%` : "0%";
        },
    };

    // Modus-Umschalter
    root.querySelectorAll(".seg button").forEach(btn => {
        btn.addEventListener("click", () => {
            currentMode = btn.dataset.mode;
            root.querySelectorAll(".seg button").forEach(b =>
                b.classList.toggle("active", b === btn));
            ui.setStatus(`Modus: ${MODE_LABELS[currentMode]}`);
        });
    });

    // Start
    runBtn.addEventListener("click", async () => {
        runBtn.disabled = true;
        const old = runBtn.textContent;
        runBtn.textContent = "Läuft …";
        try {
            await runWorkflow(currentMode, ui);
        } finally {
            runBtn.disabled = false;
            runBtn.textContent = old;
        }
    });

    $(".close").addEventListener("click", () => host.remove());
}

// Panel anzeigen (statt sofort loszulaufen – der Nutzer wählt Modus & startet).
mountPanel();

})();
