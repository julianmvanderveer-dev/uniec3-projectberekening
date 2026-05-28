"""
Uniec3 merge-logica: voegt losse woningberekeningen samen tot één projectberekening.

Deduplicatiestrategie:
- LIB*-entiteiten (bouwkundige bibliotheek): gededupliceerd op inhoud (UUID-vrij).
- RZ (rekenzones): gededupliceerd op inhoud — identieke woningtypen delen 1 RZ.
- Systeem-niveau VERW/TAPW/KOEL: gededupliceerd op inhoud.
  Per-woning entiteiten (VERW-OPWEK, UNIT-VERW, etc.) blijven per woning.
- RESULT-*: uitgesloten (herberekend door Uniec3 zelf).
- UNIT / UNIT-* / BEGR etc.: per-woning, meegenomen van alle kavels.
- Overige singletons (RZFORM etc.): uitsluitend van het eerste kavel.
"""

import zipfile
import json
import io
import time
import re
import hashlib
from collections import Counter
from datetime import datetime

# ── UUID-patroon ───────────────────────────────────────────────────────────────
_UUID_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    re.IGNORECASE,
)

# ── Patroon voor berekende resultaatwaarden (high-precision floats) ────────────
# Getallen met 3+ decimalen zijn vrijwel altijd berekeningsresultaten (bijv.
# TAPW-OPWEK_BEHOEFTE = 2092,8618267521374). Ze worden uitgesloten van de
# content-hash zodat identieke systemen correct als duplicaat worden herkend.
_CALC_FLOAT_RE = re.compile(r'^-?\d+[,.]\d{3,}$')

# ── Bekende resultaatproperties die NIET op '_NON' eindigen ───────────────────
# Sommige berekeningsresultaten worden door Uniec3 opgeslagen zonder '_NON'-suffix.
# Kavel[0] slaat ze soms als integer op (bijv. 1907), latere kavels als float
# (bijv. 1907,3543...). Uitsluiting op naam voorkomt valse hash-verschillen.
_CALC_PROPS: frozenset[str] = frozenset({
    "TAPW-OPWEK_BEHOEFTE",   # tapwater energiebehoefte (berekend)
    "TAPW-OPWEK_GELEV",      # geleverde tapwaterenergie (berekend)
})

# ── Categorieën ────────────────────────────────────────────────────────────────

# Berekeningsresultaten: niet overnemen (Uniec3 herberekent ze).
RESULT_PREFIXES = ("RESULT-",)
RESULT_EXACT:    frozenset[str] = frozenset({"PRESTATIE"})  # per-woning resultaat

# Afmeld-entiteiten: NIET overnemen in merged output.
# Als een bronbestand formeel is afgemeld (ingediend voor energielabel), bevatten
# deze entiteiten een vergrendelde "original"-staat. Wordt die meegenomen in het
# merged output, dan geeft Uniec3 InvalidDataException bij herberekening:
# "[InvalidDataException] while initializing CalculationHandler, original."
AFMELD_PREFIXES = ("AFMELD",)

# Entiteiten die content-hash dedup krijgen (UUID-vrij hash → 1 canonical per unieke inhoud).
# LIB*    = bouwkundige bibliotheek (LIBCONSTRL etc.)
# CONSTR* = bouwkundige constructie-entiteiten (CONSTRL, CONSTRD, CONSTRT, …)
#           Waren aanwezig t/m commit a3214d2, per ongeluk verdwenen bij herschrijving b7309bc.
# RZ      = rekenzone profiel (gedeeld per woningtype)
# Systeem-niveau VERW/TAPW/KOEL
LIB_EXACT: frozenset[str] = frozenset({
    # Bouwkundige bibliotheek
    "LIBCONSTRD", "LIBCONSTRT", "LIBCONSTRL", "LIBCONSTRFORM",
    # Bouwkundige constructie-entiteiten (beschrijven de opbouw van bouwdelen;
    # mogen gedeeld worden tussen woningen met dezelfde constructie).
    # LET OP: BEGR (begrenzingsvlak) staat hier NIET — BEGR is kind van UNIT-RZ
    #         en moet per woning uniek blijven, anders verdwijnen woningen in Uniec3.
    # LET OP: INFILUNIT staat hier NIET — geen ouderrelatie gevonden, per woning.
    "CONSTRL", "CONSTRD", "CONSTRT",
    "BELEMMERING",      # ouder = VERW-OPWEK (gedeeld systeem)
    "CONSTRKRVENT", "CONSTRZOMNAC",
    "CONSTRKENMV", "CONSTRKENMW",
    "CONSTRWG", "CONSTRWWGVL", "CONSTRWWKLDR",
    "CONSTRERROR",
    "PV-VELD",          # ouder = PV (gedeeld systeem)
    # Rekenzone profiel
    "RZ",
    # Installatie – systeem-niveau verwarming
    "VERW", "VERW-AFG", "VERW-AFG-VENT",
    "VERW-DISTR", "VERW-DISTR-BUI", "VERW-DISTR-EIG", "VERW-DISTR-POMP", "VERW-VAT",
    # Installatie – systeem-niveau warm tapwater
    "TAPW", "TAPW-AFG", "TAPW-DISTR", "TAPW-VAT",
    "TAPW-DISTR-BUI", "TAPW-DISTR-EIG", "TAPW-DISTR-POMP",
    "TAPW-DOUCHE", "TAPW-DOUCHE-AANG",
    # TAPW-UNIT staat NIET hier — is per-woning junction (zie MULTI_EXACT)
    # Opwekkers: dedup op inhoud zodat identieke systemen gedeeld worden
    "VERW-OPWEK", "TAPW-OPWEK", "KOEL-OPWEK",
    # Installatie – systeem-niveau koeling
    "KOEL", "KOEL-AFG", "KOEL-AFG-VENT",
    "KOEL-DISTR", "KOEL-DISTR-BUI", "KOEL-DISTR-EIG", "KOEL-DISTR-POMP",
    # Installatie – INSTALLATIE (koppelentiteit; dedup op naam+type)
    "INSTALLATIE",
})

# Per-woning junction-entiteiten: altijd multi (1 per woning, ook al zijn ze inhoudelijk identiek).
MULTI_EXACT: frozenset[str] = frozenset({
    "TAPW-UNIT",   # TAPW ↔ UNIT koppeling, 1 per woning
})

# Per-woning-entiteiten via prefix: altijd multi (van alle kavels).
MULTI_PREFIXES = ("UNIT",)


def _is_result(eid: str) -> bool:
    return (eid in RESULT_EXACT
            or any(eid.startswith(p) for p in RESULT_PREFIXES)
            or any(eid.startswith(p) for p in AFMELD_PREFIXES))


def _is_lib(eid: str) -> bool:
    return eid in LIB_EXACT or eid.startswith("LIB")


def _is_forced_multi(eid: str) -> bool:
    if eid in MULTI_EXACT:
        return True
    return any(eid.startswith(p) for p in MULTI_PREFIXES)


def _content_key(e: dict) -> str:
    """Hash van entity-inhoud op basis van invoerparameters.
    Uitgesloten:
    - UUID-waarden  (verwijzingen naar andere entiteiten)
    - Properties die eindigen op '_NON'  (berekende resultaatwaarden die per
      woning kunnen verschillen maar geen deel uitmaken van de systeemkeuze)
    - High-precision floats (3+ decimalen, bijv. TAPW-OPWEK_BEHOEFTE = 2092,86...)
      Dit zijn berekeningsresultaten die niet op '_NON' eindigen maar toch per
      woning verschillen en geen systeemkeuze representeren.
    Zo worden twee entiteiten met identieke invoer maar verschillende berekenings-
    resultaten correct als duplicaat herkend."""
    parts = [e.get("NTAEntityId", "")]
    for p in sorted(e.get("NTAPropertyDatas", []), key=lambda x: x.get("NTAPropertyId", "")):
        prop_id = p.get("NTAPropertyId", "")
        if prop_id.endswith("_NON"):
            continue   # sla berekende resultaatwaarden over
        if prop_id in _CALC_PROPS:
            continue   # sla bekende resultaatproperties over (ook zonder _NON-suffix)
        val = str(p.get("Value", ""))
        if _UUID_RE.match(val):
            continue   # sla ID-referenties over
        if _CALC_FLOAT_RE.match(val):
            continue   # sla berekeningsresultaten met hoge precisie over
        parts.append(f"{prop_id}={val}")
    return hashlib.md5("|".join(parts).encode()).hexdigest()


def _remap_entity(e: dict, remap: dict) -> dict:
    """Pas id_remap toe op property-waarden van een entiteit.
    De eigen NTAEntityDataId wordt NIET aangeraakt — alleen verwijzingen
    in NTAPropertyDatas.Value naar andere entiteiten worden bijgewerkt."""
    if not remap:
        return e
    new_props = []
    for p in e.get("NTAPropertyDatas", []):
        val = p.get("Value", "")
        if isinstance(val, str) and val in remap:
            p = dict(p)
            p["Value"] = remap[val]
        new_props.append(p)
    return dict(e, NTAPropertyDatas=new_props)


def _remap_relation(r: dict, remap: dict) -> dict:
    """Pas id_remap toe op ParentId en ChildId van een relatie."""
    if not remap:
        return r
    r = dict(r)
    if r.get("ParentId") in remap:
        r["ParentId"] = remap[r["ParentId"]]
    if r.get("ChildId") in remap:
        r["ChildId"] = remap[r["ChildId"]]
    # Composite sleutel bijwerken
    if "NTAEntityRelationDataId" in r:
        r["NTAEntityRelationDataId"] = f"{r['ParentId']}:{r['ChildId']}"
    return r


# ── ZIP-helpers ────────────────────────────────────────────────────────────────

def read_json_from_zip(zf, name):
    with zf.open(name) as f:
        raw = f.read()
    return json.loads(raw.decode("utf-8-sig"))


def encode_json(obj):
    return ("\ufeff" + json.dumps(obj, ensure_ascii=False, separators=(",", ":"))).encode("utf-8")


# ── Merge ──────────────────────────────────────────────────────────────────────

def merge_uniec3(file_objects):
    """
    Voegt meerdere .uniec3 bestanden samen tot één projectberekening.
    Geeft terug: (bytes van .uniec3, aantal woningen)
    """
    kavels = []

    for fo in file_objects:
        with zipfile.ZipFile(fo, "r") as zf:
            names     = zf.namelist()
            meta      = read_json_from_zip(zf, "meta.json")
            folders   = read_json_from_zip(zf, "folders.json")
            projects  = read_json_from_zip(zf, "projects.json")
            buildings = read_json_from_zip(zf, "buildings.json")
            if isinstance(buildings, dict):
                buildings = [buildings]

            for building in buildings:
                bid    = building["BuildingId"]
                prefix = f"buildings/{bid}/"

                def get(suffix, _p=prefix):
                    n = next((x for x in names if x.startswith(_p) and x.endswith(suffix)), None)
                    return read_json_from_zip(zf, n) if n else []

                entities  = get("entities.json")
                relations = get("relations.json")
                deltas    = get("deltas.json")
                summary   = get("summary.json") or {}

                kavels.append({
                    "meta": meta, "folders": folders, "projects": projects,
                    "building": building, "bid": bid,
                    "entities":  entities  if isinstance(entities, list)  else [],
                    "relations": relations if isinstance(relations, list) else [],
                    "deltas":    deltas    if isinstance(deltas, list)    else [],
                    "summary":   summary   if isinstance(summary, dict)   else {},
                })

    if not kavels:
        raise ValueError("Geen woningberekeningen gevonden in de aangeleverde bestanden.")

    # ── Nieuw project-BuildingId ──────────────────────────────────────────────
    new_bid  = int(time.time())
    first    = kavels[0]
    now_iso  = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.0000000")

    proj_building = dict(first["building"])
    proj_building["BuildingId"] = new_bid
    proj_building["ChangeDate"] = now_iso

    # ── Stap 1: Bouwkundige bibliotheek dedupliceren + ID-remap opbouwen ────────
    # Voor elk LIB*-type: bij dubbele inhoud → canonical ID bewaren,
    # duplicaat-ID opnemen in id_remap zodat verwijzingen daarnaar worden
    # bijgewerkt naar het canonical exemplaar.
    # TAPW/VERW/KOEL zijn PER WONING en worden NIET hier verwerkt.
    lib_content_seen: dict[str, str] = {}   # content_hash → canonical NTAEntityDataId
    id_remap:         dict[str, str] = {}   # duplicate_id → canonical_id
    deduped_lib:      list           = []   # unieke LIB*-entiteiten

    for k in kavels:
        for e in k["entities"]:
            eid = e.get("NTAEntityId", "")
            if not _is_lib(eid):
                continue
            if _is_result(eid):
                continue
            old_id = e.get("NTAEntityDataId", "")

            ck = _content_key(e)
            if ck in lib_content_seen:
                canonical_id = lib_content_seen[ck]
                if old_id and old_id != canonical_id:
                    id_remap[old_id] = canonical_id
            else:
                lib_content_seen[ck] = old_id
                entry = dict(e)
                entry["BuildingId"] = new_bid
                deduped_lib.append(entry)

    # ── Stap 2: Singleton vs. multi bepalen (excl. LIB*) ─────────────────────
    type_counts = [Counter(e["NTAEntityId"] for e in k["entities"]) for k in kavels]

    def is_multi(eid: str) -> bool:
        if _is_forced_multi(eid):
            return True
        return any(c.get(eid, 0) > 1 for c in type_counts)

    # ── Stap 3: Overige entiteiten samenvoegen ────────────────────────────────
    # singleton_canonical: eid → canonical NTAEntityDataId (kavel[0]-exemplaar)
    # Latere kavels' singleton-IDs worden via id_remap naar canonical omgezet
    # zodat relaties van multi-entiteiten (bijv. INSTALLATIE→VENT) niet breken.
    other_entities      = []
    seen_entity_ids     = set()
    seen_singletons     = set()
    singleton_canonical: dict[str, str] = {}

    for kavel_idx, k in enumerate(kavels):
        is_first = (kavel_idx == 0)

        for e in k["entities"]:
            eid = e.get("NTAEntityId", "")

            # LIB_EXACT + LIB* al verwerkt in stap 1
            if _is_lib(eid):
                continue

            # Berekeningsresultaten overslaan
            if _is_result(eid):
                continue

            # Globale ID-dedup
            entity_id = e.get("NTAEntityDataId", "")
            if entity_id:
                if entity_id in seen_entity_ids:
                    continue
                seen_entity_ids.add(entity_id)

            # Singletons: alleen eerste kavel bewaren.
            # Latere kavels: ID toevoegen aan id_remap → canonical, zodat
            # verwijzingen vanuit multi-entiteiten correct worden bijgewerkt.
            if not is_multi(eid):
                if eid in seen_singletons:
                    if entity_id and eid in singleton_canonical:
                        id_remap[entity_id] = singleton_canonical[eid]
                    continue
                seen_singletons.add(eid)
                if entity_id:
                    singleton_canonical[eid] = entity_id

            entry = dict(e)
            entry["BuildingId"] = new_bid

            # Zet berekeningstype op projectberekening.
            # Status=3 = "door gebruiker ingesteld" (Uniec3 respecteert de waarde).
            # Status=7 = "berekend/overridden" (Uniec3 negeert de waarde en toont
            # "per gebouw" ook al staat Value op RZUNIT_PROJECT). Bronkavels gemaakt
            # met NTAVersionId=109 hebben Status=7 op RZFORM_CALCUNIT — zonder
            # expliciete Status-correctie werkt de RZUNIT_PROJECT-instelling niet.
            if eid == "RZFORM":
                for p in entry.get("NTAPropertyDatas", []):
                    if p.get("NTAPropertyId") == "RZFORM_CALCUNIT":
                        p = dict(p)          # los van het origineel
                        p["Value"]  = "RZUNIT_PROJECT"
                        p["Status"] = 3      # forceer "door gebruiker ingesteld"
                        # Vervang in de lijst
                        props = list(entry.get("NTAPropertyDatas", []))
                        idx = next(i for i, x in enumerate(props)
                                   if x.get("NTAPropertyId") == "RZFORM_CALCUNIT")
                        props[idx] = p
                        entry["NTAPropertyDatas"] = props

            other_entities.append(entry)

    # ── Stap 4: ID-remap toepassen op alle entiteiten ─────────────────────────
    # Remap wordt ook op deduped_lib toegepast zodat cross-referenties
    # tussen bibliotheek/installatie-typen onderling correct worden bijgewerkt.
    merged_entities = (
        [_remap_entity(e, id_remap) for e in deduped_lib] +
        [_remap_entity(e, id_remap) for e in other_entities]
    )

    # Set van geldige entity-IDs in het eindresultaat (voor relatie-filtering)
    valid_entity_ids = {
        e.get("NTAEntityDataId", "")
        for e in merged_entities
        if e.get("NTAEntityDataId")
    }

    # ── Stap 5: Relaties samenvoegen + remap + dedup + filteren ──────────────
    seen_relation_ids = set()
    merged_relations  = []
    for k in kavels:
        for r in k["relations"]:
            r2 = _remap_relation(dict(r, BuildingId=new_bid), id_remap)

            # Dedup op composite relatie-ID (na remap)
            rid = r2.get("NTAEntityRelationDataId") or ""
            if rid:
                if rid in seen_relation_ids:
                    continue
                seen_relation_ids.add(rid)

            # Sla relaties over met ontbrekende parent of child
            parent_id = r2.get("ParentId", "")
            child_id  = r2.get("ChildId", "")
            if (parent_id and parent_id not in valid_entity_ids) or \
               (child_id  and child_id  not in valid_entity_ids):
                continue

            merged_relations.append(r2)

    # ── Stap 6: Deltas samenvoegen + remap + dedup ────────────────────────────
    seen_delta_ids = set()
    merged_deltas  = []
    for k in kavels:
        for d in k["deltas"]:
            d2  = _remap_entity(dict(d, BuildingId=new_bid), id_remap)
            did = d2.get("NTADeltaId") or d2.get("Id") or d2.get("id") or ""
            if did:
                if did in seen_delta_ids:
                    continue
                seen_delta_ids.add(did)
            merged_deltas.append(d2)

    summary = dict(first["summary"])
    summary["BuildingId"] = new_bid

    # ── ZIP bouwen ────────────────────────────────────────────────────────────
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        zout.writestr("meta.json",      encode_json(first["meta"]))
        zout.writestr("folders.json",   encode_json(first["folders"]))
        zout.writestr("projects.json",  encode_json(first["projects"]))
        zout.writestr("buildings.json", encode_json([proj_building]))
        zout.writestr(f"buildings/{new_bid}/entities.json",  encode_json(merged_entities))
        zout.writestr(f"buildings/{new_bid}/relations.json", encode_json(merged_relations))
        zout.writestr(f"buildings/{new_bid}/deltas.json",    encode_json(merged_deltas))
        zout.writestr(f"buildings/{new_bid}/summary.json",   encode_json(summary))

    n_units = sum(1 for e in merged_entities if e.get("NTAEntityId") == "UNIT")
    return buf.getvalue(), n_units
