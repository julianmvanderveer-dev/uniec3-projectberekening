"""
Uniec3 merge-logica: voegt losse woningberekeningen samen tot één projectberekening.

Deduplicatiestrategie:
- LIB*-entiteiten (bouwkundige bibliotheek): gededupliceerd op inhoud (UUID-vrij),
  met ID-remapping zodat alle verwijzingen naar duplicaten naar het
  behouden exemplaar wijzen.
- RESULT-* / PRESTATIE: uitgesloten (herberekend door Uniec3 zelf).
- UNIT / RZ / *: per-woning-entiteiten, meegenomen van alle kavels.
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

# ── Categorieën ────────────────────────────────────────────────────────────────

# Berekeningsresultaten: niet overnemen (Uniec3 herberekent ze).
RESULT_EXACT    = {"PRESTATIE"}
RESULT_PREFIXES = ("RESULT-",)

# Gedeelde systeemdefinities: dedup op inhoud + ID-remapping.
# Dit zijn entiteiten die per woning worden gekopieerd maar inhoudelijk
# identiek zijn (bibliotheek, installaties).
LIB_PREFIXES = (
    "LIB",    # LIBCONSTRD, LIBCONSTRT, LIBCONSTRL, LIBCONSTRFORM, …
    "TAPW",   # TAPW, TAPW-AFG, TAPW-DISTR, TAPW-OPWEK, TAPW-UNIT, …
    "VERW",   # VERW, VERW-AFG, VERW-DISTR, VERW-OPWEK, …
    "KOEL",   # KOEL, KOEL-AFG, KOEL-DISTR, KOEL-OPWEK, …
)

# Per-woning-entiteiten: altijd multi (van alle kavels).
MULTI_EXACT    = {"RZ"}
MULTI_PREFIXES = ("UNIT",)


def _is_result(eid: str) -> bool:
    return eid in RESULT_EXACT or any(eid.startswith(p) for p in RESULT_PREFIXES)


def _is_lib(eid: str) -> bool:
    return any(eid.startswith(p) for p in LIB_PREFIXES)


def _is_forced_multi(eid: str) -> bool:
    return eid in MULTI_EXACT or any(eid.startswith(p) for p in MULTI_PREFIXES)


def _content_key(e: dict) -> str:
    """Hash van entity-inhoud zonder UUID-waarden.
    Twee entiteiten met identieke eigenschappen maar andere ID's
    (bijv. gekopieerde bibliotheek per woning) krijgen dezelfde hash."""
    parts = [e.get("NTAEntityId", "")]
    for p in sorted(e.get("NTAPropertyDatas", []), key=lambda x: x.get("NTAPropertyId", "")):
        val = str(p.get("Value", ""))
        if _UUID_RE.match(val):
            continue   # sla ID-referenties over
        parts.append(f"{p.get('NTAPropertyId', '')}={val}")
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

    # ── Stap 1: Bibliotheek dedupliceren + ID-remap opbouwen ─────────────────
    # Voor elk LIB*-type: bij dubbele inhoud → canonical ID bewaren,
    # duplicaat-ID opnemen in id_remap zodat verwijzingen daarnaar worden
    # bijgewerkt naar het canonical exemplaar.
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
            ck     = _content_key(e)
            old_id = e.get("NTAEntityDataId", "")
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
    other_entities   = []
    seen_entity_ids  = set()
    seen_singletons  = set()
    seen_installatie = set()

    for kavel_idx, k in enumerate(kavels):
        is_first = (kavel_idx == 0)

        for e in k["entities"]:
            eid = e.get("NTAEntityId", "")

            # LIB* al verwerkt in stap 1
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

            # Singletons: alleen eerste kavel
            if not is_multi(eid):
                if eid in seen_singletons:
                    continue
                seen_singletons.add(eid)

            # INSTALLATIE: dedup op naam
            if eid == "INSTALLATIE":
                naam = next(
                    (p.get("Value", "") for p in e.get("NTAPropertyDatas", [])
                     if p.get("NTAPropertyId") == "INSTALL_NAAM"),
                    entity_id,
                )
                if naam in seen_installatie:
                    continue
                seen_installatie.add(naam)

            entry = dict(e)
            entry["BuildingId"] = new_bid

            # Zet berekeningstype op projectberekening
            if eid == "RZFORM":
                for p in entry.get("NTAPropertyDatas", []):
                    if p.get("NTAPropertyId") == "RZFORM_CALCUNIT":
                        p["Value"] = "RZUNIT_PROJECT"

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
