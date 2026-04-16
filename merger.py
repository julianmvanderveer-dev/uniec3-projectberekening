"""
Uniec3 merge-logica: voegt losse woningberekeningen samen tot één projectberekening.
"""

import zipfile
import json
import io
import time
from collections import Counter
from datetime import datetime

# ── Constanten ─────────────────────────────────────────────────────────────────

# Entiteitstypen die berekeningsresultaten zijn. Uniec3 herberekent deze
# zelf na import; ze horen niet in een invoerbestand.
RESULT_PREFIXES = ("RESULT-",)
RESULT_EXACT    = {"PRESTATIE"}

# Entiteitstypen die de gedeelde bouwkundige bibliotheek vormen.
# Elke woning heeft zijn eigen kopie met unieke ID's, maar de inhoud is
# identiek. We nemen deze uitsluitend van het EERSTE kavel/building.
BIBLIOTHEEK_EXACT    = {"LIBCONSTRL"}
BIBLIOTHEEK_PREFIXES = ("CONSTRT",)   # CONSTRT, CONSTRT_LAAG, enz.

# Entiteitstypen die per woning uniek zijn (altijd multi).
FORCED_MULTI_EXACT    = {"RZ"}
FORCED_MULTI_PREFIXES = ("UNIT",)


def _is_result(eid):
    if eid in RESULT_EXACT:
        return True
    return any(eid.startswith(p) for p in RESULT_PREFIXES)


def _is_bibliotheek(eid):
    if eid in BIBLIOTHEEK_EXACT:
        return True
    return any(eid.startswith(p) for p in BIBLIOTHEEK_PREFIXES)


def _is_forced_multi(eid):
    if eid in FORCED_MULTI_EXACT:
        return True
    return any(eid.startswith(p) for p in FORCED_MULTI_PREFIXES)


def read_json_from_zip(zf, name):
    with zf.open(name) as f:
        raw = f.read()
    return json.loads(raw.decode("utf-8-sig"))


def encode_json(obj):
    return ("\ufeff" + json.dumps(obj, ensure_ascii=False, separators=(",", ":"))).encode("utf-8")


def merge_uniec3(file_objects):
    """
    Voegt meerdere .uniec3 bestanden (file-achtige objecten) samen
    tot één projectberekening.

    Geeft terug: (bytes van .uniec3, aantal woningen)
    """
    kavels = []

    for fo in file_objects:
        with zipfile.ZipFile(fo, "r") as zf:
            names = zf.namelist()

            meta      = read_json_from_zip(zf, "meta.json")
            folders   = read_json_from_zip(zf, "folders.json")
            projects  = read_json_from_zip(zf, "projects.json")
            buildings = read_json_from_zip(zf, "buildings.json")
            if isinstance(buildings, dict):
                buildings = [buildings]

            # Verzamelbestanden: itereer over ALLE buildings per bestand
            for building in buildings:
                bid    = building["BuildingId"]
                prefix = f"buildings/{bid}/"

                def get(suffix, _prefix=prefix):
                    n = next((x for x in names if x.startswith(_prefix) and x.endswith(suffix)), None)
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

    # ── Singleton vs. multi (voor overige typen) ──────────────────────────────
    type_counts = [Counter(e["NTAEntityId"] for e in k["entities"]) for k in kavels]

    def is_multi(eid):
        if _is_forced_multi(eid):
            return True
        return any(c.get(eid, 0) > 1 for c in type_counts)

    # ── Nieuw project-BuildingId ──────────────────────────────────────────────
    new_bid  = int(time.time())
    first    = kavels[0]
    now_iso  = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.0000000")

    proj_building = dict(first["building"])
    proj_building["BuildingId"] = new_bid
    proj_building["ChangeDate"] = now_iso

    # ── Entiteiten samenvoegen ────────────────────────────────────────────────
    merged_entities  = []
    seen_entity_ids  = set()   # globale dedup op NTAEntityDataId
    seen_singletons  = set()   # dedup op entity-type voor singletons
    seen_libconstrl  = set()   # key: LIBCONSTRL_BEPALING
    seen_installatie = set()   # key: INSTALL_NAAM

    for kavel_idx, k in enumerate(kavels):
        is_first = (kavel_idx == 0)

        for e in k["entities"]:
            eid = e["NTAEntityId"]

            # ── 0. Berekeningsresultaten: altijd overslaan ────────────────────
            if _is_result(eid):
                continue

            # ── 1. Bouwkundige bibliotheek: uitsluitend van eerste kavel ──────
            # CONSTRT-entities bevatten interne ID-referenties die per building
            # uniek zijn, waardoor inhoudelijke dedup onbetrouwbaar is.
            # De bibliotheek is gedeeld over alle woningen → eerste kavel volstaat.
            if _is_bibliotheek(eid) and not is_first:
                continue

            # ── 2. Globale ID-deduplicatie ────────────────────────────────────
            entity_data_id = e.get("NTAEntityDataId", "")
            if entity_data_id:
                if entity_data_id in seen_entity_ids:
                    continue
                seen_entity_ids.add(entity_data_id)

            # ── 3. Singletons: alleen eerste kavel ────────────────────────────
            if not is_multi(eid):
                if eid in seen_singletons:
                    continue
                seen_singletons.add(eid)

            # ── 4. LIBCONSTRL: extra dedup op bepaling-code ───────────────────
            if eid == "LIBCONSTRL":
                bepaling = next(
                    (p.get("Value", "") for p in e.get("NTAPropertyDatas", [])
                     if p.get("NTAPropertyId") == "LIBCONSTRL_BEPALING"),
                    entity_data_id
                )
                if bepaling in seen_libconstrl:
                    continue
                seen_libconstrl.add(bepaling)

            # ── 5. INSTALLATIE: dedup op installatienaam ──────────────────────
            if eid == "INSTALLATIE":
                naam = next(
                    (p.get("Value", "") for p in e.get("NTAPropertyDatas", [])
                     if p.get("NTAPropertyId") == "INSTALL_NAAM"),
                    entity_data_id
                )
                if naam in seen_installatie:
                    continue
                seen_installatie.add(naam)

            entry = dict(e)
            entry["BuildingId"] = new_bid
            # Zet het berekeningstype altijd op projectberekening
            if eid == "RZFORM":
                for p in entry.get("NTAPropertyDatas", []):
                    if p.get("NTAPropertyId") == "RZFORM_CALCUNIT":
                        p["Value"] = "RZUNIT_PROJECT"
            merged_entities.append(entry)

    # ── Relaties samenvoegen (dedup op relatie-ID) ────────────────────────────
    seen_relation_ids = set()
    merged_relations  = []
    for k in kavels:
        for r in k["relations"]:
            rid = r.get("NTARelationId") or r.get("Id") or r.get("id") or ""
            if rid and rid in seen_relation_ids:
                continue
            if rid:
                seen_relation_ids.add(rid)
            merged_relations.append(dict(r, BuildingId=new_bid))

    # ── Deltas samenvoegen (dedup op delta-ID) ────────────────────────────────
    seen_delta_ids = set()
    merged_deltas  = []
    for k in kavels:
        for d in k["deltas"]:
            did = d.get("NTADeltaId") or d.get("Id") or d.get("id") or ""
            if did and did in seen_delta_ids:
                continue
            if did:
                seen_delta_ids.add(did)
            merged_deltas.append(dict(d, BuildingId=new_bid))

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

    n_units = sum(1 for e in merged_entities if e["NTAEntityId"] == "UNIT")
    return buf.getvalue(), n_units
