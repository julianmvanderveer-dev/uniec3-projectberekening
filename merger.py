"""
Uniec3 merge-logica: voegt losse woningberekeningen samen tot één projectberekening.
"""

import zipfile
import json
import io
import time
from collections import Counter
from datetime import datetime


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

            meta     = read_json_from_zip(zf, "meta.json")
            folders  = read_json_from_zip(zf, "folders.json")
            projects = read_json_from_zip(zf, "projects.json")
            buildings = read_json_from_zip(zf, "buildings.json")
            if isinstance(buildings, dict):
                buildings = [buildings]

            building = buildings[0]
            bid = building["BuildingId"]
            prefix = f"buildings/{bid}/"

            def get(suffix):
                n = next((x for x in names if x.startswith(prefix) and x.endswith(suffix)), None)
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
        raise ValueError("Geen geldige bestanden ontvangen.")

    # ── Singleton vs. multi ───────────────────────────────────────────────────
    type_counts = [Counter(e["NTAEntityId"] for e in k["entities"]) for k in kavels]
    all_types   = set(t for c in type_counts for t in c)

    def is_multi(eid):
        if eid == "RZ" or eid.startswith("UNIT"):
            return True
        return any(c.get(eid, 0) > 1 for c in type_counts)

    # ── Nieuw project-BuildingId ──────────────────────────────────────────────
    new_bid = int(time.time())
    first   = kavels[0]
    now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.0000000")

    proj_building = dict(first["building"])
    proj_building["BuildingId"] = new_bid
    proj_building["ChangeDate"] = now_iso

    # ── Entiteiten samenvoegen ────────────────────────────────────────────────
    merged_entities = []
    seen_singletons = set()
    # LIBCONSTRL: dedupliceer op LIBCONSTRL_BEPALING (zelfde bibliotheekelement
    # komt in elk kavel voor maar mag maar één keer in het project).
    seen_libconstrl = set()

    for k in kavels:
        for e in k["entities"]:
            eid = e["NTAEntityId"]

            # Singletons: alleen eerste kavel
            if not is_multi(eid):
                if eid in seen_singletons:
                    continue
                seen_singletons.add(eid)

            # LIBCONSTRL: dedupliceer op bepaling-code
            if eid == "LIBCONSTRL":
                bepaling = next(
                    (p.get("Value", "") for p in e.get("NTAPropertyDatas", [])
                     if p.get("NTAPropertyId") == "LIBCONSTRL_BEPALING"),
                    e.get("NTAEntityDataId", "")
                )
                if bepaling in seen_libconstrl:
                    continue
                seen_libconstrl.add(bepaling)

            entry = dict(e)
            entry["BuildingId"] = new_bid
            if eid == "RZFORM":
                for p in entry.get("NTAPropertyDatas", []):
                    if p.get("NTAPropertyId") == "RZFORM_CALCUNIT":
                        p["Value"] = "RZUNIT_PROJECT"
            merged_entities.append(entry)

    # ── Relaties & deltas samenvoegen ─────────────────────────────────────────
    merged_relations = [dict(r, BuildingId=new_bid) for k in kavels for r in k["relations"]]
    merged_deltas    = [dict(d, BuildingId=new_bid) for k in kavels for d in k["deltas"]]

    summary = dict(first["summary"])
    summary["BuildingId"] = new_bid

    # ── ZIP bouwen ────────────────────────────────────────────────────────────
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        zout.writestr("meta.json",     encode_json(first["meta"]))
        zout.writestr("folders.json",  encode_json(first["folders"]))
        zout.writestr("projects.json", encode_json(first["projects"]))
        zout.writestr("buildings.json", encode_json([proj_building]))
        zout.writestr(f"buildings/{new_bid}/entities.json",  encode_json(merged_entities))
        zout.writestr(f"buildings/{new_bid}/relations.json", encode_json(merged_relations))
        zout.writestr(f"buildings/{new_bid}/deltas.json",    encode_json(merged_deltas))
        zout.writestr(f"buildings/{new_bid}/summary.json",   encode_json(summary))

    n_units = sum(1 for e in merged_entities if e["NTAEntityId"] == "UNIT")
    return buf.getvalue(), n_units
