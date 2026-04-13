"""
demo_cache.py — Pre-loads demo_records data from GCS/BQ into memory for
instant responses on 360, KYC, Fraud, and Clusters pages.

Loaded once at startup. All lookups are O(1) dict access.
"""

import csv
import logging
from collections import defaultdict
from io import StringIO

log = logging.getLogger("demo_cache")

# ---------------------------------------------------------------------------
# In-memory stores
# ---------------------------------------------------------------------------
_source_records: list[dict] = []          # raw CSV rows
_records_by_id: dict[str, dict] = {}      # record_id -> row
_clusters: dict[str, list[dict]] = {}     # cluster_id -> [records]
_cluster_meta: dict[str, dict] = {}       # cluster_id -> {size, avg_edge_score, ...}
_edges: list[dict] = []                   # all edges
_edges_by_cluster: dict[str, list[dict]] = {}
_network_scores: dict[str, dict] = {}     # record_id -> scores
_fraud_rings: list[dict] = []             # computed cross-cluster rings
_kyc_alerts: list[dict] = []             # computed KYC risk alerts
_loaded = False


def is_loaded() -> bool:
    return _loaded


def load_demo_data(bucket_name: str, job_suffix: str = "demo_records"):
    """Load source CSV, clusters, edges, scores from GCS + BQ."""
    global _loaded
    if _loaded:
        return

    try:
        from google.cloud import bigquery, storage

        log.info(f"Loading demo data for job_suffix={job_suffix}")

        # 1. Source records CSV from GCS
        gcs = storage.Client()
        blob = gcs.bucket(bucket_name).blob(f"to-process/{job_suffix}.csv")
        text = blob.download_as_text()
        reader = csv.DictReader(StringIO(text))
        for row in reader:
            _source_records.append(row)
            _records_by_id[row["id"]] = row
        log.info(f"  Loaded {len(_source_records)} source records from GCS")

        # 2. Entity clusters from BQ
        bq = bigquery.Client()
        rows = bq.query(
            f"SELECT * FROM entity_resolution.entity_clusters "
            f"WHERE job_suffix = '{job_suffix}'"
        ).result()
        for r in rows:
            d = dict(r)
            rid = d["record_id"]
            cid = d["cluster_id"]
            if cid not in _clusters:
                _clusters[cid] = []
                _cluster_meta[cid] = {
                    "cluster_id": cid,
                    "cluster_size": d["cluster_size"],
                    "avg_edge_score": round(d.get("avg_edge_score", 1.0), 4),
                    "is_singleton": d["is_singleton"],
                }
            # Merge source record fields
            src = _records_by_id.get(rid, {})
            record = {
                "record_id": rid,
                "cluster_id": cid,
                "name": src.get("name", ""),
                "address": src.get("address", ""),
                "dob": src.get("dob", ""),
                "email": src.get("email", ""),
                "phone": src.get("phone", ""),
                "company": src.get("company", ""),
                "source": src.get("source", ""),
            }
            _clusters[cid].append(record)
        log.info(f"  Loaded {sum(len(v) for v in _clusters.values())} cluster assignments across {len(_clusters)} clusters")

        # 3. Cluster edges from BQ
        rows = bq.query(
            f"SELECT * FROM entity_resolution.cluster_edges "
            f"WHERE job_suffix = '{job_suffix}'"
        ).result()
        for r in rows:
            d = dict(r)
            edge = {
                "record_id_1": d["record_id_1"],
                "record_id_2": d["record_id_2"],
                "deberta_score": round(d["deberta_score"], 4),
                "cluster_id": d["cluster_id"],
                "has_email": d.get("has_email", 0),
                "email_score": round(d.get("email_score", 0), 4),
                "has_phone": d.get("has_phone", 0),
                "has_dob": d.get("has_dob", 0),
                "has_company": d.get("has_company", 0),
                "vetoed": d.get("vetoed", False),
                "veto_reason": d.get("veto_reason", ""),
            }
            _edges.append(edge)
            cid = edge["cluster_id"]
            if cid not in _edges_by_cluster:
                _edges_by_cluster[cid] = []
            _edges_by_cluster[cid].append(edge)
        log.info(f"  Loaded {len(_edges)} cluster edges")

        # 4. Network scores from BQ
        rows = bq.query(
            f"SELECT DISTINCT record_id, cluster_id_x, cluster_risk_score, "
            f"bad_neighbour_score, shared_field_score, network_centrality, "
            f"composite_context_score, is_flagged_direct, is_flagged_2hop "
            f"FROM entity_resolution.network_scores "
            f"WHERE cluster_id_x IN ("
            f"  SELECT DISTINCT cluster_id FROM entity_resolution.entity_clusters "
            f"  WHERE job_suffix = '{job_suffix}')"
        ).result()
        for r in rows:
            d = dict(r)
            _network_scores[d["record_id"]] = {
                "cluster_risk": round(d.get("cluster_risk_score", 0), 4),
                "bad_neighbour": round(d.get("bad_neighbour_score", 0), 4),
                "shared_field": round(d.get("shared_field_score", 0), 4),
                "centrality": round(d.get("network_centrality", 0), 4),
                "composite": round(d.get("composite_context_score", 0), 4),
                "is_flagged_direct": d.get("is_flagged_direct", False),
                "is_flagged_2hop": d.get("is_flagged_2hop", False),
            }
        log.info(f"  Loaded {len(_network_scores)} network scores")

        # 5. Compute fraud rings (cross-cluster shared fields)
        _compute_fraud_rings()

        # 6. Compute KYC alerts (OFAC + company linkages)
        _compute_kyc_alerts()

        _loaded = True
        log.info("Demo cache ready")

    except Exception as e:
        log.error(f"Failed to load demo cache: {e}")


def _compute_fraud_rings():
    """Detect cross-cluster shared emails/phones/companies."""
    # Group records by email, phone, company
    email_map: dict[str, list[dict]] = defaultdict(list)
    company_map: dict[str, list[dict]] = defaultdict(list)

    for cid, records in _clusters.items():
        for rec in records:
            if rec["email"]:
                email_map[rec["email"].strip().lower()].append(rec)
            if rec["company"]:
                # Normalize company name
                co = rec["company"].strip().lower().rstrip(".,")
                for suffix in [" inc", " llc", " ltd", " co"]:
                    co = co.rstrip().removesuffix(suffix)
                company_map[co].append(rec)

    rings = []
    seen_keys = set()

    # Email rings: same email, different clusters
    for email, recs in email_map.items():
        cluster_ids = set(r["cluster_id"] for r in recs)
        if len(cluster_ids) > 1:
            key = f"email:{email}"
            if key in seen_keys:
                continue
            seen_keys.add(key)
            rings.append({
                "ring_id": f"email-{len(rings)+1}",
                "type": "shared_email",
                "shared_value": email,
                "records": [
                    {"record_id": r["record_id"], "name": r["name"],
                     "cluster_id": r["cluster_id"], "source": r["source"]}
                    for r in recs
                ],
                "cluster_count": len(cluster_ids),
                "record_count": len(recs),
            })

    # Company rings: same company, different clusters
    for company, recs in company_map.items():
        cluster_ids = set(r["cluster_id"] for r in recs)
        if len(cluster_ids) > 1:
            key = f"company:{company}"
            if key in seen_keys:
                continue
            seen_keys.add(key)
            # Get original company name from first record
            orig_name = recs[0]["company"]
            rings.append({
                "ring_id": f"company-{len(rings)+1}",
                "type": "shared_company",
                "shared_value": orig_name,
                "records": [
                    {"record_id": r["record_id"], "name": r["name"],
                     "cluster_id": r["cluster_id"], "source": r["source"]}
                    for r in recs
                ],
                "cluster_count": len(cluster_ids),
                "record_count": len(recs),
            })

    _fraud_rings.extend(rings)
    log.info(f"  Computed {len(_fraud_rings)} fraud rings")


def _compute_kyc_alerts():
    """Detect KYC risk via OFAC linkage and shared companies."""
    # Find OFAC/sanctions records
    flagged_ids = set()
    flagged_clusters = set()
    for rec in _source_records:
        src = rec.get("source", "").upper()
        if "OFAC" in src or "SDN" in src or "SANCTION" in src:
            flagged_ids.add(rec["id"])
            # Find their cluster
            for cid, members in _clusters.items():
                if any(m["record_id"] == rec["id"] for m in members):
                    flagged_clusters.add(cid)

    if not flagged_ids:
        log.info("  No OFAC/sanctioned records found for KYC alerts")
        return

    # Find entities that share company/email with flagged entities
    flagged_companies = set()
    flagged_emails = set()
    for fid in flagged_ids:
        rec = _records_by_id.get(fid, {})
        if rec.get("company"):
            co = rec["company"].strip().lower().rstrip(".,")
            for suffix in [" inc", " llc", " ltd", " co"]:
                co = co.rstrip().removesuffix(suffix)
            flagged_companies.add(co)
        if rec.get("email"):
            flagged_emails.add(rec["email"].strip().lower())

    alerts = []

    # Direct flags (the OFAC entity itself)
    for fid in flagged_ids:
        rec = _records_by_id.get(fid, {})
        for cid, members in _clusters.items():
            if any(m["record_id"] == fid for m in members):
                alerts.append({
                    "record_id": fid,
                    "name": rec.get("name", ""),
                    "cluster_id": cid,
                    "risk_type": "direct_flag",
                    "risk_label": f"OFAC SDN List — {rec.get('source', '')}",
                    "severity": "critical",
                    "path": [],
                })

    # 1-hop: other records in the same cluster as flagged
    for cid in flagged_clusters:
        for member in _clusters.get(cid, []):
            if member["record_id"] not in flagged_ids:
                flagged_name = next(
                    (m["name"] for m in _clusters[cid] if m["record_id"] in flagged_ids),
                    "unknown"
                )
                alerts.append({
                    "record_id": member["record_id"],
                    "name": member["name"],
                    "cluster_id": cid,
                    "risk_type": "1_hop",
                    "risk_label": f"Same entity cluster as {flagged_name} (OFAC)",
                    "severity": "high",
                    "path": [member["name"], flagged_name],
                })

    # 2-hop: entities that share company/email with flagged cluster entities
    for cid, members in _clusters.items():
        if cid in flagged_clusters:
            continue
        for member in members:
            link_reason = None
            link_value = None
            if member.get("company"):
                co = member["company"].strip().lower().rstrip(".,")
                for suffix in [" inc", " llc", " ltd", " co"]:
                    co = co.rstrip().removesuffix(suffix)
                if co in flagged_companies:
                    link_reason = "shared_company"
                    link_value = member["company"]
            if not link_reason and member.get("email"):
                em = member["email"].strip().lower()
                if em in flagged_emails:
                    link_reason = "shared_email"
                    link_value = member["email"]

            if link_reason:
                # Find the flagged entity name
                flagged_name = next(
                    (_records_by_id[fid].get("name", "?") for fid in flagged_ids),
                    "unknown"
                )
                alerts.append({
                    "record_id": member["record_id"],
                    "name": member["name"],
                    "cluster_id": cid,
                    "risk_type": "2_hop",
                    "risk_label": f"{link_reason.replace('_', ' ').title()}: {link_value} → links to {flagged_name} (OFAC)",
                    "severity": "medium",
                    "path": [member["name"], link_value, flagged_name],
                })

    _kyc_alerts.extend(alerts)
    log.info(f"  Computed {len(_kyc_alerts)} KYC alerts")


# ---------------------------------------------------------------------------
# Query API — called by app.py endpoints
# ---------------------------------------------------------------------------

def search_entities(query: str, limit: int = 10) -> list[dict]:
    """Fuzzy search by name across all source records."""
    q = query.strip().lower()
    if not q:
        return []

    results = []
    seen_clusters = set()

    for rec in _source_records:
        name = rec.get("name", "").lower()
        if q in name or any(w in name for w in q.split()):
            cid = None
            for c, members in _clusters.items():
                if any(m["record_id"] == rec["id"] for m in members):
                    cid = c
                    break
            if cid and cid not in seen_clusters:
                seen_clusters.add(cid)
                meta = _cluster_meta.get(cid, {})
                members = _clusters.get(cid, [])
                results.append({
                    "cluster_id": cid,
                    "name": rec["name"],
                    "record_id": rec["id"],
                    "cluster_size": meta.get("cluster_size", 1),
                    "avg_edge_score": meta.get("avg_edge_score", 1.0),
                    "sources": list(set(m["source"] for m in members if m["source"])),
                })
    return results[:limit]


def get_cluster_profile(cluster_id: str) -> dict | None:
    """Return full cluster profile: golden record + source records + edges."""
    if cluster_id not in _clusters:
        return None

    members = _clusters[cluster_id]
    meta = _cluster_meta[cluster_id]
    edges = _edges_by_cluster.get(cluster_id, [])

    # Build golden record (best-of-each-field)
    golden = _build_golden_record(members)

    # Get network scores for members
    scores = {}
    for m in members:
        s = _network_scores.get(m["record_id"])
        if s:
            scores[m["record_id"]] = s

    return {
        "cluster_id": cluster_id,
        "golden_record": golden,
        "records": members,
        "edges": edges,
        "meta": meta,
        "network_scores": scores,
    }


def _build_golden_record(members: list[dict]) -> dict:
    """Pick the best value for each field across source records."""
    golden = {}
    for field in ["name", "address", "dob", "email", "phone", "company"]:
        values = [m[field] for m in members if m.get(field)]
        if values:
            # Pick the longest (most complete) value
            golden[field] = max(values, key=len)
        else:
            golden[field] = ""
    golden["source_count"] = len(members)
    golden["sources"] = list(set(m["source"] for m in members if m["source"]))
    return golden


def get_fraud_rings() -> list[dict]:
    """Return detected fraud rings."""
    return _fraud_rings


def get_kyc_alerts() -> list[dict]:
    """Return KYC risk alerts."""
    return _kyc_alerts


def get_kyc_investigation(record_id: str) -> dict | None:
    """Return 2-hop graph for KYC investigation of a record."""
    # Find the alert for this record
    alert = next((a for a in _kyc_alerts if a["record_id"] == record_id), None)
    if not alert:
        return None

    # Build the graph: this record's cluster + linked clusters
    nodes = []
    graph_edges = []
    seen_nodes = set()

    # Add this record's cluster
    cid = alert["cluster_id"]
    for m in _clusters.get(cid, []):
        if m["record_id"] not in seen_nodes:
            seen_nodes.add(m["record_id"])
            nodes.append({
                "id": m["record_id"],
                "name": m["name"],
                "cluster_id": cid,
                "source": m["source"],
                "is_flagged": any(a["record_id"] == m["record_id"] and a["risk_type"] == "direct_flag" for a in _kyc_alerts),
                "is_target": m["record_id"] == record_id,
            })

    # Add edges within this cluster
    for e in _edges_by_cluster.get(cid, []):
        graph_edges.append({
            "source": e["record_id_1"],
            "target": e["record_id_2"],
            "score": e["deberta_score"],
            "type": "direct",
        })

    # Find linked flagged clusters
    for a in _kyc_alerts:
        if a["record_id"] == record_id and a["risk_type"] in ("1_hop", "2_hop"):
            # Find the flagged entity's cluster
            for fcid in set(fa["cluster_id"] for fa in _kyc_alerts if fa["risk_type"] == "direct_flag"):
                for m in _clusters.get(fcid, []):
                    if m["record_id"] not in seen_nodes:
                        seen_nodes.add(m["record_id"])
                        nodes.append({
                            "id": m["record_id"],
                            "name": m["name"],
                            "cluster_id": fcid,
                            "source": m["source"],
                            "is_flagged": any(fa["record_id"] == m["record_id"] and fa["risk_type"] == "direct_flag" for fa in _kyc_alerts),
                            "is_target": False,
                        })
                for e in _edges_by_cluster.get(fcid, []):
                    graph_edges.append({
                        "source": e["record_id_1"],
                        "target": e["record_id_2"],
                        "score": e["deberta_score"],
                        "type": "direct",
                    })

    # Add cross-cluster link
    rec = _records_by_id.get(record_id, {})
    for a in _kyc_alerts:
        if a["record_id"] == record_id and a["risk_type"] == "2_hop":
            # Find a flagged record to link to
            flagged_rec = next(
                (fa for fa in _kyc_alerts if fa["risk_type"] == "direct_flag"),
                None
            )
            if flagged_rec:
                graph_edges.append({
                    "source": record_id,
                    "target": flagged_rec["record_id"],
                    "score": 0,
                    "type": "cross_cluster",
                    "label": a["risk_label"],
                })

    scores = _network_scores.get(record_id, {})

    return {
        "record_id": record_id,
        "alert": alert,
        "nodes": nodes,
        "edges": graph_edges,
        "scores": scores,
    }


def get_all_clusters() -> list[dict]:
    """Return all non-singleton clusters with summary info."""
    result = []
    for cid, meta in _cluster_meta.items():
        if meta["is_singleton"]:
            continue
        members = _clusters[cid]
        names = list(set(m["name"] for m in members if m["name"]))
        result.append({
            "cluster_id": cid,
            "cluster_size": meta["cluster_size"],
            "avg_edge_score": meta["avg_edge_score"],
            "names": names[:3],
            "edge_count": len(_edges_by_cluster.get(cid, [])),
        })
    return sorted(result, key=lambda x: x["cluster_size"], reverse=True)
