from flask import Flask, render_template, request, Response, stream_with_context
import threading
import queue
import json
import time
import requests as req

app = Flask(__name__)

# ── helpers ──────────────────────────────────────────────────────────────────

BASE_URL = "https://apis.roblox.com/datastores/v1/universes/{universe_id}"

def make_headers(api_key):
    return {"x-api-key": api_key}

def list_keys(api_key, universe_id, datastore_name, prefix="", cursor=None):
    url = f"{BASE_URL.format(universe_id=universe_id)}/standard-datastores/datastore/entries"
    params = {"datastoreName": datastore_name, "limit": 100}
    if prefix:
        params["prefix"] = prefix
    if cursor:
        params["cursor"] = cursor
    r = req.get(url, headers=make_headers(api_key), params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def get_entry(api_key, universe_id, datastore_name, key):
    url = f"{BASE_URL.format(universe_id=universe_id)}/standard-datastores/datastore/entries/entry"
    params = {"datastoreName": datastore_name, "entryKey": key}
    r = req.get(url, headers=make_headers(api_key), params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def set_entry(api_key, universe_id, datastore_name, key, value):
    url = f"{BASE_URL.format(universe_id=universe_id)}/standard-datastores/datastore/entries/entry"
    params = {"datastoreName": datastore_name, "entryKey": key}
    r = req.post(url, headers={**make_headers(api_key), "Content-Type": "application/json"},
                 params=params, data=json.dumps(value), timeout=30)
    r.raise_for_status()

def list_ordered_keys(api_key, universe_id, datastore_name, prefix="", cursor=None):
    url = f"https://apis.roblox.com/ordered-data-stores/v1/universes/{universe_id}/orderedDataStores/{datastore_name}/scopes/global/entries"
    params = {"max_page_size": 100}
    if cursor:
        params["page_token"] = cursor
    r = req.get(url, headers=make_headers(api_key), params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def set_ordered_entry(api_key, universe_id, datastore_name, key, value):
    url = f"https://apis.roblox.com/ordered-data-stores/v1/universes/{universe_id}/orderedDataStores/{datastore_name}/scopes/global/entries"
    params = {"id": key}
    r = req.post(url, headers={**make_headers(api_key), "Content-Type": "application/json"},
                 params=params, json={"value": value}, timeout=30)
    r.raise_for_status()

# ── migration worker ──────────────────────────────────────────────────────────

def run_migration(config, log_queue):
    api_key      = config["api_key"]
    old_uid      = config["old_universe_id"]
    new_uid      = config["new_universe_id"]
    src_ds_name  = config["source_datastore_name"]
    tgt_ds_name  = config.get("target_datastore_name") or src_ds_name
    # prefix       = config.get("key_prefix", "")  # coming soon: filter by key list
    prefix       = ""
    dry_run      = config.get("dry_run", False)
    ds_type      = config.get("datastore_type", "standard")

    def log(msg, level="info"):
        log_queue.put({"msg": msg, "level": level})

    log(f"🚀 Starting {'DRY RUN' if dry_run else 'MIGRATION'} — {ds_type.upper()} datastore")
    log(f"📦 Source DS: {src_ds_name}  →  Target DS: {tgt_ds_name}")
    log(f"🔁 {old_uid}  →  {new_uid}")
    if prefix:
        log(f"🔍 Prefix filter: {prefix}")

    success = 0
    failed  = []
    cursor  = None

    try:
        while True:
            if ds_type == "standard":
                data   = list_keys(api_key, old_uid, src_ds_name, prefix, cursor)
                keys   = [e["key"] for e in data.get("keys", [])]
                cursor = data.get("nextPageCursor")
            else:
                data   = list_ordered_keys(api_key, old_uid, src_ds_name, prefix, cursor)
                keys   = [e["id"] for e in data.get("entries", [])]
                cursor = data.get("nextPageToken")

            for key in keys:
                try:
                    if ds_type == "standard":
                        value = get_entry(api_key, old_uid, src_ds_name, key)
                        if not value:
                            log(f"⏭ SKIP (empty): {key}", "info")
                            continue
                        if not dry_run:
                            set_entry(api_key, new_uid, tgt_ds_name, key, value)
                    else:
                        entry = get_entry(api_key, old_uid, src_ds_name, key)
                        if not entry:
                            log(f"⏭ SKIP (empty): {key}", "info")
                            continue
                        if not dry_run:
                            set_ordered_entry(api_key, new_uid, tgt_ds_name, key, entry)
                    success += 1
                    log(f"✅ {'[DRY]' if dry_run else ''} {key}", "success")
                    time.sleep(0.1)
                except Exception as e:
                    failed.append(key)
                    log(f"❌ FAILED: {key} — {e}", "error")

            if not cursor:
                break

    except Exception as e:
        log(f"💥 Fatal error: {e}", "error")

    log("─" * 40)
    log(f"✅ Success: {success}   ❌ Failed: {len(failed)}", "info")
    if failed:
        log(f"Failed keys: {', '.join(failed)}", "error")
    log("🏁 Done!", "success")
    log("__DONE__", "__done__")

# ── routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/shutdown", methods=["POST"])
def shutdown():
    import os
    os.kill(os.getpid(), 9)
    return {"status": "shutting down"}

@app.route("/migrate", methods=["POST"])
def migrate():
    config = request.get_json()
    log_queue = queue.Queue()

    thread = threading.Thread(target=run_migration, args=(config, log_queue), daemon=True)
    thread.start()

    def generate():
        while True:
            try:
                item = log_queue.get(timeout=60)
                yield f"data: {json.dumps(item)}\n\n"
                if item.get("level") == "__done__":
                    break
            except queue.Empty:
                yield f"data: {json.dumps({'msg': '⏱ Timeout', 'level': 'error'})}\n\n"
                break

    return Response(stream_with_context(generate()), mimetype="text/event-stream")

if __name__ == "__main__":
    print("\n🟢  Roblox Datastore Migrator")
    print("   Open → http://localhost:5000\n")
    app.run(debug=False, port=5000)
