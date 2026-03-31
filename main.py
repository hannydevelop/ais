import websocket
import os
from dotenv import load_dotenv
import json
import time
from datetime import datetime, timezone
from supabase import create_client  # COMMENTED OUT

load_dotenv()  

# =========================
# CONFIG
# =========================
API_KEY = os.getenv("AIS_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

BATCH_SIZE = 50
FLUSH_INTERVAL = 5  # seconds
RECONNECT_DELAY = 3

# Africa bounding box (ONLY Africa)
AFRICA_BBOX = [[[-35, -20], [37, 55]]]

# =========================
# INIT
# =========================
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)  # COMMENTED OUT

position_batch = []
last_flush_time = time.time()

# Dedup cache (avoid duplicate timestamps per vessel)
last_seen = {}

#=========================
# PARSE TIMESTAMP (ROBUST)
#=========================
def parse_timestamp(ts):
    if not ts:
        return None
    # Remove trailing " UTC" and parse
    ts = ts.replace(" UTC", "").strip()
    try:
        dt = datetime.fromisoformat(ts)
        return dt.isoformat()
    except Exception as e:
        print(f"Timestamp parse error: {e} | raw: {ts}")
        return None


# =========================
# LIGHTWEIGHT LOCATION TAGGING
# =========================
def get_region(lat, lon):
    if 4 <= lat <= 6 and 6 <= lon <= 8:
        return "Port Harcourt, Nigeria"
    if 6 <= lat <= 7 and 3 <= lon <= 4:
        return "Lagos, Nigeria"
    return "Africa"

# =========================
# UPSERT VESSEL (COMMENTED OUT)
# =========================
def upsert_vessel(mmsi, imo, name, vessel_type):
    vessel_row = {
        "mmsi": mmsi,
        "imo": imo,
        "name": name,
        "vessel_type": vessel_type,
        "updated_at": datetime.utcnow().isoformat()
    }

    try:
        supabase.table("vessels").upsert(vessel_row).execute()
    except Exception as e:
        print("Vessel upsert error:", e)

# =========================
# BATCH INSERT POSITIONS (COMMENTED OUT)
# =========================
def flush_batch():
    global position_batch, last_flush_time

    if not position_batch:
        return

    try:
        supabase.table("vessel_positions").insert(position_batch).execute()
    except Exception as e:
        print(f"❌ Batch insert error: {e}")
        # Try row by row to isolate the bad row
        print("Retrying row by row...")
        success = 0
        for row in position_batch:
            try:
                supabase.table("vessel_positions").insert(row).execute()
                success += 1
            except Exception as row_e:
                print(f"  ❌ Failed row mmsi={row.get('mmsi')}: {row_e}")
    finally:
        position_batch.clear()
        last_flush_time = time.time()

# =========================
# MESSAGE PROCESSOR
# =========================
def handle_message(data):
    global position_batch

    msg = data.get("Message", {})
    meta = data.get("MetaData", {})

    # --- DEBUG: print the raw message type so we know what's arriving ---
    msg_type = list(msg.keys()) if msg else []

    pos = (
        msg.get("PositionReport")
        or msg.get("StandardClassBPositionReport")
        or msg.get("ExtendedClassBPositionReport")
    )

    static = msg.get("ShipStaticData") or msg.get("StaticDataReport")

    if not pos:
        return

    mmsi = pos.get("UserID") or meta.get("MMSI")
    lat = pos.get("Latitude")
    lon = pos.get("Longitude")

    if not mmsi or lat is None or lon is None:
        return

    timestamp = meta.get("time_utc")

    if mmsi in last_seen and last_seen[mmsi] == timestamp:
        return
    last_seen[mmsi] = timestamp

    speed = pos.get("Sog")
    if speed is not None and speed < 0.3:
        return

    # --- Extract name from MetaData as fallback (AISStream includes ShipName here) ---
    name = None
    imo = None
    vessel_type = None

    if static:
        name = static.get("Name")
        imo = static.get("ImoNumber")
        vessel_type = static.get("ShipType")

    # ✅ KEY FIX: AISStream puts ShipName in MetaData even for position reports
    if not name:
        name = meta.get("ShipName")

    upsert_vessel(mmsi, imo, name, vessel_type)

    position_batch.append({
        "mmsi": mmsi,
        "lat": lat,
        "lon": lon,
        "speed": speed,
        "course": pos.get("Cog"),
        "location_name": get_region(lat, lon),
        "timestamp": parse_timestamp(timestamp)
    })

# =========================
# WEBSOCKET CALLBACKS
# =========================
def on_open(ws):
    ws.send(json.dumps({
        "APIKey": API_KEY,
        "BoundingBoxes": AFRICA_BBOX
    }))

def on_message(ws, message):
    global last_flush_time

    try:
        data = json.loads(message)
        handle_message(data)
    except Exception as e:
        print("Message error:", e)
        return

    if len(position_batch) >= BATCH_SIZE or (time.time() - last_flush_time > FLUSH_INTERVAL):
        flush_batch()

def on_error(ws, error):
    print("⚠️ WebSocket error:", error)

def on_close(ws, code, msg):
    print(f"🔌 Connection closed ({code}). Reconnecting in {RECONNECT_DELAY}s...")

# =========================
# RECONNECT LOOP
# =========================
def run():
    while True:
        try:
            print("🚀 Starting AISStream connection...")
            ws = websocket.WebSocketApp(
                "wss://stream.aisstream.io/v0/stream",
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print("❌ Fatal error:", e)
        time.sleep(RECONNECT_DELAY)

# =========================
# START
# =========================
if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("🛑 Shutting down...")
        flush_batch()