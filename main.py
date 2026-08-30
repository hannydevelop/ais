import websocket
import os
from dotenv import load_dotenv
import json
import time
from datetime import datetime
from supabase import create_client

load_dotenv()

# =============================================================================
# CONFIG
# =============================================================================

OCEANHELM_RAW_WS = os.getenv(
    "OCEANHELM_RAW_WS",
    "wss://stream.oceanhelmtech.com/ws/decoded"
)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

BATCH_SIZE = 50
FLUSH_INTERVAL = 5
RECONNECT_DELAY = 3

# =============================================================================
# INIT
# =============================================================================

if not SUPABASE_URL:
    raise RuntimeError("SUPABASE_URL is not configured")

if not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_KEY is not configured")

supabase = create_client(
    SUPABASE_URL,
    SUPABASE_KEY
)

position_batch = []

last_flush_time = time.time()

# Prevent processing the exact same MMSI/timestamp twice.
last_seen = {}


# =============================================================================
# AFRICAN VESSEL MMSI CACHE
#
# Cache loaded at startup and refreshed every 10 minutes.
# =============================================================================

african_vessel_mmsi_cache = set()

last_cache_refresh = 0

CACHE_REFRESH_INTERVAL = 600


def refresh_african_vessel_cache():

    global african_vessel_mmsi_cache
    global last_cache_refresh

    try:

        all_mmsi = set()

        page_size = 1000
        start = 0

        while True:

            result = (
                supabase
                .rpc("get_african_vessel_mmsi")
                .range(start, start + page_size - 1)
                .execute()
            )

            if not result.data:
                break

            all_mmsi.update(
                row["mmsi"]
                for row in result.data
            )

            if len(result.data) < page_size:
                break

            start += page_size

        african_vessel_mmsi_cache = all_mmsi

        last_cache_refresh = time.time()

        print(
            f"✅ African vessel cache refreshed: "
            f"{len(african_vessel_mmsi_cache)} vessels"
        )

    except Exception as e:

        print(
            f"❌ Cache refresh error: {e}"
        )


def is_african_owned(mmsi):

    if (
        time.time() - last_cache_refresh
        > CACHE_REFRESH_INTERVAL
    ):
        refresh_african_vessel_cache()

    return mmsi in african_vessel_mmsi_cache


# =============================================================================
# HELPERS
# =============================================================================

def parse_timestamp(ts):

    if not ts:
        return None

    if not isinstance(ts, str):
        return None

    ts = ts.replace(" UTC", "").strip()

    try:

        return datetime.fromisoformat(ts).isoformat()

    except Exception as e:

        print(
            f"Timestamp parse error: "
            f"{e} | raw: {ts}"
        )

        return None


def get_region(lat, lon):

    if lat is None or lon is None:
        return None

    # Port Harcourt / Gulf of Guinea area
    if 4 <= lat <= 6 and 6 <= lon <= 8:
        return "Port Harcourt, Nigeria"

    # Lagos
    if 6 <= lat <= 7 and 3 <= lon <= 4:
        return "Lagos, Nigeria"

    # Africa
    in_africa = (
        -35 <= lon <= 55
        and
        -20 <= lat <= 37
    )

    if in_africa:
        return "Africa"

    return "International Waters"


# =============================================================================
# VESSEL UPSERT
# =============================================================================

def upsert_vessel(
    mmsi,
    imo,
    name,
    vessel_type
):

    vessel_row = {
        "mmsi": mmsi,
        "imo_number": imo,
        "name": name,
        "vessel_type": vessel_type,
        "updated_at": datetime.utcnow().isoformat()
    }

    try:

        supabase \
            .table("vessels") \
            .upsert(
                vessel_row,
                on_conflict="mmsi"
            ) \
            .execute()

    except Exception as e:

        print(
            f"❌ Vessel upsert error "
            f"(mmsi={mmsi}): {e}"
        )


# =============================================================================
# BATCH FLUSH
# =============================================================================

def flush_batch():

    global position_batch
    global last_flush_time

    if not position_batch:
        return

    batch = position_batch

    try:

        (
            supabase
            .table("vessel_positions")
            .insert(batch)
            .execute()
        )

        print(
            f"✅ Flushed "
            f"{len(batch)} positions"
        )

    except Exception as e:

        print(
            f"❌ Batch insert error: {e}"
        )

        # Fallback:
        # Try each row individually so one bad row
        # doesn't destroy the entire batch.

        for row in batch:

            try:

                (
                    supabase
                    .table("vessel_positions")
                    .insert(row)
                    .execute()
                )

            except Exception as row_e:

                print(
                    f"  ❌ Failed row "
                    f"mmsi={row.get('mmsi')}: "
                    f"{row_e}"
                )

    finally:

        position_batch.clear()

        last_flush_time = time.time()


# =============================================================================
# DECODED AIS MESSAGE HANDLER
#
# Node.js has already decoded the NMEA.
#
# Expected structure:
#
# {
#     "receiver_id": "receiver-001",
#     "timestamp": "...",
#     "type": "ais",
#     "ais_message_type": 1,
#     "ais": {
#         "mmsi": 636093408,
#         "lat": 4.6947,
#         "lon": 7.1755,
#         ...
#     }
# }
# =============================================================================

def handle_message(data):

    global position_batch

    if not isinstance(data, dict):
        return

    # -------------------------------------------------------------------------
    # Only accept decoded AIS messages.
    #
    # GPS should never reach this service because Node.js drops GPS before
    # broadcasting.
    # -------------------------------------------------------------------------

    if data.get("type") != "ais":
        return

    ais = data.get("ais")

    if not isinstance(ais, dict):
        return

    # -------------------------------------------------------------------------
    # MMSI
    # -------------------------------------------------------------------------

    mmsi = ais.get("mmsi")

    if not mmsi:
        return

    try:
        mmsi = int(mmsi)
    except (TypeError, ValueError):
        return

    # -------------------------------------------------------------------------
    # Position
    # -------------------------------------------------------------------------

    lat = ais.get("lat")
    lon = ais.get("lon")

    if lat is None or lon is None:
        return

    try:

        lat = float(lat)
        lon = float(lon)

    except (TypeError, ValueError):

        return

    # -------------------------------------------------------------------------
    # African vessel filter
    #
    # Unlike before, this is the ONLY feed we receive.
    #
    # If you want all vessels received by your receivers, remove this gate.
    #
    # For now we preserve your old behavior:
    # only African-owned vessels go into Supabase.
    # -------------------------------------------------------------------------

    if not is_african_owned(mmsi):

        return

    # -------------------------------------------------------------------------
    # Timestamp
    # -------------------------------------------------------------------------

    timestamp = data.get("timestamp")

    # -------------------------------------------------------------------------
    # Deduplication
    #
    # Node.js may aggregate multiple receivers.
    #
    # The same vessel can therefore potentially arrive from multiple
    # receivers at nearly the same time.
    # -------------------------------------------------------------------------

    dedupe_key = (
        mmsi,
        timestamp
    )

    if dedupe_key in last_seen:

        return

    last_seen[dedupe_key] = True

    # -------------------------------------------------------------------------
    # Speed
    # -------------------------------------------------------------------------

    speed = ais.get("speedOverGround")

    if speed is not None:

        try:

            speed = float(speed)

        except (TypeError, ValueError):

            speed = None

    # -------------------------------------------------------------------------
    # Preserve your old rule:
    # ignore vessels moving below 0.3 knots.
    # -------------------------------------------------------------------------

    if speed is not None and speed < 0.3:

        return

    # -------------------------------------------------------------------------
    # Vessel metadata
    #
    # Position AIS messages normally don't contain static data.
    #
    # Node.js may eventually provide static AIS messages separately.
    # -------------------------------------------------------------------------

    name = ais.get("name")
    imo = ais.get("imo")
    vessel_type = ais.get("vesselType")

    # -------------------------------------------------------------------------
    # Vessel metadata update
    # -------------------------------------------------------------------------

    if (
        name is not None
        or imo is not None
        or vessel_type is not None
    ):

        upsert_vessel(
            mmsi=mmsi,
            imo=imo,
            name=name,
            vessel_type=vessel_type
        )

    # -------------------------------------------------------------------------
    # Create Supabase position record
    # -------------------------------------------------------------------------

    position_batch.append({

        "mmsi": mmsi,

        "lat": lat,

        "lon": lon,

        "speed": speed,

        "course": ais.get(
            "courseOverGround"
        ),

        "location_name": get_region(
            lat,
            lon
        ),

        "timestamp": parse_timestamp(
            timestamp
        )
    })


# =============================================================================
# WEBSOCKET CONNECTION
#
# Python now connects to OceanHelm.
#
# Node.js is responsible for:
#   - receiving receiver streams
#   - dropping GPS
#   - decoding AIS
#   - aggregating
#   - broadcasting decoded AIS
# =============================================================================

def make_ws():

    def on_open(ws):

        print(
            "🌊 Connected to OceanHelm "
            "decoded AIS stream"
        )

        print(
            f"Stream: {OCEANHELM_RAW_WS}"
        )

    def on_message(ws, message):

        global last_flush_time

        try:

            data = json.loads(
                message
            )

            handle_message(data)

        except json.JSONDecodeError as e:

            print(
                f"⚠️ Invalid JSON from "
                f"OceanHelm stream: {e}"
            )

            return

        except Exception as e:

            print(
                f"⚠️ Message processing error: "
                f"{e}"
            )

            return

        # ---------------------------------------------------------------------
        # Flush based on size OR time
        # ---------------------------------------------------------------------

        if (
            len(position_batch) >= BATCH_SIZE
            or
            (
                time.time()
                - last_flush_time
                > FLUSH_INTERVAL
            )
        ):

            flush_batch()

    def on_error(ws, error):

        print(
            f"⚠️ OceanHelm WebSocket error: "
            f"{error}"
        )

    def on_close(ws, code, msg):

        print(
            f"🔌 OceanHelm stream closed "
            f"({code}): {msg}"
        )

        print(
            f"Reconnecting in "
            f"{RECONNECT_DELAY}s..."
        )

    return websocket.WebSocketApp(

        OCEANHELM_RAW_WS,

        on_open=on_open,

        on_message=on_message,

        on_error=on_error,

        on_close=on_close
    )


# =============================================================================
# RECONNECT RUNNER
# =============================================================================

def run_connection():

    while True:

        try:

            ws = make_ws()

            ws.run_forever(
                ping_interval=30,
                ping_timeout=10
            )

        except Exception as e:

            print(
                f"❌ Fatal OceanHelm "
                f"stream error: {e}"
            )

        time.sleep(
            RECONNECT_DELAY
        )


# =============================================================================
# START
# =============================================================================

if __name__ == "__main__":

    print(
        "========================================"
    )

    print(
        "OceanHelm AIS Processing Worker"
    )

    print(
        "========================================"
    )

    print(
        f"Input: {OCEANHELM_RAW_WS}"
    )

    print(
        f"Batch size: {BATCH_SIZE}"
    )

    print(
        f"Flush interval: {FLUSH_INTERVAL}s"
    )

    print()

    # -------------------------------------------------------------------------
    # Load African vessel cache BEFORE connecting.
    # -------------------------------------------------------------------------

    refresh_african_vessel_cache()

    # -------------------------------------------------------------------------
    # Connect to OceanHelm.
    # -------------------------------------------------------------------------

    try:

        run_connection()

    except KeyboardInterrupt:

        print(
            "\n🛑 Shutting down..."
        )

        flush_batch()