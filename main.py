import websocket
import os
from dotenv import load_dotenv
import json
import time
from datetime import datetime, timezone
from supabase import create_client
import threading

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

OCEANHELM_WS_URL = os.getenv(
    "OCEANHELM_WS_URL",
    "ws://127.0.0.1:9000/ws/stream"
)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

BATCH_SIZE = 50
FLUSH_INTERVAL = 5
RECONNECT_DELAY = 3

# OceanHelm Africa bounding box
#
# [west, south, east, north]
#
# west  = -35
# south = -20
# east  = 55
# north = 37
#
AFRICA_BBOX = [-35, -20, 55, 37]

AIS_MESSAGE_TYPES = [
    1,
    2,
    3,
    4,
    5,
    9,
    18,
    19,
    21,
    24
]

# ============================================================
# INIT
# ============================================================

supabase = create_client(
    SUPABASE_URL,
    SUPABASE_KEY
)

position_batch = []

last_flush_time = time.time()

last_seen = {}

batch_lock = threading.Lock()

# ============================================================
# AFRICAN VESSEL MMSI CACHE
#
# Cache loaded at startup and refreshed every 10 minutes.
# ============================================================

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
                .range(
                    start,
                    start + page_size - 1
                )
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
            "✅ African vessel cache refreshed: "
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


# ============================================================
# HELPERS
# ============================================================

def parse_timestamp(ts):

    if not ts:
        return None

    try:

        if isinstance(ts, str):

            ts = ts.replace(
                " UTC",
                ""
            ).strip()

            return datetime.fromisoformat(
                ts.replace(
                    "Z",
                    "+00:00"
                )
            ).isoformat()

        return datetime.now(
            timezone.utc
        ).isoformat()

    except Exception as e:

        print(
            "Timestamp parse error:",
            e,
            "| raw:",
            ts
        )

        return None


def get_region(lat, lon):

    if (
        4 <= lat <= 6
        and 6 <= lon <= 8
    ):
        return "Port Harcourt, Nigeria"

    if (
        6 <= lat <= 7
        and 3 <= lon <= 4
    ):
        return "Lagos, Nigeria"

    in_africa = (
        -35 <= lon <= 55
        and
        -20 <= lat <= 37
    )

    return (
        "Africa"
        if in_africa
        else
        "International Waters"
    )


# ============================================================
# VESSEL UPSERT
# ============================================================

def upsert_vessel(
    mmsi,
    imo=None,
    name=None,
    vessel_type=None
):

    vessel_row = {
        "mmsi": mmsi,
        "imo_number": imo,
        "name": name,
        "vessel_type": vessel_type,
        "updated_at":
            datetime.now(
                timezone.utc
            ).isoformat()
    }

    try:

        (
            supabase
            .table("vessels")
            .upsert(
                vessel_row,
                on_conflict="mmsi"
            )
            .execute()
        )

    except Exception as e:

        print(
            "Vessel upsert error:",
            e
        )


# ============================================================
# BATCH FLUSH
# ============================================================

def flush_batch():

    global position_batch
    global last_flush_time

    with batch_lock:

        if not position_batch:
            return

        batch = position_batch.copy()

        position_batch.clear()

        last_flush_time = time.time()

    try:

        (
            supabase
            .table("vessel_positions")
            .insert(batch)
            .execute()
        )

        print(
            f"✅ Flushed {len(batch)} positions"
        )

    except Exception as e:

        print(
            f"❌ Batch insert error: {e}"
        )

        # Retry individually.
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
                    "  ❌ Failed row "
                    f"mmsi={row.get('mmsi')}: "
                    f"{row_e}"
                )


# ============================================================
# CORE OCEANHELM MESSAGE HANDLER
# ============================================================

def handle_message(data):

    global position_batch

    if not isinstance(data, dict):
        return

    # --------------------------------------------------------
    # Ignore OceanHelm control messages
    # --------------------------------------------------------

    if data.get("type") != "ais":
        return

    # --------------------------------------------------------
    # MMSI
    # --------------------------------------------------------

    mmsi = data.get("mmsi")

    if not mmsi:
        return

    try:
        mmsi = int(mmsi)
    except Exception:
        return

    # --------------------------------------------------------
    # Position
    # --------------------------------------------------------

    position = data.get(
        "position"
    )

    if not position:
        return

    lat = position.get("lat")
    lon = position.get("lon")

    if lat is None or lon is None:
        return

    try:

        lat = float(lat)
        lon = float(lon)

    except Exception:

        return

    # --------------------------------------------------------
    # African-owned gate
    #
    # OceanHelm already applies the Africa bbox.
    #
    # This gate is retained because your existing pipeline
    # deliberately stores only African-owned vessels for this
    # processing path.
    # --------------------------------------------------------

    if not is_african_owned(mmsi):

        return

    # --------------------------------------------------------
    # Timestamp
    # --------------------------------------------------------

    timestamp = data.get(
        "timestamp"
    )

    timestamp = parse_timestamp(
        timestamp
    )

    # --------------------------------------------------------
    # Duplicate protection
    # --------------------------------------------------------

    duplicate_key = (
        timestamp
        if timestamp
        else f"{lat}:{lon}"
    )

    if (
        mmsi in last_seen
        and
        last_seen[mmsi] == duplicate_key
    ):

        return

    last_seen[mmsi] = duplicate_key

    # --------------------------------------------------------
    # Navigation
    # --------------------------------------------------------

    navigation = data.get(
        "navigation"
    ) or {}

    speed = navigation.get(
        "speed"
    )

    course = navigation.get(
        "course"
    )

    # --------------------------------------------------------
    # Existing behaviour:
    # Ignore vessels moving slower than 0.3 knots.
    # --------------------------------------------------------

    if speed is not None:

        try:

            speed = float(speed)

            if speed < 0.3:
                return

        except Exception:

            speed = None

    # --------------------------------------------------------
    # Vessel information
    # --------------------------------------------------------

    vessel = data.get(
        "vessel"
    ) or {}

    name = vessel.get(
        "name"
    )

    imo = vessel.get(
        "imo"
    )

    vessel_type = vessel.get(
        "vessel_type"
    )

    # --------------------------------------------------------
    # Upsert vessel master record
    # --------------------------------------------------------

    upsert_vessel(
        mmsi=mmsi,
        imo=imo,
        name=name,
        vessel_type=vessel_type
    )

    # --------------------------------------------------------
    # Queue position
    # --------------------------------------------------------

    row = {
        "mmsi": mmsi,
        "lat": lat,
        "lon": lon,
        "speed": speed,
        "course": course,
        "location_name":
            get_region(
                lat,
                lon
            ),
        "timestamp": timestamp
    }

    with batch_lock:

        position_batch.append(
            row
        )

        batch_size = len(
            position_batch
        )

    # --------------------------------------------------------
    # Flush conditions
    # --------------------------------------------------------

    if (
        batch_size >= BATCH_SIZE
        or
        (
            time.time()
            - last_flush_time
            > FLUSH_INTERVAL
        )
    ):

        flush_batch()


# ============================================================
# OCEANHELM WEBSOCKET
# ============================================================

def make_ws():

    def on_open(ws):

        print(
            "🌊 OceanHelm AIS connection open"
        )

        payload = {
            "action": "subscribe",

            "bbox": AFRICA_BBOX,

            "message_types":
                AIS_MESSAGE_TYPES
        }

        ws.send(
            json.dumps(
                payload
            )
        )

        print(
            "📡 Subscribed to OceanHelm "
            "African AIS stream"
        )

    def on_message(
        ws,
        message
    ):

        try:

            data = json.loads(
                message
            )

            # --------------------------------------------
            # Connection acknowledgement
            # --------------------------------------------

            if data.get(
                "type"
            ) == "connection_ack":

                print(
                    "✅ OceanHelm stream "
                    "connection acknowledged"
                )

                return

            # --------------------------------------------
            # Subscription acknowledgement
            # --------------------------------------------

            if data.get(
                "type"
            ) == "subscription":

                print(
                    "✅ OceanHelm subscription:",
                    data
                )

                return

            # --------------------------------------------
            # AIS
            # --------------------------------------------

            handle_message(
                data
            )

        except Exception as e:

            print(
                "Message error:",
                e
            )

    def on_error(
        ws,
        error
    ):

        print(
            "⚠️ OceanHelm WebSocket error:",
            error
        )

    def on_close(
        ws,
        code,
        msg
    ):

        print(
            "🔌 OceanHelm connection closed "
            f"({code}): {msg}"
        )

        print(
            f"Reconnecting in "
            f"{RECONNECT_DELAY}s..."
        )

    return websocket.WebSocketApp(

        OCEANHELM_WS_URL,

        on_open=on_open,

        on_message=on_message,

        on_error=on_error,

        on_close=on_close
    )


# ============================================================
# RECONNECT RUNNER
# ============================================================

def run_connection():

    while True:

        try:

            print(
                f"🔗 Connecting to "
                f"{OCEANHELM_WS_URL}"
            )

            ws = make_ws()

            ws.run_forever(
                ping_interval=30,
                ping_timeout=10
            )

        except Exception as e:

            print(
                "❌ Fatal OceanHelm "
                f"WebSocket error: {e}"
            )

        time.sleep(
            RECONNECT_DELAY
        )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    print(
        "🚢 OceanHelm AIS Python pipeline"
    )

    print(
        f"WebSocket: {OCEANHELM_WS_URL}"
    )

    # --------------------------------------------------------
    # Pre-load African vessel cache
    # --------------------------------------------------------

    refresh_african_vessel_cache()

    # --------------------------------------------------------
    # Start OceanHelm stream
    # --------------------------------------------------------

    stream_thread = threading.Thread(
        target=run_connection,
        daemon=True
    )

    stream_thread.start()

    # --------------------------------------------------------
    # Keep process alive
    # --------------------------------------------------------

    try:

        while True:

            time.sleep(1)

    except KeyboardInterrupt:

        print(
            "🛑 Shutting down..."
        )

        flush_batch()