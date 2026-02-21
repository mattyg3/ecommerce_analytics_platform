import json
import uuid
import random
import math
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import os
import signal

# ----------------------------------------
# Configuration
# ----------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
# CLICKSTREAM_DIR = BASE_DIR / "data" / "clickstream" / "raw"
CLICKSTREAM_DIR = Path("/home/surff/spark_data/clickstream/raw") #WSL path
CLICKSTREAM_DIR.mkdir(parents=True, exist_ok=True)
# ORDERS_DIR = BASE_DIR /  "data" / "orders" / "raw"
ORDERS_DIR = Path("/home/surff/spark_data/orders/raw") #WSL path
ORDERS_DIR.mkdir(parents=True, exist_ok=True)
PRODUCTS = BASE_DIR / "data" / "products.json"

STOP_FILE = BASE_DIR / "control" / "clickstream.stop"
STOP_FILE.parent.mkdir(parents=True, exist_ok=True)
stop_requested = False

def handle_stop_signal(signum, frame):
    global stop_requested
    stop_requested = True
    print("⚠️ Stop signal received. Ending generator gracefully...")

signal.signal(signal.SIGINT, handle_stop_signal)
signal.signal(signal.SIGTERM, handle_stop_signal)

BATCH_INTERVAL_SECONDS = 2         # interval between batches (simulated)
SIMULATION_HOURS = int(os.getenv("SIMULATION_HOURS", 24))  # default 24h if not set
TIME_MULTIPLIER = 60               # 1 real second = 1 simulated minute

EVENT_TYPES = ["page_view", "view_product", "add_to_cart", "checkout_start", "purchase"]
ORDER_STATUSES = ["completed", "cancelled"] #"pending", 
DEVICE_PROFILES = {
    "mobile": {
        "user_agents": ["Mozilla/5.0 (iPhone)", "Mozilla/5.0 (Android)"],
        "countries": ["US", "CA", "MX", "BR", "AR"]
    },
    "desktop": {
        "user_agents": ["Mozilla/5.0 (Windows NT 10.0)"],
        "countries": ["US", "CA", "GB", "DE", "FR"]
    },
    "tablet": {
        "user_agents": ["Mozilla/5.0 (iPad)"],
        "countries": ["US", "GB"]
    }
}
REFERRERS = ["google.com", "facebook.com", "email_campaign", None]
EXPERIMENTS = [None, "checkout_redesign", "pricing_test"]

FUNNEL_STEP_PROB = {
    "view_product":0.95,
    "add_to_cart":0.25,
    "checkout_start":0.6,
    "purchase":0.80,
}

FUNNEL_STEP_PROB_RETURNING = {
    "view_product":0.95,
    "add_to_cart": 0.45,
    "checkout_start": 0.75,
    "purchase": 0.90
}

LATE_EVENT_PROB = 0.15
LATE_EVENT_MAX_DELAY = 10 #minutes
MAX_SESSION_SECONDS = 1800  # 30 minutes

RETURNING_USER_PROB = 0.3
MAX_KNOWN_USERS = 50000
known_users = []

SESSION_SPLIT_PROB = 0.2


def load_products(filename=PRODUCTS):
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)
PRODUCTS = load_products()
PRODUCT_INDEX = {p["product_id"]: p for p in PRODUCTS}
# ----------------------------------------
# Helper Functions
# ----------------------------------------
def get_user_id():
    global known_users
    # Returning user
    if known_users and random.random() < RETURNING_USER_PROB:
        return random.choice(known_users), True
    # New user
    user_id = str(uuid.uuid4())
    known_users.append(user_id)
    # Prevent unbounded growth
    if len(known_users) > MAX_KNOWN_USERS:
        known_users = known_users[-MAX_KNOWN_USERS:]
    return user_id, False

def maybe_new_session(current_session_id):
    if random.random() < SESSION_SPLIT_PROB:
        return str(uuid.uuid4())
    return current_session_id

def event_delay(scale=30, max_delay=14400):
    """
    Return a realistic delay in seconds using an exponential-like distribution.

    scale: controls the "typical" delay (mean ~ scale seconds)
    max_delay: maximum allowed delay in seconds (default 4 hours)
    """
    u = random.random()
    delay_seconds = -scale * math.log(1 - u) # 1-u to avoid ln(0)
    delay_seconds = min(delay_seconds, max_delay) # Clip to maximum delay
    return delay_seconds

def maybe_force_late(event_time):
    """
    With some probability, shift event_time backwards
    while keeping ingest_time near simulated_now.
    """
    if random.random() < LATE_EVENT_PROB:
        delay_minutes = random.randint(1, LATE_EVENT_MAX_DELAY)
        return event_time - timedelta(minutes=delay_minutes)
    return event_time

def generate_event(event_type, session_dict, product_id=None, simulated_now=None, session_time=None):
    if simulated_now is None:
        simulated_now = datetime.now(timezone.utc)

    #Network jitter
    ingest_delay = random.randint(0,20) #seconds
    ingest_time = simulated_now + timedelta(seconds=ingest_delay)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "version": session_dict["version"],
        "user_id": session_dict["user_id"],
        "session_id": session_dict["session_id"],
        "product_id": product_id,
        "event_time": session_time.isoformat(),
        "ingest_time": ingest_time.isoformat(),
        "device": session_dict["device"],
        "country": session_dict["country"],
    }

    if session_dict["version"] == 2:
        event.update({
            "user_agent": session_dict["user_agent"],
            "referrer": session_dict["referrer"],
            "experiment_id": session_dict["experiment_id"],
        })

    return event

def generate_session(simulated_now=None):
    if simulated_now is None:
        simulated_now = datetime.now(timezone.utc)
    user_id, is_returning = get_user_id()
    session_time = simulated_now - timedelta(seconds=random.randint(30, 90))
    session_start = session_time
    device_type = random.choices(list(DEVICE_PROFILES.keys()), [0.7, 0.25, 0.05], k=1)[0] #mobile: 70%, desktop: 25%, tablet: 5%
    profile = DEVICE_PROFILES[device_type]
    session_dict = {
        "version": 2 if random.random() < 0.3 else 1, #30% are version 2
        "user_id": user_id,
        "session_id": str(uuid.uuid4()),
        "device": device_type,
        "country": random.choice(profile["countries"])
    }
    if session_dict["version"] == 2:
        session_dict.update({
            "user_agent": random.choice(profile["user_agents"]),
            "referrer": random.choice(REFERRERS),
            "experiment_id": random.choice(EXPERIMENTS)
        })
    funnel_probs = (FUNNEL_STEP_PROB_RETURNING if is_returning else FUNNEL_STEP_PROB)
    # Returning users are quicker
    min_gap, max_gap = (5, 45) if is_returning else (10, 90)

    def advance_time(current_time):
        """
        Advance simulated event time
        """
        return current_time + timedelta(seconds=random.randint(min_gap, max_gap))
    
    events = []

    def emit(event_type, product_id=None):
        nonlocal session_time, session_dict, simulated_now, session_start
        session_dict["session_id"] = maybe_new_session(session_dict["session_id"])
        session_time = advance_time(session_time)
        if (session_time-session_start).total_seconds() > MAX_SESSION_SECONDS:
            # session_dict["session_id"] = str(uuid.uuid4())
            # session_start = session_time
            return #abandoned session
        true_event_time = maybe_force_late(session_time)
        events.append(generate_event(event_type, session_dict, product_id, simulated_now, true_event_time))
    
    def conversion_multiplier(hour):
        if 0 <= hour < 6:
            return 0.6
        elif 18 <= hour < 22:
            return 1.2
        return 1.0
    
    try:
        emit("page_view")
        # only attach referrer to initial page view
        if session_dict["version"] == 2:
            session_dict["referrer"] = None 
        num_products = random.randint(1,5)
        products = random.sample(PRODUCTS, num_products)
        order_generated = False
        ordered_products = []
        order_session_id = None

        for product in products:
            #View Product
            if random.random() > funnel_probs["view_product"]:
                continue
            emit("view_product", product["product_id"])
            #Add to Cart with dynamic prob
            price = PRODUCT_INDEX[product["product_id"]]["price_usd"]
            price_factor = min(1.0, 50 / price)
            effective_add_to_cart = funnel_probs["add_to_cart"] * price_factor
            if random.random() > effective_add_to_cart:
                continue
            emit("add_to_cart", product["product_id"])
            ordered_products.append(product["product_id"])
            #Start Checkout
            if random.random() > funnel_probs["checkout_start"]:
                continue
            emit("checkout_start", product["product_id"])
            #Purchase with dynamic prob
            conversion_factor = conversion_multiplier(session_time.hour)
            if random.random() < funnel_probs["purchase"] * conversion_factor:
                session_time = session_time + timedelta(seconds=random.randint(90, 220)) #extra long wait for purchase
                true_event_time = maybe_force_late(session_time)
                events.append(generate_event("purchase", session_dict, product["product_id"], simulated_now, true_event_time))
                order_generated = True
                order_session_id = session_dict["session_id"]
    except StopIteration:
        pass

    if not order_generated:
        ordered_products = []

    return session_dict, events, order_generated, ordered_products, order_session_id

def generate_order(session_dict, ordered_products, order_session_id, simulated_now=None):
    if simulated_now is None:
        simulated_now = datetime.now(timezone.utc)
    order_time = simulated_now - timedelta(seconds=event_delay(scale=10, max_delay=900))
    items_ordered = []
    for product_id in ordered_products:
        item = {
            "product_id": product_id,
            "quantity": random.randint(1,6),
            "price": PRODUCT_INDEX[product_id]["price_usd"],
        }
        items_ordered.append(item)
    
    if random.random() < 0.97:
        status = "completed"
    else:
        status = "cancelled"

    return {
        "session_id": order_session_id,
        "order_id": str(uuid.uuid4()),
        "user_id": session_dict["user_id"],
        "items": items_ordered,
        "order_status": status,
        "order_time": order_time.isoformat(),
        "ingest_time": simulated_now.isoformat()
    }

def write_events(events, directory, filename_prefix):
    timestamp_ms = int(time.time() * 1000)
    filename = directory / f"{filename_prefix}_{timestamp_ms}.json"
    with open(filename, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

def sessions_per_batch(sim_hour):
    if 0 <= sim_hour < 6:
        return random.randint(2, 5)
    elif 6 <= sim_hour < 12:
        return random.randint(5, 15)
    elif 12 <= sim_hour < 18:
        return random.randint(15, 30)
    else:
        return random.randint(10, 20)

# ----------------------------------------
# MAIN
# ----------------------------------------
if __name__ == "__main__":
    start_time = time.time()
    end_time = start_time + SIMULATION_HOURS * 3600 / TIME_MULTIPLIER
    print(f"Starting simulated clickstream & orders for {SIMULATION_HOURS} hours...")

    while time.time() < end_time  and not stop_requested:
        # Check for stop signal at the start of each loop
        if STOP_FILE.exists():
            print("🛑 Stop file detected. Exiting generator gracefully.")
            break

        simulated_seconds = (time.time() - start_time) * TIME_MULTIPLIER
        simulated_now = datetime.now(timezone.utc) + timedelta(seconds=simulated_seconds)
        num_sessions = sessions_per_batch(simulated_now.hour)

        batch_clickstream = []
        batch_orders = []
        for _ in range(num_sessions):
            session_dict, session_events, order_generated, ordered_products, order_session_id = generate_session(simulated_now)
            batch_clickstream.extend(session_events)
            if order_generated:
                batch_orders.append(generate_order(session_dict, ordered_products, order_session_id, simulated_now))

        #Add duplicates
        if random.random() < 0.05 and batch_clickstream:
            batch_clickstream.append(random.choice(batch_clickstream))
        if random.random() < 0.02 and batch_orders:
            batch_orders.append(random.choice(batch_orders))

        write_events(batch_clickstream, CLICKSTREAM_DIR, "clickstream")
        write_events(batch_orders, ORDERS_DIR, "orders")
        print(f"Wrote Clickstream: {len(batch_clickstream)} and Orders: {len(batch_orders)} for simulated {simulated_now.hour}:{simulated_now.minute}:{simulated_now.second}")

        # Sleep scaled by TIME_MULTIPLIER
        time.sleep(BATCH_INTERVAL_SECONDS / TIME_MULTIPLIER)

    print("Simulation complete!")