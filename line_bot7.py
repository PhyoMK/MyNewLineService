from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage, PostbackEvent
import os, requests, re, sqlite3, time

import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

logging.info("Starting LINE bot...")


app = Flask(__name__)

# Env variables
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN")
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET")
POWERAPP_FLOW_URL = os.getenv("POWERAPP_FLOW_URL")
ACKNOWLEDGE_FLOW_URL = os.getenv("ACKNOWLEDGE_FLOW_URL")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

# DB_PATH = "database.db"  # Use a relative path for SQLite database
DB_PATH = "/home/site/wwwroot/database.db" 

# In-memory cache { user_id: {"display_name": str, "last_record_id": int or None} }
user_cache = {}


# ---------- Load cache from SQLite ----------
def load_cache_from_db():
    global user_cache
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT user_id, display_name, last_record_id, last_acknowledge FROM users")
    rows = c.fetchall()
    conn.close()
    user_cache = {r[0]: {"display_name": r[1], "last_record_id": r[2], "last_acknowledge": r[3]} for r in rows}
    logging.info(f"Cache loaded with {len(user_cache)} users.")

# ---------- SQLite helper functions ----------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            display_name TEXT,
            last_record_id INTEGER,
            last_acknowledge TEXT
        )
    ''')
    columns = {row[1] for row in c.execute("PRAGMA table_info(users)")}
    if "last_acknowledge" not in columns:
        c.execute("ALTER TABLE users ADD COLUMN last_acknowledge TEXT")
    conn.commit()
    conn.close()

def add_user(user_id, display_name):
    # Update cache
    user_cache[user_id] = {"display_name": display_name, "last_record_id": None, "last_acknowledge": None}

    # Write only once for new users
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        'INSERT OR IGNORE INTO users (user_id, display_name) VALUES (?, ?)',
        (user_id, display_name)
    )
    conn.commit()
    conn.close()

def get_display_name(user_id):
    # Always use cache
    return user_cache.get(user_id, {}).get("display_name")


def update_last_record_id(user_id, record_id):
    # Update cache only
    if user_id in user_cache:
        user_cache[user_id]["last_record_id"] = record_id

    # No DB update needed unless you want persistence
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        'UPDATE users SET last_record_id = ? WHERE user_id = ?',
        (record_id, user_id)
    )
    conn.commit()
    conn.close()

def get_last_record_id(user_id):
    return user_cache.get(user_id, {}).get("last_record_id")


def update_last_acknowledge(user_id, acknowledge):
    if user_id in user_cache:
        user_cache[user_id]["last_acknowledge"] = acknowledge

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "UPDATE users SET last_acknowledge = ? WHERE user_id = ?",
        (acknowledge, user_id)
    )
    conn.commit()
    conn.close()


def get_last_acknowledge(user_id):
    return user_cache.get(user_id, {}).get("last_acknowledge")
@app.route("/delete_all_users", methods=['GET', 'DELETE'])
def delete_all_users():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('DELETE FROM users')
    conn.commit()
    conn.close()
    return 

@app.route("/list_users", methods=['GET'])
def list_users():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT * FROM users')
    rows = c.fetchall()
    conn.close()
    # Return ID, display name, and last record ID
    return "<br>".join([f"{r[0]} - {r[1]} - Last Record ID: {r[2] if r[2] is not None else 'None'}" for r in rows])


# Initialize DB at startup
init_db()
load_cache_from_db()

def send_to_powerapp(user_id, display_name, feedback, record_id, feedbacktxt, list_type):
    return requests.post(POWERAPP_FLOW_URL, json={
        "userId": user_id,
        "displayName": display_name,
        "feedback": feedback,
        "recordId": record_id,
        "feedbacktxt": feedbacktxt,
        "list": list_type
    })


def send_to_acknowledge(user_id, display_name, record_id, acknowledge, list_type, comment):
    return requests.post(ACKNOWLEDGE_FLOW_URL, json={
        "userId": user_id,
        "displayName": display_name,
        "recordId": record_id,
        "acknowledge": acknowledge,
        "list": list_type,
        "comment": comment
    }, timeout=10)


@app.route("/webhook", methods=['POST'])
def webhook():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'


@app.route("/health", methods=['GET'])
def health():
    return "OK", 200

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    global user_cache
    
    # Ensure cache is loaded
    if not user_cache:  # cache empty → reload from DB
        logging.info("Cache empty, reloading from DB...")
        load_cache_from_db()
    user_id = event.source.user_id
    msg = event.message.text
        

    display_name = get_display_name(user_id)
    if not display_name:
        # New user
        profile = line_bot_api.get_profile(user_id)
        add_user(user_id, profile.display_name)
        display_name = profile.display_name
        send_to_powerapp(user_id, display_name, 0, 0, "", "")
        logging.info(f"{user_id}, {display_name}, New user added")
        return

    if msg.lower() == "hello":
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"Hello {display_name}!"))
        return

    for prefix in ['Service Feedback :', 'Action Feedback :']:
        if prefix in msg:
            list_type = 'service' if 'Service' in prefix else 'action'
            match = re.search(r':\s*(.+)', msg)
            record_id = get_last_record_id(user_id)
            if match:                
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Thanks for your feedback!"))
                logging.info(f"{user_id}, {display_name}, 0, {record_id}, {match.group(1)}, {list_type}")
                send_to_powerapp(user_id, display_name, 0, record_id, match.group(1), list_type)
            return
    
    for prefix in ['Action Comment :', 'Service Comment :']:
        if prefix in msg:
            list_type = 'service' if 'Service' in prefix else 'action'
            match = re.search(r':\s*(.+)', msg)
            record_id = get_last_record_id(user_id)
            if match:                
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Thanks for your feedback!"))
                logging.info(f"{user_id}, {display_name}, 0, {record_id}, {match.group(1)}, {list_type}")
                send_to_acknowledge(
                    user_id,
                    display_name,
                    record_id,
                    get_last_acknowledge(user_id),
                    list_type,
                    match.group(1)
                )
            return

@handler.add(PostbackEvent)
def handle_postback(event):
    user_id = event.source.user_id
    display_name = get_display_name(user_id)
    data = event.postback.data.strip()

    logging.info(f"Postback received: {data}")

    # ----------------------------------------
    # ACKNOWLEDGE
    # ----------------------------------------

    ack_match = re.search(
        r'^(Action|Service) Acknowledge\s*:\s*(Yes|No)\s*\(\s*ID\s*:\s*(\d+)\s*\)$',
        data,
        re.IGNORECASE
    )

    if ack_match:
        list_type = ack_match.group(1).lower()
        acknowledge = ack_match.group(2).capitalize()
        record_id = int(ack_match.group(3))

        logging.info(
            f"{user_id}, {display_name}, "
            f"{acknowledge}, {record_id}, {list_type}"
        )

        # Reply to LINE
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(
                text=f"Thank you for your acknowledgement."
            )
        )

        update_last_record_id(user_id, record_id)
        update_last_acknowledge(user_id, acknowledge)
        
        # Send to acknowledge Power Automate flow
        response = send_to_acknowledge(
            user_id,
            display_name,
            record_id,
            acknowledge,
            list_type,
            "-"
        )

        logging.info(
            f"Acknowledge flow response: "
            f"{response.status_code} {response.text}"
        )

        return

    # ----------------------------------------
    # NORMAL FEEDBACK
    # ----------------------------------------

    data_lower = data.lower()

    list_type = (
        "service"
        if data_lower.startswith("service feedback")
        else "action"
        if data_lower.startswith("action feedback")
        else None
    )

    if not list_type:
        return

    match = re.search(
        r'^(?:Action|Service) Feedback\s*:\s*(\d+)\s*\(\s*ID\s*:\s*(\d+)\s*\)$',
        data,
        re.IGNORECASE
    )

    if match:
        feedback = int(match.group(1))
        record_id = int(match.group(2))

        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(
                text=f"Thanks for your feedback: {feedback}/5!"
            )
        )

        logging.info(
            f"{user_id}, {display_name}, "
            f"{feedback}, {record_id}, {list_type}"
        )

        update_last_record_id(user_id, record_id)

        send_to_powerapp(
            user_id,
            display_name,
            feedback,
            record_id,
            "-",
            list_type
        )

        return

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))