from bs4 import BeautifulSoup
import urllib.request
from hashlib import sha256
from redis import StrictRedis
import tweepy
from telegram.ext import Updater, CommandHandler

from conf import REDIS_HOST, REDIS_DB, REDIS_PORT, TELEGRAM_API_TOKEN

# Some useful constants
R_TELEGRAM_USER_LIST = "ACTCOVID:USERS"
R_MONITOR_SITE_LIST = "ACTCOVID:MONITOR"
R_CLOSE_SITE_LIST = "ACTCOVID:CLOSE"
R_CASUAL_SITE_LIST = "ACTCOVID:CASUAL"
R_UPDATE_TIME = "ACTCOVID:UPDATE"

# The message headers
EXPOSURE_MONITOR = "ℹ️ Monitor for Symptoms".upper()
EXPOSURE_CLOSE = "☣️ Close Contact".upper()
EXPOSURE_CASUAL = "😷 Casual Contact".upper()

# Webpage extraction details.
URL = "https://www.covid19.act.gov.au/act-status-and-response/act-covid-19-exposure-locations"
CLOSE_TABLE_ID = 'table14458'
CASUAL_TABLE_ID = 'table66547'
MONITOR_TABLE_ID = 'table04293'
HASH_FIELDS =  ["suburb", "place", "date", "arrival time", "departure time"]


# Telegram Interface
telegram_updater = Updater(token=TELEGRAM_API_TOKEN, use_context=True)
telegram_dispatcher = telegram_updater.dispatcher

# Redis interface
redis = StrictRedis(REDIS_HOST, REDIS_PORT, REDIS_DB)

def start(update, context):
    # Add user to our list.
    redis.sadd(R_TELEGRAM_USER_LIST, update.effective_chat.id)
    context.bot.send_message(chat_id=update.effective_chat.id, text="Thanks! Please note this information is provided on a best effort basis!")
start_handler = CommandHandler('start', start)
telegram_dispatcher.add_handler(start_handler)

def stop(update, context):
    # Remove user from our list.
    redis.srem(R_TELEGRAM_USER_LIST, update.effective_chat.id)
    context.bot.send_message(chat_id=update.effective_chat.id, text="Thanks!")
stop_handler = CommandHandler('stop', stop)
telegram_dispatcher.add_handler(stop_handler)

def build_message(row_dict, exposure_type):
    return (
        f"{exposure_type}:{row_dict['suburb'].upper()}\n"
        f"{row_dict['date']} @ {row_dict['place'].strip()}\n"
        f"Between {row_dict['arrival time']} and {row_dict['departure time']}\n"
    )

def get_all_cells(in_soup, query_str):
    return [i.get_text().strip().replace("\n","") for i in in_soup.find_all(query_str)]

def get_all_rows(in_soup):
    # First row is skipped as header
    all_rows = in_soup.find_all('tr')

    headers_soup = all_rows[0]
    headers = get_all_cells(headers_soup, "th")

    # remove case sensitivity
    headers = [h.lower() for h in headers]

    data_soup = all_rows[1:]

    all_data = []

    for row_soup in data_soup:
        this_row = get_all_cells(row_soup, "td")

        this_row_dict = dict(zip(headers, this_row))
        all_data.append(this_row_dict)

    return all_data

def hash_row(row_dict, fields):
    hash_str = "".join([row_dict.get(k) for k in fields])
    hash = sha256(hash_str.encode()).hexdigest()
    return hash

def do_update():
    with urllib.request.urlopen(URL) as response:
        raw_html = response.read()

    soup = BeautifulSoup(raw_html, features="html.parser")

    strong_soup = soup.find("strong")
    for s in strong_soup:
        if s.startswith("Page last updated"):
            update_time = s.split("updated:")[1].strip()


    previous_time = redis.get(R_UPDATE_TIME)
    print(previous_time, update_time)
    if previous_time == update_time:
        print("Done")


    close_table_html = soup.find("table", { "id" : CLOSE_TABLE_ID })
    casual_table_html = soup.find("table", { "id" : CASUAL_TABLE_ID })
    monitor_table_html = soup.find("table", { "id" : MONITOR_TABLE_ID })

    close_data = get_all_rows(close_table_html)
    casual_data = get_all_rows(casual_table_html)
    monitor_data = get_all_rows(monitor_table_html)

    def send_to_members(message):
        all_users = redis.smembers(R_TELEGRAM_USER_LIST)

        for user in all_users:
            print(user, message)
            #telegram_updater.bot.send_message(int(user), message)

    for row in close_data:
        row_hash = hash_row(row, HASH_FIELDS)
        if redis.sismember(R_CLOSE_SITE_LIST, row_hash):
            pass
        else:
            send_to_members(build_message(row, EXPOSURE_CLOSE))
            redis.sadd(R_CLOSE_SITE_LIST, row_hash)

    for row in casual_data:
        row_hash = hash_row(row, HASH_FIELDS)
        if redis.sismember(R_CASUAL_SITE_LIST, row_hash):
            pass
        else:
            send_to_members(build_message(row, EXPOSURE_CASUAL))
            redis.sadd(R_CASUAL_SITE_LIST, row_hash)

    for row in monitor_data:
        row_hash = hash_row(row, HASH_FIELDS)
        if redis.sismember(R_MONITOR_SITE_LIST, row_hash):
            pass
        else:
            send_to_members(build_message(row, EXPOSURE_MONITOR))
            redis.sadd(R_MONITOR_SITE_LIST, row_hash)

    # Mark this update as done.
    redis.set(R_UPDATE_TIME, update_time)

telegram_updater.start_polling()

do_update()

print("Done")
