import os
import yaml
import logging
import pytz
import re
from dotenv import load_dotenv
from telethon import TelegramClient, events
import asyncio
from datetime import datetime
from pymongo import MongoClient
from telethon.errors.rpcerrorlist import MessageNotModifiedError

# Load .env file
load_dotenv()

# Define the path to the config.yaml file
CONFIG_PATH = "/home/admin/Production/AlertsV2DB/config.yaml"

# Load the configuration from the config.yaml file
def load_config():
    with open(CONFIG_PATH, 'r') as file:
        config = yaml.safe_load(file)
    return config

# Initialize MongoDB client using the URI from .env
def initialize_mongo_client():
    mongo_username = os.getenv('MONGO_USERNAME')
    mongo_password = os.getenv('MONGO_PASSWORD')
    ec2_instance_ip = os.getenv('EC2_INSTANCE_IP')
    mongo_database = os.getenv('MONGO_DATABASE')

    mongo_uri = f"mongodb://{mongo_username}:{mongo_password}@{ec2_instance_ip}:27017/{mongo_database}"

    mongo_client = MongoClient(mongo_uri)

    config = load_config()
    db_name = config['mongodb']['database']
    collection_name = config['mongodb']['collection']
    db = mongo_client[db_name]
    collection = db[collection_name]
    return collection

# Rest of the script remains unchanged...


# Updated keyword-based extraction method
def parse_message_with_keywords(message, message_id, timestamp):
    try:
        keywords = ['Swapped', 'for', '@', '$', 'Token:']
        words = message.split()

        wallet_name = 'N/A'
        token_amount_sent = 'N/A'
        token_name_sent = 'N/A'
        token_amount_received = 'N/A'
        token_name_received = 'N/A'
        blockchain = 'N/A'
        token_price = 'N/A'
        token_address = 'N/A'
        starred = False

        wallet_name_captured = False

        sent_token_amount_found = False
        sent_token_name_found = False
        received_token_amount_found = False
        received_token_name_found = False

        token_price_pattern = re.compile(r'@\s*\$(\d+(?:,\d+)?(?:\.\d+)?)')
        token_address_pattern = re.compile(r'Token:\s*(0x[a-fA-F0-9]{40})')

        for i, word in enumerate(words):
            if word.startswith('#') and not wallet_name_captured:
                wallet_name = word[1:]
                wallet_name_captured = True
            elif word in keywords:
                if word == 'Swapped':
                    token_amount_sent = float(words[i + 1].replace(',', ''))
                    token_name_sent = words[i + 2].replace('#', '')
                    sent_token_amount_found = True
                    sent_token_name_found = True
                elif word == 'for':
                    token_amount_received = float(words[i + 1].replace(',', ''))
                    token_name_received = words[i + 2].replace('#', '')
                    received_token_amount_found = True
                    received_token_name_found = True
                elif word == '@' and i < len(words) - 1:
                    token_price_match = token_price_pattern.search(message)
                    if token_price_match:
                        token_price = token_price_match.group(1)
                elif word == 'Token:' and i < len(words) - 1:
                    token_address_match = token_address_pattern.search(message)
                    if token_address_match:
                        token_address = token_address_match.group(1)
                        # Extract blockchain information based on the context of the message
                        following_words = words[i + 1:]
                        for w in following_words:
                            if w.startswith('#'):
                                blockchain = w[1:]
                                break

                if '⭐️' in message:
                    starred = True

        if not sent_token_amount_found or not sent_token_name_found:
            token_amount_sent = None
            token_name_sent = None
        if not received_token_amount_found or not received_token_name_found:
            token_amount_received = None
            token_name_received = None

        # Convert timestamp to EAT Timezone
        eat_timezone = pytz.timezone('Africa/Nairobi')
        eat_timestamp = datetime.fromtimestamp(timestamp, pytz.utc).astimezone(eat_timezone).strftime('%Y-%m-%d %H:%M:%S')

        # Initialize MongoDB client and collection
        mongo_collection = initialize_mongo_client()

        # Insert data into MongoDB
        mongo_collection.insert_one({
            'message_id': message_id,
            'timestamp': eat_timestamp,
            'wallet_name': wallet_name,
            'token_amount_sent': token_amount_sent,
            'token_name_sent': token_name_sent,
            'token_amount_received': token_amount_received,
            'token_name_received': token_name_received,
            'blockchain': blockchain,
            'token_price': token_price,
            'token_address': token_address,
            'starred': starred
        })

        return {
            'wallet_name': wallet_name,
            'token_amount_sent': token_amount_sent,
            'token_name_sent': token_name_sent,
            'token_amount_received': token_amount_received,
            'token_name_received': token_name_received,
            'blockchain': blockchain,
            'token_price': token_price,
            'token_address': token_address,
            'starred': starred
        }
    except Exception as e:
        logging.error(f"Error parsing message with keywords: {e}")
        return None

async def message_handler(event, processed_message_ids, processed_messages_file, sourcemessages_file, alert_chat_id, alert_bot_token, message_queue):
    try:
        message_id = event.id
        eat_timezone = pytz.timezone('Africa/Nairobi')  # EAT timezone
        eat_datetime = event.date.astimezone(eat_timezone)
        eat_timestamp = eat_datetime.strftime('%Y-%m-%d %H:%M:%S')

        # Parse the message
        # Parse the message using keyword-based extraction method
        parsed_message_keywords = parse_message_with_keywords(event.raw_text, message_id, event.date.timestamp())

        if parsed_message_keywords:
            # Add the message ID to the processed set
            processed_message_ids.add(message_id)

            # Log message ID and timestamp
            with open(processed_messages_file, 'a') as f:
                f.write(f'Message ID: {message_id}, Timestamp: {eat_timestamp}\n')

            # Record message ID and timestamp in sourcemessages.txt
            with open(sourcemessages_file, 'a') as f:
                f.write(f'Message ID: {message_id}, Timestamp: {eat_timestamp}\n')

    except Exception as e:
        logging.error(f'Error in message_handler: {e}')
        send_telegram_alert(f"Error processing message: {e}\nMessage ID: {message_id}", alert_chat_id, alert_bot_token)
        # Retry the message later by putting it back into the queue
        await message_queue.put(event)

async def process_message_queue(message_queue, processed_message_ids, processed_messages_file, sourcemessages_file, alert_chat_id, alert_bot_token):
    while True:
        try:
            # Get a message from the queue
            event = await message_queue.get()

            # Handle the message
            await message_handler(event, processed_message_ids, processed_messages_file, sourcemessages_file, alert_chat_id, alert_bot_token, message_queue)

            # Mark the message as processed
            message_queue.task_done()
        except Exception as e:
            logging.error(f'Error in process_message_queue: {e}')

async def main():
    try:
        # Load configuration from config.yaml
        config = load_config()

        # Telegram configuration
        api_id = os.getenv('TELEGRAM_API_ID')
        api_hash = os.getenv('TELEGRAM_API_HASH')
        source_channel_id = int(config['telegram']['source_channel_id'])
        alert_chat_id = os.getenv('ALERT_CHAT_ID')
        alert_bot_token = os.getenv('ALERT_BOT_TOKEN')

        # Derive file paths from the config file
        base_dir = os.path.dirname(CONFIG_PATH)
        sourcemessages_file = os.path.join(base_dir, config['files']['sourcemessages'])
        processed_messages_file = os.path.join(base_dir, config['files']['processed_messages'])
        log_file = os.path.join(base_dir, config['logs']['filename'])
        session_file = os.path.join(base_dir, config['telegram']['session_file'])  # Grab session file path from config.yaml
        # Initialize an empty set to store processed message IDs
        processed_message_ids = set()

        # Setup logging
        logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        # Create or load the Telegram client session with the absolute session file path
        client = TelegramClient(session_file, api_id, api_hash)

        # Ensure that the client is connected before starting event handling
        await client.start()

        # Create a message queue
        message_queue = asyncio.Queue()

        # Start the message processing queue
        asyncio.create_task(process_message_queue(message_queue, processed_message_ids, processed_messages_file, sourcemessages_file, alert_chat_id, alert_bot_token))

        # Register the event handler for processing new messages
        @client.on(events.NewMessage(chats=source_channel_id))
        async def event_handler(event):
            await message_queue.put(event)

        # Start the client's event loop
        await client.run_until_disconnected()

    except Exception as e:
        logging.error(f"Error in main: {e}")

# Run the main function
asyncio.run(main())
