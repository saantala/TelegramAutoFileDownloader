
     


import os
import datetime
import zipfile
import logging
import pandas as pd
import pickle
import csv
import zstandard as zstd
from telethon.sync import TelegramClient
from telethon.tl.types import MessageMediaDocument
from pathlib import Path

# Configure logging
logging.basicConfig(filename='telegram_downloader.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

api_id = '23217964'
api_hash = '79ef1a09d15a6814e1bb23aae74e092b'
phone_number = '+918317501842'
group_username = 'nfo_data'

client = TelegramClient('session_name', api_id, api_hash)

download_folder = 'downloads'
if not os.path.exists(download_folder):
    os.makedirs(download_folder)

async def fetch_available_dates(group):
    """Fetch available dates from the Telegram group."""
    dates = []
    async for message in client.iter_messages(group, limit=None):
        if isinstance(message.media, MessageMediaDocument):
            message_date = message.date.date()
            if message_date not in dates:
                dates.append(message_date)
    return sorted(dates)

def process_feather_file(feather_path, delete_original=True):
    """Process a feather file and convert it to CSV."""
    try:
        # Read the feather file
        df = pd.read_feather(feather_path)
        if df.empty:
            logging.warning(f"Empty feather file: {feather_path}")
            return False

        # Create CSV path in the same directory
        csv_path = str(Path(feather_path).with_suffix('.csv'))
        
        # Convert to CSV
        df.to_csv(csv_path, index=False)
        logging.info(f"Successfully converted feather to CSV: {csv_path}")

        # Delete original if requested
        print(delete_original)
        if delete_original:
            os.remove(feather_path)
            logging.info(f"Deleted original feather file: {feather_path}")

        return True

    except Exception as e:
        logging.error(f"Error processing feather file {feather_path}: {str(e)}")
        return False

def convert_pickle_to_csv(pickle_file, output_csv):
    """Convert pickle file to CSV format."""
    try:
        with open(pickle_file, 'rb') as f:
            data = pickle.load(f)
        
        if isinstance(data, pd.DataFrame):
            # If the pickle contains a DataFrame, save directly to CSV
            data.to_csv(output_csv, index=False)
        elif isinstance(data, dict):
            # Handle dictionary data
            with open(output_csv, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                # Write header
                writer.writerow(['Key', 'Value'])
                # Write data
                for key, value in data.items():
                    if isinstance(value, (list, tuple)):
                        for item in value:
                            writer.writerow([key, item])
                    else:
                        writer.writerow([key, value])
        else:
            logging.error(f"Unsupported pickle data format in: {pickle_file}")
            return False

        logging.info(f"Successfully converted pickle to CSV: {output_csv}")
        return True

    except Exception as e:
        logging.error(f"Error converting pickle to CSV {pickle_file}: {str(e)}")
        return False

def extract_nested_zstd(file_path, delete_original=True):
    """Extract nested files treating them as Zstandard compressed files."""
    try:
        # Create extraction folder
        extract_folder = str(Path(file_path).with_suffix(''))
        os.makedirs(extract_folder, exist_ok=True)
        
        # Decompress Zstandard file
        decompressed_path = os.path.join(extract_folder, 'decompressed_file')
        with open(file_path, 'rb') as compressed_file:
            dctx = zstd.ZstdDecompressor()
            with open(decompressed_path, 'wb') as decompressed_file:
                dctx.copy_stream(compressed_file, decompressed_file)
        
        logging.info(f"Decompressed Zstd file to: {decompressed_path}")
        
        # Try to determine the file type and process accordingly
        try:
            # Try to read as feather
            df = pd.read_feather(decompressed_path)
            csv_path = str(Path(decompressed_path).with_suffix('.csv'))
            df.to_csv(csv_path, index=False)
            logging.info(f"Converted decompressed file to CSV: {csv_path}")
            os.remove(decompressed_path)
            
        except Exception as feather_error:
            logging.debug(f"Not a feather file: {str(feather_error)}")
            try:
                # Try to read as pickle
                csv_path = str(Path(decompressed_path).with_suffix('.csv'))
                if convert_pickle_to_csv(decompressed_path, csv_path):
                    os.remove(decompressed_path)
                else:
                    logging.warning(f"Failed to convert decompressed file to CSV: {decompressed_path}")
            except Exception as pickle_error:
                logging.debug(f"Not a pickle file: {str(pickle_error)}")
                # Keep the decompressed file as is
                pass
        
        if delete_original:
            os.remove(file_path)
            logging.info(f"Deleted original compressed file: {file_path}")
        
        return True
        
    except Exception as e:
        logging.error(f"Error extracting Zstd file {file_path}: {str(e)}")
        return False

# def extract_zip(file_path, delete_original=True, max_depth=10, current_depth=0):
#     """Extract zip files, handling nested archives and converting contents to CSV."""
#     if current_depth >= max_depth:
#         logging.warning(f"Maximum recursion depth ({max_depth}) reached for {file_path}")
#         return False
    
#     try:
#         extract_folder = str(Path(file_path).with_suffix(''))
#         os.makedirs(extract_folder, exist_ok=True)
        
#         # For top-level files, try as ZIP first
#         if current_depth == 0:
#             try:
#                 with zipfile.ZipFile(file_path, 'r') as zip_ref:
#                     # Check for zip bombs
#                     uncompressed_size = sum(file.file_size for file in zip_ref.filelist)
#                     if uncompressed_size > 1024 * 1024 * 1024:  # 1GB limit
#                         logging.error(f"Potential zip bomb detected in {file_path}")
#                         return False
                    
#                     zip_ref.extractall(extract_folder)
#                 logging.info(f"Extracted ZIP contents to: {extract_folder}")
#             except zipfile.BadZipFile:
#                 # If not a valid zip, try as Zstandard
#                 return extract_nested_zstd(file_path, delete_original)
#         else:
#             # For nested files, treat as Zstandard directly
#             return extract_nested_zstd(file_path, delete_original)
        
#         # Process extracted contents
#         for root, _, files in os.walk(extract_folder):
#             for file in files:
#                 file_path = os.path.join(root, file)
#                 if file.endswith('.zip'):
#                     extract_zip(file_path, delete_original=True,
#                               max_depth=max_depth, current_depth=current_depth + 1)
#                 elif file.endswith('.feather'):
#                     process_feather_file(file_path)
#                 elif file.endswith('.zst'):
#                     extract_nested_zstd(file_path)
        
#         if delete_original:
#             os.remove(file_path)
#             logging.info(f"Deleted original archive: {file_path}")
        
#         return True
        
#     except Exception as e:
#         logging.error(f"Error extracting {file_path}: {str(e)}")
#         return False

def extract_zip(file_path, delete_original=True, max_depth=10, current_depth=0):
    """Extract zip files, handling nested archives and converting contents to CSV."""
    if current_depth >= max_depth:
        logging.warning(f"Maximum recursion depth ({max_depth}) reached for {file_path}")
        return False
    
    try:
        extract_folder = str(Path(file_path).with_suffix(''))
        os.makedirs(extract_folder, exist_ok=True)
        
        # For top-level files, try as ZIP first
        if current_depth == 0:
            try:
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    # Check for zip bombs
                    uncompressed_size = sum(file.file_size for file in zip_ref.filelist)
                    if uncompressed_size > 1024 * 1024 * 1024:  # 1GB limit
                        logging.error(f"Potential zip bomb detected in {file_path}")
                        return False
                    
                    zip_ref.extractall(extract_folder)
                    
                # Delete the original zip file immediately after successful extraction
                if delete_original:
                    os.remove(file_path)
                    logging.info(f"Deleted original archive: {file_path}")
                    
                logging.info(f"Extracted ZIP contents to: {extract_folder}")
            except zipfile.BadZipFile:
                # If not a valid zip, try as Zstandard
                return extract_nested_zstd(file_path, delete_original)
        else:
            # For nested files, treat as Zstandard directly
            return extract_nested_zstd(file_path, delete_original)
        
        # Process extracted contents
        for root, _, files in os.walk(extract_folder):
            for file in files:
                nested_file_path = os.path.join(root, file)
                if file.endswith('.zip'):
                    extract_zip(nested_file_path, delete_original=True,
                              max_depth=max_depth, current_depth=current_depth + 1)
                elif file.endswith('.feather'):
                    process_feather_file(nested_file_path)
                elif file.endswith('.zst'):
                    extract_nested_zstd(nested_file_path)
        
        return True
        
    except Exception as e:
        logging.error(f"Error extracting {file_path}: {str(e)}")
        return False


async def download_and_process_files(date_range):
    """Download and process files from Telegram within the specified date range."""
    await client.start(phone=phone_number)
    group = await client.get_entity(group_username)

    current_time = datetime.datetime.now(datetime.timezone.utc)
    
    date_range_mapping = {
        'week': datetime.timedelta(weeks=1),
        'month': datetime.timedelta(days=30),
        'two_days': datetime.timedelta(days=2),
        'three_days': datetime.timedelta(days=3),
        'five_days': datetime.timedelta(days=5)
    }

    if date_range == 'custom':
        days_ago = int(input("Enter the number of days ago to start from: "))
        start_time = current_time - datetime.timedelta(days=days_ago)
    else:
        start_time = current_time - date_range_mapping.get(date_range, datetime.timedelta(days=2))

    print(f"Fetching files from: {start_time.date()} to {current_time.date()}")

    stats = {'downloaded': 0, 'extracted': 0, 'failed': 0, 'feather_processed': 0}

    async for message in client.iter_messages(group, limit=None):
        if message.date < start_time:
            break

        if isinstance(message.media, MessageMediaDocument):
            try:
                file_path = await message.download_media(file=download_folder)
                stats['downloaded'] += 1
                logging.info(f"Downloaded file: {file_path}")

                if file_path.endswith('.zip'):
                    if extract_zip(file_path):
                        stats['extracted'] += 1
                    else:
                        stats['failed'] += 1
                elif file_path.endswith('.feather'):
                    if process_feather_file(file_path):
                        stats['feather_processed'] += 1
                    else:
                        stats['failed'] += 1
                elif file_path.endswith('.zst'):
                    if extract_nested_zstd(file_path):
                        stats['extracted'] += 1
                    else:
                        stats['failed'] += 1
            except Exception as e:
                stats['failed'] += 1
                logging.error(f"Error processing file: {str(e)}")

    logging.info("\nProcessing Statistics:")
    for key, value in stats.items():
        logging.info(f"{key.title()}: {value}")

async def main():
    """Main function to run the program."""
    await client.start(phone=phone_number)
    group = await client.get_entity(group_username)

    dates = await fetch_available_dates(group)
    print(f"Available Dates for Download: {dates}")

    print("\nChoose the time range for downloading files:")
    print("1. Week (last 7 days)")
    print("2. Month (last 30 days)")
    print("3. Custom (enter specific number of days)")
    print("4. Last 2 days")
    print("5. Last 3 days")
    print("6. Last 5 days")
    choice = input("Enter your choice (1/2/3/4/5/6): ")

    date_range_map = {
        '1': 'week',
        '2': 'month',
        '3': 'custom',
        '4': 'two_days',
        '5': 'three_days',
        '6': 'five_days'
    }

    date_range = date_range_map.get(choice, 'two_days')
    if choice not in date_range_map:
        print("Invalid choice, defaulting to last 2 days.")
    
    await download_and_process_files(date_range)

if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main())