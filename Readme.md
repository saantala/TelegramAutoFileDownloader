# Telegram File Downloader and Processor

A Python script for downloading and processing files from a Telegram group. This tool can handle various file formats including ZIP archives, Feather files, and Zstandard compressed files, automatically converting them to CSV format where applicable.

## Features

- Download files from specified Telegram groups
- Process multiple file formats:
  - ZIP archives (with nested file support)
  - Feather files (converts to CSV)
  - Zstandard (.zst) compressed files
  - Pickle files (converts to CSV)
- Flexible date range selection for downloads
- Automatic file conversion to CSV format
- Comprehensive logging
- Protection against zip bombs
- Configurable download options

## Prerequisites

- Python 3.6+
- Telegram API credentials (api_id and api_hash)
- A Telegram account

## Required Python Packages

```bash
pip install telethon
pip install pandas
pip install zstandard
```

## Setup

1. Clone this repository or download the script
2. Install the required packages
3. Update the following variables in the script with your credentials:
   ```python
   api_id = 'YOUR_API_ID'
   api_hash = 'YOUR_API_HASH'
   phone_number = 'YOUR_PHONE_NUMBER'
   group_username = 'TARGET_GROUP_USERNAME'
   ```

To obtain Telegram API credentials:

1. Visit https://my.telegram.org
2. Log in with your phone number
3. Go to 'API Development Tools'
4. Create a new application to get your `api_id` and `api_hash`

## Usage

Run the script using Python:

```bash
python telegram_downloader.py
```

The script will present you with several options for downloading files:

1. Week (last 7 days)
2. Month (last 30 days)
3. Custom (enter specific number of days)
4. Last 2 days
5. Last 3 days
6. Last 5 days

## File Processing

The script automatically processes downloaded files as follows:

- **ZIP Files**: Extracted and contents processed recursively
- **Feather Files**: Converted to CSV format
- **Zstandard Files**: Decompressed and contents converted to CSV if possible
- **Pickle Files**: Converted to CSV format

## Output Structure

- Downloaded files are stored in the `downloads` directory
- Processed files are converted to CSV format
- Original files can be optionally deleted after processing
- Logs are stored in `telegram_downloader.log`

## Logging

The script maintains detailed logs in `telegram_downloader.log`, including:

- Download status
- File processing results
- Errors and warnings
- Processing statistics

## Safety Features

- Maximum recursion depth for nested archives
- Zip bomb detection (1GB size limit)
- Error handling for corrupt files
- Logging of all operations

## Error Handling

The script includes comprehensive error handling for:

- Download failures
- File processing errors
- Invalid file formats
- Network issues
- Authentication problems

## Statistics

After processing, the script provides statistics including:

- Number of files downloaded
- Number of files extracted
- Number of files processed
- Number of failures
