# FastAPI Image Compression Service

This FastAPI service allows you to upload CSV files containing image URLs, which are then processed to compress the images by 50% of their original quality. The service supports image uploads, processing, and saving to Amazon S3.

## Features

- **Upload CSV**: Accepts a CSV file containing image URLs.
- **Image Compression**: Compresses images from the URLs listed in the CSV file.
- **S3 Integration**: Uploads the processed images to an S3 bucket.

## Requirements

- Python 3.8+
- FastAPI
- Uvicorn
- Pillow (for image processing)
- Boto3 (for interacting with AWS S3)
- `python-dotenv` (for environment variable management)