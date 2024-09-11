from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
import boto3
from botocore.exceptions import NoCredentialsError
import uuid
import re
import os
import csv
from io import StringIO, BytesIO, TextIOWrapper
from pydantic import BaseModel
from dotenv import load_dotenv
import requests
from PIL import Image

# Load environment variables from .env file
load_dotenv()

app = FastAPI()

# AWS DynamoDB configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_IMAGE_BUCKET = os.getenv("AWS_IMAGE_BUCKET")
DYNAMO_TABLE_NAME = os.getenv("DYNAMO_TABLE_NAME")
VALID_HEADERS = os.getenv("VALID_HEADERS").split(",")
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE"))
IMAGE_QUALITY = int(os.getenv("IMAGE_QUALITY"))

s3_client = boto3.client('s3',
                         aws_access_key_id=AWS_ACCESS_KEY_ID,
                         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                         region_name=AWS_REGION)

dynamodb = boto3.resource('dynamodb',
                          region_name=AWS_REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

table = dynamodb.Table(DYNAMO_TABLE_NAME)

class Item(BaseModel):
    requestId: str
    status: str

def write_to_db(item: Item) -> None:
    try:
        table.put_item(Item=item.model_dump())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error occurred while writing to DB: {str(e)}")
    
def read_from_db(requestId: str) -> None:
    try:
        response = table.get_item(
            Key={
                'requestId': requestId
            }
        )
        item = response.get('Item')

        if item:
            return item
        else:
            raise HTTPException(status_code=404, detail=f"requestId: {requestId} not found!")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Exception occurred: {str(e)}")
    
def upload_to_s3(file, requestId: str, replace = False) -> None:
    file_key = f"{requestId}.csv"
    s3_metadata = {
        'Content-Type': 'text/csv'
    }
    if not replace:
        s3_metadata = {
            'Content-Type': file.content_type,
            'x-amz-meta-original-filename': file.filename
        }

    try:
        s3_client.upload_fileobj(file if replace else file.file, AWS_BUCKET_NAME, file_key, ExtraArgs={"Metadata": s3_metadata}) 
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error occurred while writing file to S3: {str(e)}")

def upload_image_to_s3(file: BytesIO, image_extension: str) -> str:
    file_key = f'{str(uuid.uuid4()).replace("-", "")}.{image_extension}'
    s3_metadata = {
        'Content-Type': f"image/{image_extension}"
    }

    try:
        s3_client.upload_fileobj(file, AWS_IMAGE_BUCKET, file_key, ExtraArgs={"Metadata": s3_metadata})
        return f"https://{AWS_IMAGE_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{file_key}"
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error occurred while uploading image to S3: {str(e)}")

def read_from_s3(requestId: str) -> str:
    print(f"Reading from S3 for requestId: {requestId}")
    file_key = f"{requestId}.csv"
    response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=file_key)
    file_content = response['Body'].read()
    return file_content
 
def process_file(requestId: str):
    print(f"Processing started for requestId: {requestId}")
    try:
        item = Item(
            requestId=requestId,
            status="Processing in progress!"
        )
        write_to_db(item)

        file_content = read_from_s3(requestId)
        csv_reader = csv.reader(StringIO(file_content.decode('utf-8')))
        headers = next(csv_reader)
        headers.append("Output Image Urls")

        new_csv = [[headers]]
        for row in csv_reader:
            print(f"Processing row: {row}")
            image_urls = row[2].split(",")
            uploaded_urls = [compress_image(url) for url in image_urls]
            row.append(uploaded_urls)
            new_csv.append(row)
        
        # Convert list of lists to CSV
        csv_buffer = BytesIO()
        text_stream = TextIOWrapper(csv_buffer, encoding='utf-8', newline='')
        csv_writer = csv.writer(text_stream)
        csv_writer.writerows(new_csv)
        text_stream.flush()
        csv_buffer.seek(0)

        upload_to_s3(csv_buffer, requestId, replace=True)
        text_stream.close()

        item = Item(
            requestId=requestId,
            status=f"Processing completed successfully!"
        )
        print(f"Processing completed successfully for requestId: {requestId}")
        write_to_db(item)
    except Exception as e:
        item = Item(
            requestId=requestId,
            status=f"Processing failed due to {str(e)}!"
        )
        print(f"Processing failed: {str(e)}")
        write_to_db(item)

def compress_image(url: str) -> str:
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad responses
    
    # Compress the image by 50% of its original quality
    image = Image.open(BytesIO(response.content))
    output_io = BytesIO()

    if image.format in ['JPEG', 'JPG']:
        image.save(output_io, format='JPEG', quality=IMAGE_QUALITY)
    elif image.format == 'PNG':
        # compress_level: 0 (no compression) to 9 (maximum compression)
        image.save(output_io, format='PNG', compress_level=IMAGE_QUALITY // 10) 
    else:
        # For formats that do not support compression, just save as is
        image.save(output_io, format=image.format)

    output_io.seek(0)
    return upload_image_to_s3(output_io, image.format.lower())

@app.post("/upload-csv", summary="Upload a CSV file with a maximum size of 2 MB")
async def upload_csv(file: UploadFile, background_tasks: BackgroundTasks) -> dict:
    # Check if the uploaded file is a CSV and size <= 2MB
    if file.content_type != 'text/csv':
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")
    
    if file.file._file.seek(0, 2) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="File size exceeds the 2 MB limit!")

    # Reset file pointer to the beginning after reading
    file.file.seek(0)

    try:
        validate_csv(file.file)
    except HTTPException as ex:
        raise ex
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CSV parsing failed with exception: {str(e)}")

    requestId = str(uuid.uuid4()).replace("-", "")
    
    print(f"Uploading file to S3: {file.filename}")
    upload_to_s3(file, requestId)

    item = Item(
        requestId=requestId,
        status="Processing Pending"
    )
    print(f"Writing status to DB: {item}")
    write_to_db(item)

    # Send to background tasks for processing
    background_tasks.add_task(process_file, requestId)

    return {"message": "File uploaded successfully!", "requestId": requestId}
    
@app.get("/status/{requestId}", summary="Get the status of a requestId")
async def get_status(requestId: str) -> dict:
    return read_from_db(requestId)

@app.get("/get-csv/{requestId}", summary="Get the CSV file with compressed image urls after processing is completed")
async def get_csv(requestId: str):
    try:
        s3_key = f"{requestId}.csv"
        s3_object = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=s3_key)
        csv_content = s3_object['Body'].read()
        csv_stream = BytesIO(csv_content)
        return StreamingResponse(csv_stream, media_type='text/csv', headers={"Content-Disposition": f"attachment; filename={s3_key.split('/')[-1]}"})

    except s3_client.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail=f"File: {requestId}.csv not found in S3")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
def validate_csv(file) -> None:
    file_content = file.read()
    csv_reader = csv.reader(StringIO(file_content.decode('utf-8')))

    row_count = sum(1 for _ in csv_reader)
    if row_count <= 1:
        raise HTTPException(status_code=400, detail="CSV file is empty")

    csv_reader = csv.reader(StringIO(file_content.decode('utf-8')))
    headers = next(csv_reader)
    if headers != VALID_HEADERS:
        raise HTTPException(status_code=400, detail=f"CSV file headers are invalid. Allowed headers are {VALID_HEADERS}")

    for row in csv_reader:
        if len(row) < 3:
            raise HTTPException(status_code=400, detail=f"Row has insufficient columns: {row}")

        sno, product_name, image_urls = row
        
        # Validate sno
        if not sno.isdigit():
            raise HTTPException(status_code=400, detail=f"Invalid S.No. (not a number): {sno}")
        
        # Validate product_name
        if not is_alphanumeric_with_spaces(product_name):
            raise HTTPException(status_code=400, detail=f"Invalid product name (not alphanumeric): {product_name} at S.No.: {sno}")
        
        # Validate image_url
        image_urls_list = image_urls.split(',')
        if not all(is_valid_image_url(url.strip()) for url in image_urls_list):
            raise HTTPException(status_code=400, detail=f"Invalid Image Urls in list: {image_urls} at S.No.: {sno}")
        
    file.seek(0)

def is_alphanumeric_with_spaces(s: str) -> bool:
    # Regular expression to match alphanumeric characters and spaces
    pattern = r'^[a-zA-Z0-9 ]*$'
    return re.match(pattern, s) is not None

def is_valid_image_url(url: str) -> bool:
    # Regex pattern for validating URLs that end with common image formats
    url_pattern = re.compile(r'^(http|https)://[^\s/$.?#].[^\s]*\.(jpg|jpeg|png|gif)$', re.IGNORECASE)
    return re.match(url_pattern, url) is not None
