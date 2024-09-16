import boto3
import requests
import json
import csv
from datetime import datetime
from botocore.exceptions import NoCredentialsError
import io

# Convert to CSV
def convert_to_csv(data):
    # Convert the JSON response to CSV format
    output = io.StringIO()
    if data:
        fieldnames = data[0].keys()
    else:
        fieldnames = []

    csv_writer = csv.DictWriter(output, fieldnames=fieldnames)
    csv_writer.writeheader()

    for row in data:
        csv_writer.writerow(row)
    csv_string = output.getvalue()
    output.close()
    return csv_string

# Upload to S3

def upload_to_s3(csv_data,current_year,page):
    s3 = boto3.client('s3')
    # s3 = boto3.client(
    #     's3',
    #     aws_access_key_id='XXXXXXXXXXXX',
    #     aws_secret_access_key='XXXXXXXXXXXXXXXXXX',
    #     region_name='us-east-1'
    # )
    bucket_name = 'enrollmentbucket'
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"enrollment_data/enrollement_data_{current_year}_{page}_{timestamp}.csv" # enrollement_data+Year_Page_No

    try:
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_data, ContentType='text/csv')
        print(f"JSON data uploaded successfully to '{bucket_name}/{s3_key}'")
    except NoCredentialsError:
        print("Credentials not available")
    except Exception as e:
        print(f"An error occurred: {e}")

# Required API Parameters setup

api_base_url = "https://educationdata.urban.org/api/v1/schools/ccd/enrollment/{year}/{grade}"
def build_apiurl(min_year):
    year = min_year
    grade = 'grade-pk'
    query_params = {
        'page': 1
    }
    api_url = api_base_url.format(year=year, grade=grade)
    return api_url,query_params

def lambda_handler(event, context):
    all_data = []
    min_retries=1
    max_retries=3
    page = 1
    retries = 0
    min_year=2020
    max_year=2022
    current_year=min_year
    api_base_url = "https://educationdata.urban.org/api/v1/schools/ccd/enrollment/{year}/{grade}"
    next_page=''

    while True:
            try:
                if next_page=='':
                    api_url,query_params = build_apiurl(current_year)
                    response =requests.get(api_url, params=query_params)
                else:
                    response = requests.get(next_page)
                
                # If Success
                if response.status_code == 200:
                    page_data = response.json()  
                    #Convert to CSV
                    csv_data=convert_to_csv(page_data['results'])
                    #Upload to S3
                    upload_to_s3(csv_data,current_year,page)

                    # Break if no more data and no more pages
                    if not page_data['results'] or (page_data.get('next') is None and current_year==max_year):
                        break
                    
                    # next page
                    if page_data.get('next') is not None:
                        next_page=page_data.get('next')
                        page+=1

                    elif current_year<max_year:
                        current_year+=1
                        next_page=''
                        page=1
                    else:
                        break

                    retries = 0  # Reset retries on success
                    
                else:
                    # Handle unsuccessful response
                    print(f"Error: Received status code {response.status_code} on page {page}")
                    retries += 1
                    if retries >= max_retries:
                        print("Max retries reached. Stopping.")
                        break
                    
            except Exception as e:
                print(f"An error occurred: {e}")
                retries += 1
                if retries >= max_retries:
                    print("Max retries reached due to errors. Stopping.")
                    break
