import csv
import requests
import json

#  CSV file  to JSON
def csv_to_json(csv_file):
    data = []
    with open(csv_file, 'r') as file:
        csv_read = csv.DictReader(file)
        for row in csv_read:
            data.append(row)
    return json.dumps(data)

# ingest data into Splunk using HEC

def ingest_splunk(json_data, splunk_url, token):
    headers = {'Authorization': 'Splunk ' + token, 'Content-Type': 'application/json'}
    payload = json.dumps({"sourcetype": "_json", "event": json_data})
    response = requests.post(splunk_url, headers=headers, data=payload)
    if response.status_code == 200:
        print("Data ingested successfully into Splunk.", response)
    else:
        print("Error ingesting data into Splunk:", response)

csv_url = "https://dataelicitinterviewdata.s3.ap-south-1.amazonaws.com/prices.csv"
splunk_url = "http://localhost:8088//services/collector"
hec_token = "HEC TOKEN VALUE"   # enter your hec token value here

# Fetching and saving CSV file
response = requests.get(csv_url)
csv_file = "prices.csv"
with open(csv_file, 'wb') as file:
    file.write(response.content)

# CSV file to JSON data
json_data = csv_to_json(csv_file)

# Ingest data(JSON) into Splunk
ingest_splunk(json_data, splunk_url, hec_token)
