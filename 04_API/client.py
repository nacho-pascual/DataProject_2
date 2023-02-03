import requests

response = requests.post('http://localhost:5000/enrich_data', json={'id': 1, 'name': 'foo', 'timestamp': '1232132131'})
print("Status code: ", response.status_code)
print("Printing Entire Post Request")
print(response.json())