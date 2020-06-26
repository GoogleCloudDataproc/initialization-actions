import requests
import sys

username = sys.argv[1]
password = sys.argv[2]
query = 'John'
expected_entities_count = 3

if len(sys.argv) > 3:
    query = sys.argv[3]
    expected_entities_count = int(sys.argv[4])

status_response = requests.get('http://localhost:21000/api/atlas/admin/status')
status = status_response.content
if status == "ACTIVE":
    response = requests.get(
        'http://localhost:21000/api/atlas/v2/search/basic',
        params={'query': query},
        auth=(username, password),
        allow_redirects=False,
    )
    assert response.status_code == 200
    response_dict = response.json()
    entities = response_dict.get('entities', [])
    assert len(entities) == expected_entities_count
elif status == "PASSIVE":
    response = requests.get(
        'http://localhost:21000/api/atlas/v2/search/basic',
        params={'query': query},
        auth=(username, password),
        allow_redirects=False,
    )
    assert response.status_code == 302
