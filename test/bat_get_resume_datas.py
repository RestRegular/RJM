import requests

result = requests.get("http://localhost:8000/api/resume/bat_get", json={
    'ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
})

print(result.json()['data'])