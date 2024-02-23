import json
import requests

if __name__ == "__main__":
    with open('request.json', 'r') as file:
        json_data = json.load(file)

    api_url = 'http://localhost:8080/products-variation'
    response = requests.post(api_url, json=json_data)

    if response.status_code == 200:
        result_json = response.json()
        with open('result.json', 'w') as result_file:
            json.dump(result_json[:100], result_file, indent=2)
        print("End of export of result in result.json", flush=True)
    else:
        print(f"Error with request: {response.status_code}")
        print(response.text)
