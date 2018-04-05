import requests
import json
class Web:
    @staticmethod
    def get(url):
        response = requests.get(url)
        return json.loads(response.text)

    @staticmethod
    def get_string(url):
        return requests.get(url)
