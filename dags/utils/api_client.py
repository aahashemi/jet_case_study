import requests

class XkcdClient:
    
    BASE_URL = "https://xkcd.com"

    def fetch_latest(self):

        response = requests.get(f"{self.BASE_URL}/info.0.json")
        return response

    def fetch_by_id(self, comic_id):
        response = requests.get(f"{self.BASE_URL}/{comic_id}/info.0.json")        
        return response
    