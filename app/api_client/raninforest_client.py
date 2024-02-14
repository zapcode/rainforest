import requests


# Singleton Class for Rainforest API Client
class RainforestAPIClient:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.api_key = kwargs.get("api_key")
            cls._instance.limit = kwargs.get("limit")
        return cls._instance

    def get_reviews(self, product_id):
        url = f'https://api.rainforestapi.com/request?api_key={self.api_key}&type=reviews&limit={self.limit}&amazon_domain=amazon.in&asin={product_id}&output=json'
        response = requests.get(url)
        if response.status_code == 200:
            return response.json().get('reviews', [])
        else:
            return []
