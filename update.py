import requests
import json

new_url = "https://datashades.info/api/portal/list"
def gather_shades_urls(url: str):
    response = requests.get(url)
    raw_content = json.loads(response.text)
    urls = [item['Href'] for item in raw_content['portals']]
    return urls