import requests
import json

datashades_url = "https://datashades.info/api/portal/list"
dataportals_url = "http://dataportals.org/api/data.json"
commondata_url = "https://raw.githubusercontent.com/commondataio/dataportals-registry/main/data/datasets/catalogs.jsonl"


def gather_shades_urls(url: str) -> list[str, ]:
    response = requests.get(url)
    raw_content = json.loads(response.text)
    urls = [item['Href'] for item in raw_content['portals']]
    return urls


def gather_portals_urls(url: str) -> list[str, ]:
    response = requests.get(url)
    raw_portals = json.loads(response.content)
    portal_names = list(raw_portals.keys())
    urls = [raw_portals[portal]['url'] for portal in portal_names]
    return urls


def gather_commondata_data(url: str) -> list[dict, ]:
    response = requests.get(url)
    return [json.loads(portal) for portal in response.text.split('\n').pop()]


def gather_commondata_urls(data: list[dict, ]):
    return [{portal['link']: portal['name']} for portal in data]


def commondata_portal_filter(data: list[dict, ], portal_type: str = 'CKAN'):
    known_portal_types = ['CKAN', 'DKAN']
    if portal_type == 'CKAN' or portal_type == 'DKAN':
        return [portal for portal in data if portal['software']['name'] == portal_type]
    else:
        return [portal for portal in data if portal['software']['name'] not in known_portal_types]

