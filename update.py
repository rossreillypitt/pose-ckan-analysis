import requests
import json
import urllib3
import csv
from bs4 import BeautifulSoup
from datetime import datetime, date
from dateutil import parser


datashades_url = "https://datashades.info/api/portal/list"
dataportals_url = "http://dataportals.org/api/data.json"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}
api_calls = ['package_list', 'tag_list', 'organization_list']
commondata_url = "https://raw.githubusercontent.com/commondataio/dataportals-registry/main/data/datasets/catalogs.jsonl"
update_api_calls = ['']

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
    portal_list = response.text.split('\n')
    portal_list.pop()
    return [json.loads(portal) for portal in portal_list]


def gather_commondata_urls(data: list[dict, ]):
    return [{portal['link']: portal['name']} for portal in data]


def commondata_portal_filter(data: list[dict, ], portal_type: str = 'CKAN'):
    known_portal_types = ['CKAN', 'DKAN']
    if portal_type == 'CKAN' or portal_type == 'DKAN':
        return [portal for portal in data if portal['software']['name'] == portal_type]
    else:
        return [portal for portal in data if portal['software']['name'] not in known_portal_types]


def url_setup(source, clean_urls):
    root_url_set = set()
    list_of_url_dicts = []
    for source_url in clean_urls:
        root_url = source_url.split("/")[2]
        base_url = source_url.split(root_url)[0]+root_url
        if root_url in root_url_set:
            pass
        else:
            root_url_set.add(root_url)
            list_of_url_dicts.append(
                {"source": source,
                 "source_url": source_url,
                 "root_url": root_url,
                 "base_url": base_url
                 }
            )
    return list_of_url_dicts


def deduplicate(list_of_lists: list[list[str, ], ])->list[str, ]:
    unique_urls:set[str, ] = set()
    output_list: list[dict[str:str, ]] = []
    for source in list_of_lists:
        for record in source:
            if record['root_url'] not in unique_urls:
                output_list.append(record)
                unique_urls.add(record['root_url'])
    return output_list


def checking_for_response(passed_list):
    full_error_list = []
    count = 0
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    for item in passed_list:
        count += 1
        generator = ""
        status: int = None
        try:
            print(f'Now checking url #{count}: {item["source_url"]}')
            response = requests.get(item["source_url"], verify=False, headers=headers, timeout=120)
            status = response.status_code
            soup = BeautifulSoup(response.text, features="html.parser")
            name = str(soup.title.string)
            meta_tags = soup.find_all("meta")
            if len(meta_tags) > 0:
                for element in meta_tags:
                    if element.get("name") == "generator":
                        generator = element.get("content")
        except AttributeError:
            name = "AttributeError"
        except requests.exceptions.SSLError as ssl_error:
            name = "SSL Error"
            generator = ""
        except requests.exceptions.ConnectionError as connect_error:
            name = "Connection Error"
            generator = ""
        except requests.exceptions.TooManyRedirects:
            name = "Too Many Redirects Error"
            generator = ""
        except requests.exceptions.Timeout:
            name = "Timeout"
            generator = ""
        except Exception as e:
                try:
                    name = e.response.text
                except:
                    name = "Error"
        item["name"] = name
        item["generator"] = generator
        item["status_code"] = status
    return passed_list


def api_check(record, url_category, api_call: str = 'status_show', update: bool = False):
    response = requests.get(f'{record[url_category]}/api/3/action/{api_call}', verify=False, headers=headers, timeout=120)
    content = json.loads(response.content)
    if api_call in api_calls:
        record[f"{api_call}_count"] = len(content["result"])
        record[f"{api_call}_source_base_or_apibase"] = url_category
        record[f"{api_call}_final_requests_url"] = response.url
    elif update:
        try:
            record["site_title"] = dict_check(content, "site_title")
        except:
            record["site_title"] = content["result"]["site_title"]
        try:
            record["site_description"] = dict_check(content, "site_description")
        except:
            record["site_description"] = content["result"]["site_description"]
        record["data_contact_email"] = content["result"]["error_emails_to"]
    else:
        record["api_base_url"] = content["result"]["site_url"]
        record["final_requests_url"] = response.url
        try:
            record["site_title"] = dict_check(content, "site_title")
        except:
            record["site_title"] = content["result"]["site_title"]
        record["version"] = content["result"]["ckan_version"]
        record["locale"] = content["result"]["locale_default"]
        record["extensions"] = content["result"]["extensions"]
        record["source_or_base"] = url_category
        try:
            record["site_description"] = dict_check(content, "site_description")
        except:
            record["site_description"] = content["result"]["site_description"]
        record["data_contact_email"] = content["result"]["error_emails_to"]
    return record


def dict_check(content, category):
    preliminary_dict = json.loads(content['result'][category])
    if 'en' in preliminary_dict:
        return preliminary_dict['en']
    else:
        return preliminary_dict


def ckan_status_show(passed_list, update: bool = False):
    full_error_list = []
    x = 0
    for record in passed_list:
        x += 1
        print(f'Now performing a status_show api call on site #{x}: {record["root_url"]}')
        try:
            record = api_check(record, 'source_url', update=update)
        except Exception as e:
            try:
                record = api_check(record, 'base_url', update=update)
            except Exception as e:
                error_list = [record["source_url"], (e.args)]
                full_error_list.append(error_list)
                pass
    return passed_list


def date_check(record, url_category, current_best_metadata_date):
    url = f'{record[url_category]}/api/3/action/current_package_list_with_resources?limit=2000000'
    response = requests.get(url, verify=False, headers=headers, timeout=120)
    content = json.loads(response.content)
    for items in content["result"]:
        metadata_creation_date = items["metadata_created"]
        m_c_d = parser.parse(metadata_creation_date)
        if m_c_d < current_best_metadata_date:
            current_best_metadata_date = m_c_d
        else:
            pass
    record["oldest_metadata_created_date"] = current_best_metadata_date
    most_recent_update_date = content["result"][0]["metadata_modified"]
    record["most_recent_update_date"] = parser.parse(most_recent_update_date)
    record["dates_source_base_or_apibase"] = url_category
    return record


def ckan_all_other_functions(passed_list, update: bool = False):
    full_error_list_packages = []
    full_error_list_dates = []
    x = 0
    for record in passed_list:
        x += 1
        print(f'Now performing additional API calls on site #{x}: {record["root_url"]}')
        current_best_metadata_date = datetime.now()
        for call in api_calls:
            try:
                api_check(record, "source_url", call)
            except Exception as e:
                try:
                    api_check(record, "base_url", call)
                except Exception as e:
                    try:
                        api_check(record, "api_base_url", call)
                    except Exception as e:
                        error_list = [record["source_url"], (e.args)]
                        full_error_list_packages.append(error_list)
                        pass
        if update:
            pass
        else:
            try:
                date_check(record, "source_url", current_best_metadata_date)
            except Exception as e:
                try:
                    date_check(record, "base_url", current_best_metadata_date)
                except Exception as e:
                    try:
                        date_check(record, "api_base_url", current_best_metadata_date)
                    except Exception as e:
                        error_list = [record["source_url"], (e.args)]
                        full_error_list_dates.append(error_list)
    return passed_list


def write_output_file(list_of_ckan_dicts, filename):
    fieldnames = []
    for item in list_of_ckan_dicts:
        if len(item.keys()) > len(fieldnames):
            fieldnames = item.keys()
    with open(filename, "w", encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for item in list_of_ckan_dicts:
            writer.writerow(item)


def steps():
    shadeslist = gather_shades_urls(datashades_url)
    portalslist = gather_portals_urls(dataportals_url)
    shades = url_setup("shades", shadeslist)
    portals = url_setup("portals", portalslist)
    list_of_open_data_instances = deduplicate([shades, portals])
    list_of_open_data_instances = checking_for_response(list_of_open_data_instances)
    list_of_open_data_instances = ckan_status_show(list_of_open_data_instances)
    list_of_open_data_instances = ckan_all_other_functions(list_of_open_data_instances)
    write_output_file(list_of_open_data_instances, "ckan_check_feb_26.csv")


def commondata_steps():
    commondata_data = gather_commondata_data(commondata_url)
    commondata_ckan = commondata_portal_filter(commondata_data)
    commondata_urls = gather_commondata_urls(commondata_data)
    commondata_ckan_url_list = [list(item.keys())[0] for item in commondata_urls]
    common = url_setup("commondata", commondata_ckan_url_list)
    ckans = checking_for_response(common)
    ckans = ckan_status_show(ckans)
    ckans = ckan_all_other_functions(ckans)
    write_output_file(ckans, "commondata_urls_update_mar_18_overnight.csv")


def old_data_update_steps():
    with open("old_list_with_new_calls.csv", "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter = ',')
        old_data = [row for row in reader]
    old_update = ckan_all_other_functions(old_data, update=True)
    write_output_file(old_update, "old_list_with_new_calls_pt_2.csv")

'''
def extract_fields_for_website():
    title = ['result']['site_title'] # this may be a stringified dict where 'en' is the key you want
    notes = ['result']['site_description'] # also may be a stringified dict where you'd hope for 'en'
    url = ['result']['site_url']
    data_contact_email = ['result']['error_emails_to']
'''


def ckan_all_other_functions_record_level(record, counter, update: bool = False):
    print(f'Now performing additional API calls on site #{counter}: {record["root_url"]}')
    current_best_metadata_date = datetime.now()
    for call in api_calls:
        try:
            api_check(record, "source_url", call)
        except Exception as e:
            try:
                api_check(record, "base_url", call)
            except Exception as e:
                try:
                    api_check(record, "api_base_url", call)
                except Exception as e:
                    error_list = [record["source_url"], (e.args)]
                    pass
    if update:
        return record
    else:
        try:
            date_check(record, "source_url", current_best_metadata_date)
        except Exception as e:
            try:
                date_check(record, "base_url", current_best_metadata_date)
            except Exception as e:
                try:
                    date_check(record, "api_base_url", current_best_metadata_date)
                except Exception as e:
                    error_list = [record["source_url"], (e.args)]
    return record


