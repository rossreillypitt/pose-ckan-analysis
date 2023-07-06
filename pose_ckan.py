import csv
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import datetime, date
from dateutil import parser
import urllib3
import sys

# preprocessing of datashades.info urls
# datashades_raw_list = sys.argv[1]

datashades_raw_list = "shades.csv"

outside_list = ['http://data.ctdata.org/',
                'https://data.ci.newark.nj.us/',
                'https://open.jacksonms.gov/',
                'https://data.ca.gov/',
                'https://datagate.snap4city.org/'
                ]

def datashades_clean_up(datashades_list):
    urls = []
    with open(datashades_list, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            row_strip = row[0].strip()
            if row_strip[:5] == "href=":
                to_add = row_strip[37:-19]
                urls.append(to_add.split('%2F">')[0])
            else:
                pass

    clean_urls = []
    for item in urls:
        item = re.sub("%3A", ":", item)
        item = re.sub("%2F", "/", item)
        item = re.sub("%26", "%", item)
        item = re.sub("%3D", "=", item)
        item = re.sub("%3F", "?", item)
        item = re.sub("%23", "#", item)
        clean_urls.append(item)

    return clean_urls

# preprocessing of dataportals.org urls
def dataportals_clean_up():
    portals_df = pd.read_csv("https://raw.githubusercontent.com/okfn/dataportals.org/master/data/portals.csv")
    portals_list = list(portals_df.url)
    return portals_list

def url_setup(source, clean_urls):
    root_url_set = set()
    list_of_url_dicts_2 = []
    for item in clean_urls:
        root_url = item.split("/")[2]
        if root_url in root_url_set:
            pass
        else:
            root_url_set.add(root_url)
            url_dict = {}
            url_dict["source"] = source
            url_dict["source_url"] = item
            url_dict["root_url"] = root_url
            url_dict["base_url"] = item.split(root_url)[0]+root_url
            list_of_url_dicts_2.append(url_dict)
    return list_of_url_dicts_2

def checking_for_response(passed_list):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}
    full_error_list = []
    count = 0

    for item in passed_list:
        count+=1
        try:
            print(f'Now checking url #{count}: {item["source_url"]}')
            response = requests.get(item["source_url"], verify=False, headers=headers, timeout=120)
            status = response.status_code
            soup = BeautifulSoup(response.text, features="html.parser")
            name = str(soup.title.string)
            meta_tags = soup.find_all("meta")
            if len(meta_tags)>0:
                for element in meta_tags:
                    if element.get("name") == "generator":
                        generator = element.get("content")
                    else:
                        generator = ""
            else:
                generator = ""
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

def status_counts(passed_list):
    status_counts_dict = {}
    for item in passed_list:
        if item['source'] in status_counts_dict.keys():
            status_counts_dict[item['source']]['tally'] += 1
            if int(item['status_code']) == 200:
                status_counts_dict[item['source']]['200_response_count'] += 1
        else:
            status_counts_dict[item['source']] = {'tally': 1}
            if int(item['status_code']) == 200:
                if '200_response_count' in status_counts_dict[item['source']].keys():
                    status_counts_dict[item['source']]['200_response_count'] += 1
                else:
                    status_counts_dict[item['source']]['200_response_count'] = 1
            else:
                status_counts_dict[item['source']]['200_response_count'] = 0
    output_list = []
    for item in status_counts_dict:
        output_list.append({'source': item, 'tally': status_counts_dict[item]['tally'],
                            '200_response_count': status_counts_dict[item]['200_response_count']})
    return output_list
    # for item in status_counts_dict:
    #     print(f'Total number of unique {item} portals: {status_counts_dict[item]["tally"]}, Count of 200 status codes: '
    #            f'{status_counts_dict[item]["200_response_count"]}')

def ckan_status_show(passed_list):
    full_error_list = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}
    x=0
    for item in passed_list:
        x+=1
        print(f'Now performing a status_show api call on site #{x}: {item["root_url"]}')
        try:
            response = requests.get(f'{item["source_url"]}/api/3/action/status_show', verify=False, headers=headers, timeout=120)
            content = json.loads(response.content)
            item["api_base_url"] = content["result"]["site_url"]
            item["site_title"] = content["result"]["site_title"]
            item["version"] = content["result"]["ckan_version"]
            item["locale"] = content["result"]["locale_default"]
            item["extensions"] = content["result"]["extensions"]
            item["source_or_base"] = "source"
        except Exception as e:
            try:
                response = requests.get(f'{item["base_url"]}/api/3/action/status_show', verify=False, headers=headers, timeout=120)
                content = json.loads(response.content)
                item["api_base_url"] = content["result"]["site_url"]
                item["site_title"] = content["result"]["site_title"]
                item["version"] = content["result"]["ckan_version"]
                item["locale"] = content["result"]["locale_default"]
                item["extensions"] = content["result"]["extensions"]
                item["source_or_base"] = "base"
            except Exception as e:
                error_list = [item["source_url"], (e.args)]
                full_error_list.append(error_list)
                pass
    return passed_list

def ckan_all_other_functions(passed_list):
    full_error_list_packages = []
    full_error_list_orgs = []
    full_error_list_tags = []
    full_error_list_dates = []
    full_error_list_new_dates = []
    x=0
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}
    api_calls = ['package_list', 'tag_list', 'organization_list']
    def api_check(dict_to_check, url_category, api_call):
        url = f'{dict_to_check[url_category]}/api/3/action/{api_call}'
        response = requests.get(url, verify=False, headers=headers, timeout=120)
        content = json.loads(response.content)
        dict_to_check[f"{api_call}_count"] = len(content["result"])
        dict_to_check[f"{api_call}_source_base_or_apibase"] = url_category
        return dict_to_check

    def date_check(dict_to_check, url_category, current_best_metadata_date):
        url = f'{dict_to_check[url_category]}/api/3/action/current_package_list_with_resources?limit=2000000'
        response = requests.get(url, verify=False, headers=headers, timeout=120)
        content = json.loaads(response.content)
        for items in content["result"]:
            metadata_creation_date = items["metadata_created"]
            m_c_d = parser.parse(metadata_creation_date)
            if m_c_d < current_best_metadata_date:
                current_best_metadata_date = m_c_d
            else:
                pass
        item["oldest_metadata_created_date"] = current_best_metadata_date
        most_recent_update_date = content["result"][0]["metadata_modified"]
        item["most_recent_update_date"] = parser.parse(most_recent_update_date)
        item["dates_source_base_or_apibase"] = url_category

    for item in passed_list:
        x+=1
        print(f'Now performing additional API calls on site #{x}: {item["root_url"]}')
        current_best_metadata_date = datetime.now()
        for call in api_calls:
            try:
                api_check(item, "source_url", call)
            except Exception as e:
                try:
                    api_check(item, "base_url", call)
                except Exception as e:
                    try:
                        api_check(item, "api_base_url", call)
                    except Exception as e:
                        print(item["source_url"])
                        error_list = [item["source_url"], (e.args)]
                        full_error_list_packages.append(error_list)
                        pass
        try:
            date_check(item, "source_url", current_best_metadata_date)
        except Exception as e:
            try:
                date_check(item, "base_url", current_best_metadata_date)
            except Exception as e:
                try:
                    date_check(item, "api_base_url", current_best_metadata_date)
                except Exception as e:
                    error_list = [item["source_url"], (e.args)]
                    full_error_list_dates.append(error_list)
    return passed_list

def duplicate_removal_processing(starting_list, new_list):
    unique_urls = set([item['root_url'] for item in starting_list])
    for item in new_list:
        if item['root_url'] in unique_urls:
            pass
        else:
            starting_list.append(item)
            unique_urls.add(item['root_url'])
    return starting_list

# def instance_combination(shades_processed, portals_processed):
#     unique_urls = set()
#     both_processed = []
#     for item in shades_processed:
#         if item["root_url"] in unique_urls:
#             pass
#         else:
#             both_processed.append(item)
#             unique_urls.add(item["root_url"])
#
#     print(len(both_processed))
#     for item in portals_processed:
#         if item["root_url"] in unique_urls:
#             print(item["root_url"])
#             pass
#         else:
#             both_processed.append(item)
#
#     return both_processed

def write_output_file(all_urls_final, filename):
    fieldnames = []
    for item in all_urls_final:
        if len(item.keys()) > len(fieldnames):
            fieldnames = item.keys()

    with open(filename, "w", encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        writer.writeheader()
        for item in all_urls_final:
            writer.writerow(item)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
clean_urls = datashades_clean_up(datashades_raw_list)
portals_list = dataportals_clean_up()
raw_shades_count = len(clean_urls)
raw_portals_count = len(portals_list)
shades_processed = url_setup("datashades.info", clean_urls)
portals_processed = url_setup("dataportals.org", portals_list)
outside_processed = url_setup("WPRDC", outside_list)
shades_and_portals_deduped = duplicate_removal_processing(shades_processed, portals_processed)
all_processed = duplicate_removal_processing(shades_and_portals_deduped, outside_processed)
checking_for_response = checking_for_response(all_processed)
output_list = status_counts(all_processed)
for item in output_list:
    if item['source'] == 'datashades.info':
        item['starting_count'] = raw_shades_count
    if item['source'] == 'dataportals.org':
        item['starting_count'] = raw_portals_count
filename_2 = "ckan_extra_stats.csv"
write_output_file(output_list, filename_2)
status_show = ckan_status_show(checking_for_response)
all_urls_final = ckan_all_other_functions(status_show)
filename = "ckan_requests_responses-3.csv"
write_output_file(all_urls_final, filename)

