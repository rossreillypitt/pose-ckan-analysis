import csv
import re
import sys
import matplotlib.pyplot as plt
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from collections import Counter
from ckanapi import RemoteCKAN
from datetime import datetime, date
from dateutil import parser
import statistics

file_name = sys.argv[1]

reader = csv.DictReader(open(file_name, 'r', encoding='utf-8'))
packages = []
all_urls_final = []
for row in reader:
    all_urls_final.append(row)

def analysis_prep(all_urls_final):
    keys_to_pop_1 = ['api_base_url',
               'site_title',
               'version',
               'locale',
               'extensions',
               'package_count',
               'organization_count',
               'tags',
               'oldest_metadata_created_date',
               'most_recent_update_date',
               ]
    keys_to_pop_2 = ['package_list_source_base_or_apibase',
                     'tag_list_source_base_or_apibase',
                     'organization_list_source_base_or_apibase',
                     'dates_source_base_or_apibase']

    for item in all_urls_final:
        for key in keys_to_pop_1:
            if pd.isna(item[key]):
                del item[key]
            elif item[key] == '':
                del item[key]
        for keys in keys_to_pop_2:
            del item[keys]

    for item in all_urls_final:
        if 'oldest_metadata_created_date' in item.keys():
            item['oldest_metadata_created_date'] = datetime.strptime(item['oldest_metadata_created_date'][:10], '%Y-%m-%d')
        if 'most_recent_update_date' in item.keys():
            item['most_recent_update_date'] = datetime.strptime(item['most_recent_update_date'][:10], '%Y-%m-%d')

def ckan_version(all_urls_final):
    version_list = []
    for item in all_urls_final:
        try:
            version_list.append(item['version'].split('.')[:2])
        except KeyError:
            pass

    a, b = zip(*version_list)
    version_count = Counter(b)
    version_dict = {f'2.{item}':version_count[item] for item in version_count if item != "0#datapress"}

    x_data = list(sorted(version_dict))
    y_data = [version_dict[item] for item in x_data]

def time_calcs(all_urls_final):
    year_list = []
    time_dict = {}
    today = datetime.today()
    url_list = []
    for y in range(2007, 2024):
        count = 0
        age = 0
        timedeltas = []
        age_pct = []
        for item in all_urls_final:
            try:
                if 'oldest_metadata_created_date' in item.keys():
                    if 'most_recent_update_date' in item.keys():
                        if type(item['oldest_metadata_created_date']) == datetime:
                            if type(item['most_recent_update_date']) == datetime:
                                year_list.append((item['oldest_metadata_created_date'], item['most_recent_update_date']))
                                if int(item['oldest_metadata_created_date'].strftime('%Y')) == y:
                                    count+=1
                                    oldest = item['oldest_metadata_created_date']
                                    most_recent = item['most_recent_update_date']
                                    timedeltas.append(int((most_recent-oldest).days))
                                    age_pct.append(int((most_recent-oldest).days) / int((today - oldest).days))
                                    url_list.append(item['source_url'])

            except Exception as e:
                print(item['oldest_metadata_created_date'], item['source_url'], e)
        if len(timedeltas)>0:
            avg_age = round((sum(timedeltas)/len(timedeltas))/365, 2)
            median_age = round(statistics.median(timedeltas)/365, 2)
        else:
            avg_age = 0
            median_age = 0
        if len(age_pct)>0:
            age_pct_amt = round(((sum(age_pct))/(len(age_pct))), 2)
        else:
            age_pct_amt = 0
        time_dict[y] = {"year":y, "count": count, "avg_age":avg_age, "median":median_age, "average_lifespan":age_pct_amt}

        return(time_dict)

def graphing(time_dict):
    time_dict = sorted(time_dict.items())
    list_of_time_dicts = [item[1] for item in time_dict]
    count_list = []
    age_list = []
    lifespan_list = []
    for item in list_of_time_dicts:
        if item["count"] > 0:
            count_list.append((item["year"], item["count"]))
            age_list.append((item["year"], item["avg_age"]))
            lifespan_list.append((item["year"], item["average_lifespan"]))

    x_axis_count, y_axis_count = zip(*count_list)
    x_axis_age, y_axis_age = zip(*age_list)
    x_axis_life, y_axis_life = zip(*lifespan_list)