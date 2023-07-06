import csv
import re
import sys
import matplotlib.pyplot as plt
import seaborn
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from collections import Counter
from datetime import datetime, date
from dateutil import parser
import statistics
import numpy as np
import warnings

def analysis_prep(all_urls_final):
    keys_to_pop_1 = ['api_base_url',
               'site_title',
               'version',
               'locale',
               'extensions',
               'package_list_count',
               'organization_list_count',
               'tag_list_count',
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
    all_urls_final_df = pd.DataFrame(all_urls_final)
    all_urls_final_df.to_csv("all_urls_final-jul5.csv")
    return all_urls_final

def package_counts(all_urls_final):
    count_of_instances_with_packages = 0
    packages_counts = []
    count_of_datasets_below_1000 = 0
    count_of_datasets_above_50k = 0
    for item in all_urls_final:
        try:
            if item['package_count']:
                count_of_instances_with_packages += 1
                packages_counts.append(int(item['package_count']))
                if int(item['package_count']) < 1001:
                    count_of_datasets_below_1000 += 1
                if int(item['package_count']) > 50000:
                    count_of_datasets_above_50k += 1
        except KeyError as e:
            pass
    package_median = statistics.median(packages_counts)
    export_dict = {"count_of_instances_with_packages": count_of_instances_with_packages,
                   "package_median": package_median,
                   "count_of_datasets_below_1000": count_of_datasets_below_1000,
                   "count_of_datasets_above_50k": count_of_datasets_above_50k
    }
    fieldnames=export_dict.keys()
    with open("package_stats.csv", "w", encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerow(export_dict)

def ckan_packages_chart(all_urls_final):
    packages = []
    for row in all_urls_final:
        try:
            package_count = row['package_count']
            if package_count != '':
                packages.append(int(package_count))
        except KeyError:
            pass

    sorted_sizes_adjusted = sorted([1 if s == 0 else s for s in packages], reverse=True)
    sizes_plus_ranks = {'rank': range(len(sorted_sizes_adjusted)), 'package_count': sorted_sizes_adjusted}

    warnings.filterwarnings("ignore", category=UserWarning)
    fig, ax = plt.subplots(figsize=(10,10))
    seaborn.histplot(data=sizes_plus_ranks, x='package_count', log_scale=(True, False))
    ax.set_xlabel("Count of Datasets per Instance")
    ax.set_ylabel("Number of Instances")
    plt.xticks(rotation=45)
    ax.set_xticklabels(['0', '.1', '1', '10', '100', '1000', '10,000', '100,000', '1,000,000'])
    plt.title("Fig. 1: Bar Chart of CKAN Instance Size (Measured in Number of Datasets)\n", size=8)
    plt.savefig("packages_chart.png")

def time_calcs(all_urls_final):
    year_list = []
    time_dict = {}
    today = datetime.today()
    url_list = []
    overall_count = 0
    for y in range(2007, 2024):
        annual_count = 0
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
                                    annual_count+=1
                                    overall_count+=1
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
        time_dict[y] = {"year":y, "count": annual_count, "avg_age":avg_age, "median":median_age, "average_lifespan":age_pct_amt}
    return(time_dict, overall_count)

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

    fig, ax = plt.subplots(figsize=(8, 8))
    bars = ax.bar(x_axis_count, y_axis_count)
    ax.set_xlabel("Year Launched", fontsize=12)
    ax.set_ylabel("Count of Instances", fontsize=12)
    x_list = [2007, 2011, 2013, 2015, 2017, 2019, 2021, 2023]
    x = np.array(x_list)
    plt.xticks(x, fontsize=12)
    ax.bar_label(bars)
    ax.set_xlim(2006, 2024)
    plt.title("Fig. 3: Count of CKAN Instances Launched Per Year, 2007-2023\n", size=8)
    plt.savefig("ckan_per_year.png")

    fig, ax = plt.subplots(figsize=(8, 8))
    bars = ax.bar(x_axis_life, y_axis_life, color="#1f77b4")
    ax.set_xlabel("\nYear Launched", fontsize=12)
    ax.set_ylabel("Average Percent of Lifetime Spent Active\n", fontsize=12)
    ax.tick_params(axis='y', labelsize=10)
    x_list = [2007, 2011, 2013, 2015, 2017, 2019, 2021, 2023]
    x = np.array(x_list)
    labels = [str(item) for item in x_axis_life]
    plt.xticks(x, fontsize=12)
    rects = ax.patches
    for rect, label in zip(rects, y_axis_count):
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2, height + 0.01, label, ha="center", va="bottom")
    fig.savefig("test.png", bbox_inches="tight")
    plt.title("Fig. 4: Average Percent of CKAN Instance 'Lifetime' Spent Active, by Year of Release\n", size=8)
    plt.savefig("ckan_lifetime.png")

def ckan_version(all_urls_final):
    version_list = []
    for item in all_urls_final:
        try:
            version_list.append(item['version'].split('.')[:2])
        except KeyError:
            pass

    for item in version_list:
        if "b" in item[1]:
            item[1] = item[1].replace("b", "")

    version_3_list = [item for item in version_list if item[0] == '3']
    for item in version_3_list:
        version_list.remove(item)

    version_3_list_final = [item for item in version_3_list if item[1] != "0#datapress"]
    a, b = zip(*version_list)
    version_count = Counter(b)
    version_dict = {f'2.{item}': version_count[item] for item in version_count}

    if len(version_3_list_final) > 0:
        c, d = zip(*version_3_list_final)
        version_3_count = Counter(d)
        version_3_dict = {f'3.{item}': version_3_count[item] for item in version_3_count}
        for item in version_3_dict:
            version_dict[item] = version_3_dict[item]
    x_data = list(sorted(version_dict))
    if '2.10' in x_data:
        if x_data[-1] == '2.10':
            pass
        elif x_data[2] == '2.10':
            x_data.pop(2)
            if x_data[-1] == '2.9':
                x_data.append('2.10')
            else:
                for x in range(len(x_data)):
                    if x_data[x][0] == '3':
                        insert_point = x
                        break
                x_data.insert(insert_point, '2.10')
    y_data = [version_dict[item] for item in x_data]
    return(x_data, y_data)

def ckan_version_chart(x_data, y_data):
    fig, ax = plt.subplots(figsize=(8,8))
    bars = ax.bar(x_data, y_data, color="#1f77b4")
    ax.set_xlabel("\nCKAN Version", fontsize=12)
    ax.set_ylabel("Count of CKAN Instances\n", fontsize=12)
    ax.tick_params(axis='y', labelsize=10)
    labels = [str(item) for item in x_data]
    plt.xticks(labels, fontsize=12)
    rects = ax.patches
    for rect, label in zip(rects, y_data):
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2, height+ 0.01, label, ha="center", va="bottom")
    fig.savefig("test.png", bbox_inches="tight")
    plt.title("Fig. 5: Count of Analyed CKAN Instances Using CKAN Versions 2.0-2.10\n", size=8)
    plt.savefig("version_count.png")

file_name = sys.argv[1]

reader = csv.DictReader(open(file_name, 'r', encoding='utf-8'))
packages = []
all_urls_final = []
for row in reader:
    all_urls_final.append(row)

prepped_file = analysis_prep(all_urls_final)
package_counts(prepped_file)
ckan_packages_chart(prepped_file)
time_dict, overall_count = time_calcs(prepped_file)
graphing(time_dict)
x_data, y_data = ckan_version(prepped_file)
ckan_version_chart(x_data, y_data)


# def time_calcs(all_urls_final):
#     year_list = []
#     time_dict = {}
#     today = datetime.today()
#     url_list = []
#     overall_count = 0
#     for y in range(2007, 2024):
#         annual_count = 0
#         age = 0
#         timedeltas = []
#         age_pct = []
#         for item in all_urls_final:
#             try:
#                 if 'oldest_metadata_created_date' in item.keys():
#                     if 'most_recent_update_date' in item.keys():
#                         if type(item['oldest_metadata_created_date']) == datetime:
#                             if type(item['most_recent_update_date']) == datetime:
#                                 year_list.append((item['oldest_metadata_created_date'], item['most_recent_update_date']))
#                                 if int(item['oldest_metadata_created_date'].strftime('%Y')) == y:
#                                     annual_count+=1
#                                     overall_count+=1
#                                     oldest = item['oldest_metadata_created_date']
#                                     most_recent = item['most_recent_update_date']
#                                     timedeltas.append(int((most_recent-oldest).days))
#                                     age_pct.append(int((most_recent-oldest).days) / int((today - oldest).days))
#                                     url_list.append(item['source_url'])
#
#             except Exception as e:
#                 print(item['oldest_metadata_created_date'], item['source_url'], e)
#         if len(timedeltas)>0:
#             avg_age = round((sum(timedeltas)/len(timedeltas))/365, 2)
#             median_age = round(statistics.median(timedeltas)/365, 2)
#         else:
#             avg_age = 0
#             median_age = 0
#         if len(age_pct)>0:
#             age_pct_amt = round(((sum(age_pct))/(len(age_pct))), 2)
#         else:
#             age_pct_amt = 0
#         time_dict[y] = {"year":y, "count": annual_count, "avg_age":avg_age, "median":median_age, "average_lifespan":age_pct_amt}
#
#         return(time_dict, overall_count)

