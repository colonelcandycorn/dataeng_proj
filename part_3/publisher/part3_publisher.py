from google.cloud import pubsub_v1
import json
import requests
import os
from io import StringIO
import re
from bs4 import BeautifulSoup
import pandas as pd
# pip command to install google cloud pubsub: pip install google-cloud-pubsub

project_id = os.environ.get("PROJECT_ID")
topic_id = os.environ.get("PART3_TOPIC_ID")

class Breadcrumb_publisher:
    def __init__(self, project_id, topic_id):
        self.client = pubsub_v1.PublisherClient()
        self.topic_path = self.client.topic_path(project_id, topic_id)

    def read_file_and_convert_to_ints(self, filename) -> list[int]:
        """
        This will generate an integer list from a the vehicle_ids.txt file
        :param filename: just a string that is the path to the file
        :return: a list of integers representing the vehicle ids or an empty list if the file is not found
        """
        try:
            with open(filename, 'r') as file:
                lines = file.readlines()

                integers_list = []
                for line in lines:
                    for value in line.strip().split(','):
                        try:
                            integer_value = int(value)
                            integers_list.append(integer_value)
                        except ValueError:
                            continue

                return integers_list
        except FileNotFoundError:
            return []

    # json should be { {"vehicle_id": 3908}, {"vehicle_id": 3909}, {"vehicle_id": 3910} }
    def publish_json_breadcrumbs(self, some_json: dict, project_id: str, topic_id: str) -> int:
        successful_messages = 0
        client = pubsub_v1.PublisherClient()
        topic_path = client.topic_path(project_id, topic_id)

        breadcrumb_list = json.dumps(some_json)

        for breadcrumb in breadcrumb_list:
            data = json.dumps(breadcrumb).encode("utf-8")
            future = client.publish(topic_path, data)
            try:
                future.result()
            except Exception as e:
                print(f"Error publishing message: {e}")
                continue

            successful_messages += 1

        return successful_messages

    def future_callback(self, future):
        try:
            future.result()
        except Exception as e:
            print(f"An error occurred: {e}")

    def publish_json_breadcrumb(self, some_json: dict) -> int:

        data = json.dumps(some_json, indent=4).encode("utf-8")
        future = self.client.publish(self.topic_path, data)
        future.add_done_callback(self.future_callback)

        return 1
    
def html_to_breadcrumb(html: str) -> list[dict]:
    """
    This function will take in an html string and return a list of breadcrumbs
    :param html: a string of html
    :return: a list of breadcrumb dictionaries
    """
    soup = BeautifulSoup(html, 'lxml')
    breadcrumbs = []
    trip_pattern = re.compile(r'\d+')
    trip_ids = []

    for trip in soup.find_all('h2'):
        if 'Stop events for PDX_TRIP' in trip.text:
            trip_id = trip_pattern.search(trip.text).group()
            trip_ids.append(trip_id)

    for table in soup.find_all('table'):
        df = pd.read_html(StringIO(str(table)))[0]

        # add trip id to dataframe
        df['trip_id'] = trip_ids.pop(0)

        # we only need 1 row
        row = df.iloc[0]
        breadcrumbs.append(row.to_dict())

    return breadcrumbs

if __name__ == '__main__':
    publisher = Breadcrumb_publisher(project_id, topic_id)
    vehicle_ids = publisher.read_file_and_convert_to_ints("/home/sarah/class_project/vehicle_ids.txt")
    if not vehicle_ids:
        print("No vehicle ids found in file")
        exit(1)

    for vehicle_id in vehicle_ids:
        api = f"https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={vehicle_id}"
        r = requests.get(api)
        if not r.ok:
            print(f"Failed to get breadcrumbs for vehicle id: {vehicle_id}")
            continue

        try:
            breadcrumbs = html_to_breadcrumb(r.text)
            successful_messages = 0
            for breadcrumb in breadcrumbs:
                successful_messages += 1 if publisher.publish_json_breadcrumb(breadcrumb) == 1 else 0
        except Exception as e:
            print(f"Error processing breadcrumbs for vehicle id: {vehicle_id}")
            continue

        print(f"Published {successful_messages} messages for vehicle id: {vehicle_id}")

