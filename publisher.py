
from google.cloud import pubsub_v1
import json
import requests
import os
# pip command to install google cloud pubsub: pip install google-cloud-pubsub

project_id = os.environ.get("PROJECT_ID")
topic_id = os.environ.get("TOPIC_ID")


class Discord_logger:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url
        self._errors = []
        self._info = []
        self._published_vehicles = 0

    def error(self, message):
        self._errors.append(message)

    def info(self, message):
        self._info.append(message)

    def send(self):
        if len(self._errors) != 0:
            payload_errors = {
                "content": "<@136966818886582273>  \n" + "\n".join(self._errors),
            }
        else:
            payload_errors = {
                "content": "<@136966818886582273>  \n" + "No errors",
            }


        if len(self._info) != 0:
            payload_info = {
                "content": "<@136966818886582273>  \n" + "\n".join(self._info),
            }
        else:
            payload_info = {
                "content": "<@136966818886582273>  \n" + "No info",
            }

        self._published_vehicles += len(self._info) + len(self._errors)

        payload_percentage = {
            "content": "<@136966818886582273>  \n" + f"We are {self._published_vehicles}% done",
        }

        self._errors.clear()
        self._info.clear()

        r  = requests.post(self.webhook_url, json=payload_errors)
        if not r.ok:
            print("Error sending error message")
            print(r.text)
        r2 = requests.post(self.webhook_url, json=payload_info)
        if not r2.ok:
            print("Error sending info message")
            print(r2.text)
        r3 = requests.post(self.webhook_url, json=payload_percentage)
        if not r3.ok:
            print("Error sending percentage message")
            print(r3.text)

class Breadcrumb_publisher:
    def __init__(self, logger, project_id, topic_id):
        self.logger = logger
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
                self.logger.error(f"Error publishing message: {e}")
                continue

            successful_messages += 1

        self.logger.info(f"Successfully published {successful_messages} messages")
        return successful_messages

    def publish_json_breadcrumb(self, some_json: dict) -> int:

        data = json.dumps(some_json, indent=4).encode("utf-8")
        future = self.client.publish(self.topic_path, data)
        try:
            future.result()
        except Exception as e:
            self.logger.error(f"Error publishing message: {e}")
            return 0

        return 1

if __name__ == '__main__':
    logger = Discord_logger("https://discord.com/api/webhooks/1226677851843989657/tiieQtc6oXsgkkZQb8bc7BT___vgH8H-gHEOiiV_6wPdKlB-wseYFTnupQ4_sb4DefcY")
    publisher = Breadcrumb_publisher(logger, project_id, topic_id)
    vehicle_ids = publisher.read_file_and_convert_to_ints("/home/sarah/class_project/vehicle_ids.txt")
    if not vehicle_ids:
        logger.error("No vehicle ids found in file")
        logger.send()
        exit(1)

    for vehicle_id in vehicle_ids:
        api = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
        r = requests.get(api)
        if not r.ok:
            logger.error(f"Error getting breadcrumbs for vehicle id: {vehicle_id}")
            continue

        breadcrumbs = r.json()
        successful_messages = 0
        for breadcrumb in breadcrumbs:
            successful_messages += 1 if publisher.publish_json_breadcrumb(breadcrumb) == 1 else 0

        logger.info(f"Published {successful_messages} messages for vehicle id: {vehicle_id}")

        logger.send()

