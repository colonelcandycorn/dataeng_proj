#!/home/sarah/sub_env/bin/python

from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
import json
from logger import Discord_logger
import datetime as dt
import os
import fcntl
import pytz
project_id = os.environ.get("PROJECT_ID")
subscriber_id = os.environ.get("SUBSCRIBER_ID")


class Subscriber:
    def __init__(self, logger: Discord_logger, file_path):
        self._logger = logger
        self._file = file_path

    def sub(self, project_id: str, subscription_id: str) -> None:
        subscriber_client = pubsub_v1.SubscriberClient()

        subscription_path = subscriber_client.subscription_path(project_id, subscription_id)

        def message_parser(message: pubsub_v1.subscriber.message.Message) -> None:
            if message.data is None:
                message.ack()
                return

            decoded_message = message.data.decode("utf-8")

            try:
                json_message = json.loads(decoded_message)
            except json.JSONDecodeError:
                message.ack()
                return

            json_file = open(self._file, 'a', encoding="utf-8")
            try:
                fcntl.flock(json_file, fcntl.LOCK_EX)
                json.dump(json_message, json_file)
                json_file.write("\n")
            finally:
                fcntl.flock(json_file, fcntl.LOCK_UN)
                json_file.close()

            message.ack()

        streaming_pull_future = subscriber_client.subscribe(
            subscription_path, callback=message_parser
        )

        with subscriber_client:
            try:
                streaming_pull_future.result()
            except TimeoutError:
                self._logger.info("Timeout")
                self._logger.send()
                streaming_pull_future.cancel()

if __name__ == '__main__':
    logger = Discord_logger(
        "https://discord.com/api/webhooks/1226677851843989657/tiieQtc6oXsgkkZQb8bc7BT___vgH8H-gHEOiiV_6wPdKlB-wseYFTnupQ4_sb4DefcY")
    # get today's date
    today = dt.datetime.now(pytz.timezone('US/Pacific'))
    today_str = today.strftime("%Y%m%d")
    file_path = f"/home/sarah/breadcrumb_data/{today_str}.ndjson"

    subscriber = Subscriber(logger, f"/home/sarah/breadcrumb_data/{today_str}.ndjson")

    subscriber.sub(project_id, subscriber_id)

