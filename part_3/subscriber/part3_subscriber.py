import pandas as pd
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
import json
import os
from src.breadcrumb_processor import BreadCrumbProcessor
from src.postgres_connector import PostgresConnector
from threading import Thread, Lock

project_id = os.environ.get("PROJECT_ID")
subscriber_id = os.environ.get("PART3_SUBSCRIBER_ID")
MAX_BREADCRUMB = 20
MAX_TIMEOUT = 60


class Subscriber:
    """
    Subscriber class that listens for messages from a Google Pub/Sub subscription. It calls the breadcrumb processor
    to process the messages and appends the processed data to the Postgres database using the PostgresConnector. It
    takes in a logger, file path, and a PostgresConnector object as parameters. I chose to add the PostgresConnector
    and logger as parameters to the constructor to allow for dependency injection. This way, the class is more flexible
    but I doubt this flexibility will be needed in the future. I also added a clean_up method to finalize the processing
    and send the logs to Discord.
    """

    def __init__(self, postgres_connector: PostgresConnector):
        self._postgres_connector = postgres_connector
        self._processed_breadcrumbs = pd.DataFrame()
        self._lock = Lock()
        self._bad_breadcrumbs = 0

    def _finalize_and_send(self):
        if self._processed_breadcrumbs is None:
            return
        
        print(f"Appending {self._processed_breadcrumbs.shape[0]} breadcrumbs to part3 table")

        self._postgres_connector.upsert_to_trip(self._processed_breadcrumbs)

        self._processed_breadcrumbs = None


    def clean_up(self):
        """
        Right now this method just calls the finalize_and_send method. I made it a separate method in case I need to add
        more clean up steps in the future.
        :return: None
        """
        self._finalize_and_send()

    def sub(self, project_id: str, subscription_id: str) -> None:
        """
        This method listens for messages on a Google Pub/Sub subscription. It calls the message_parser method to process
        the messages. It takes in a project_id and subscription_id as parameters. In the previous version of this code,
        the message parser would do a ton of I/O operations. I refactored the code to only do I/O operations when the
        processed_breadcrumbs DataFrame reaches a certain size. This should improve performance. I also added a timeout
        to the streaming_pull_future.result method to prevent the program from hanging indefinitely. Once the timeout
        is reached, the program will cancel the streaming_pull_future and exit. Main will then call the clean_up method
        to make sure any remaining breadcrumbs are processed and sent to the database.
        :param project_id:
        :param subscription_id:
        :return: None
        """
        subscriber_client = pubsub_v1.SubscriberClient()

        subscription_path = subscriber_client.subscription_path(project_id, subscription_id)

        def message_parser(message: pubsub_v1.subscriber.message.Message) -> None:
            if message.data is None:
                message.ack()
                return

            decoded_message = message.data.decode("utf-8")

            # This is a little inefficient because I'm doing the processing once on the raw data and then again on the
            # processed data. Currently, the publisher is the real bottleneck so I'm not too worried about this.
            try:
                json_message = json.loads(decoded_message)
            except json.JSONDecodeError:
                message.ack()
                return

            try:
                breadcrumb_df = BreadCrumbProcessor.process_individual_part3(json_message)
            except Exception as e:
                print(f"Error processing message: {e}")
                message.ack()
                return

            if breadcrumb_df is None:
                self._lock.acquire()
                self._bad_breadcrumbs += 1
                if self._bad_breadcrumbs > 1000:
                    print("Too many bad breadcrumbs")
                self._lock.release()
                message.ack()
                return

            if breadcrumb_df is not None:
                try:
                    self._lock.acquire()
                    if self._processed_breadcrumbs is None:
                        self._processed_breadcrumbs = breadcrumb_df
                    else:
                        self._processed_breadcrumbs = pd.concat([self._processed_breadcrumbs, breadcrumb_df])
                    if self._processed_breadcrumbs.shape[0] > MAX_BREADCRUMB:
                        self._finalize_and_send()
                except Exception as e:
                    print(f"Error processing message: {str(e)}")
                    message.ack()
                    return
                finally:
                    self._lock.release()

            message.ack()

        streaming_pull_future = subscriber_client.subscribe(
            subscription_path, callback=message_parser
        )

        print(f"Listening for messages on {subscription_path}..\n")

        with subscriber_client:
            try:
                streaming_pull_future.result(timeout=MAX_TIMEOUT)
            except TimeoutError:
                streaming_pull_future.cancel()


if __name__ == '__main__':
    subscriber = Subscriber(PostgresConnector())

    subscriber.sub(project_id, subscriber_id)

    print("We exited the subscriber loop")

    subscriber.clean_up()

