import requests

class Discord_logger:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url
        self._errors = []
        self._info = []

    def error(self, message):
        self._errors.append(message)

    def info(self, message):
        self._info.append(message)

    def send(self):
        payload_errors = {
            "content": "<@136966818886582273>  \n" + "\n".join(self._errors),
        }


        payload_info = {
            "content": "<@136966818886582273>  \n" + "\n".join(self._info),
        }


        r  = requests.post(self.webhook_url, json=payload_errors)
        if not r.ok:
            print("Error sending error message")
            print(r.text)
        r2 = requests.post(self.webhook_url, json=payload_info)
        if not r2.ok:
            print("Error sending info message")
            print(r2.text)

