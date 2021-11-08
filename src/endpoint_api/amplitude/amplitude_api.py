import requests
import json
from src.config import settings


def send_batch_events(events):
    payload = {"api_key": settings.amplitude.api_key}

    flag = True
    while flag:
        json_events = []
        for event in events:
            json_events.append(
                {
                    "user_id": event.user_id,
                    "event_type": event.name,
                    "time": event.date,
                    "event_properties": event.event_properties,
                    "user_properties": event.user_properties
                }
            )
            if len(json_events) > 201:
                break
        else:
            flag = False

        payload["events"] = json_events

        headers = {
            'Content-Type': 'application/json',
            'Accept': '*/*'
        }
        print(payload['events'][0]['event_type'])

        r = requests.post('https://api2.amplitude.com/batch', data=json.dumps(payload), headers=headers)