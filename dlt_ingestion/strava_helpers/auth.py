import requests
import dlt

def get_access_token():
    response = requests.post(
        f"{dlt.config.get('strava.api_url')}/oauth/token",
        data = {
            "client_id": dlt.secrets.get("strava.client_id"),
            "client_secret": dlt.secrets.get("strava.client_secret"),
            "refresh_token": dlt.secrets.get("strava.refresh_token"),
            "grant_type": "refresh_token",
        },
    )
    response.raise_for_status()
    return response.json()["access_token"]