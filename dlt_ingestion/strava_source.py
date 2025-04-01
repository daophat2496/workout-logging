import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dlt_ingestion.strava_helpers.auth import get_access_token
import dlt_ingestion.helpers.util as Util
from datetime import datetime, timedelta
import time

def strava_source(latest_timestamp=None):
    access_token = get_access_token()
    client = RESTClient(
        base_url=dlt.config.get("strava.api_url"),
        auth=BearerTokenAuth(access_token),
        paginator=PageNumberPaginator(page_param="page", base_page=1, total_path=None)
    )
    
    if latest_timestamp is None:
        latest_timestamp = datetime(2024, 2, 1)  # Default timestamp
    
    after = int((latest_timestamp - timedelta(days=7)).timestamp())
    before = int(min(latest_timestamp + timedelta(days=60), datetime.now(latest_timestamp.tzinfo)).timestamp())

    @dlt.resource(write_disposition="merge", primary_key="id", max_table_nesting=0)
    def activities():
        yield client.paginate(f"/athlete/activities?per_page=100&after={after}&before={before}")
        time.sleep(5)  # Small delay to avoid rate limits
    
    @dlt.transformer(data_from=activities, write_disposition="merge", primary_key="id")
    def activity_details(activities):
        for activity in activities:
            res = client.get(f"/activities/{activity['id']}?include_all_efforts=false")
            time.sleep(1)  # Small delay to avoid rate limits
            yield res.json()
    
    return [activities, activity_details]

def load_strava_activities(latest_timestamp=None, bigquery_dataset_name="strava_raw_data"):
    pipeline = dlt.pipeline(
        pipeline_name="strava_pipeline",
        destination="bigquery",
        dataset_name=bigquery_dataset_name
    )
    
    load_info = pipeline.run(strava_source(latest_timestamp))
    print(load_info)

if __name__ == "__main__":
    load_strava_activities()
