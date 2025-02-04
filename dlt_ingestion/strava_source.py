import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dlt_ingestion.strava_helpers.auth import get_access_token
import dlt_ingestion.helpers.util as Util

@dlt.source
def strava_source():
    access_token = get_access_token()
    client = RESTClient(
        base_url=dlt.config.get("strava.api_url")
        , auth=BearerTokenAuth(access_token)
        , paginator=PageNumberPaginator(page_param="page", base_page=1, maximum_page=3, total_path=None)
    )

    @dlt.resource(write_disposition="merge", primary_key="id", max_table_nesting=0)
    def activities():
        # Load 7 days activities in order to capture updated activities
        # Strava does not provide updated_at-like field
        from_epoch = Util.get_sod_n_days_from_now(-7)
        yield from client.paginate(f"/athlete/activities?per_page=5&after={from_epoch}")

    @dlt.transformer(data_from=activities, write_disposition="merge", primary_key="id")
    def activity_details(activities):
        for activity in activities:
            res = client.get(f"/activities/{activity['id']}?include_all_efforts=false")
            yield res.json()
    
    @dlt.transformer(data_from=activities, write_disposition="merge", primary_key="id", max_table_nesting=0)
    def gears(activities):
        for activity in activities:
            if activity["gear_id"] is None:
                continue

            res = client.get(f"/gear/{activity['gear_id']}")
            res.raise_for_status()
            print(res.json())
            yield res.json()

    return [activities, activity_details, gears]

def load_strava_activities():
    pipeline = dlt.pipeline(
        pipeline_name="strava_pipeline"
        # , destination=dlt.destinations.duckdb(dlt.config.get("duckdb.file_path"))
        , destination="bigquery"
        , dataset_name="raw_data"
    )

    # print(dlt.config.get("duckdb.file_path"))
    load_info = pipeline.run(strava_source())
    print(load_info)

if __name__ == "__main__":
    # run our main example
    load_strava_activities()