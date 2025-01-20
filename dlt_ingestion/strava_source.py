import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dlt_ingestion.strava_helper.auth import get_access_token

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
        yield from client.paginate("/athlete/activities?per_page=2")

    @dlt.transformer(data_from=activities, write_disposition="merge", primary_key="id")
    def activity_details(activities):
        for activity in activities:
            res = client.get(f"/activities/{activity['id']}?include_all_efforts=false")
            yield res.json()
    
    @dlt.transformer(data_from=activities, write_disposition="merge", primary_key="id", max_table_nesting=0)
    def gears(activities):
        for activity in activities:
            res = client.get(f"/gear/{activity['gear_id']}")
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