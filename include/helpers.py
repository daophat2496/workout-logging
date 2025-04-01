import os
import json

def set_dlt_env_vars(_gcp_hook):
    keyfile_dict = json.loads(_gcp_hook.extras["keyfile_dict"])

    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "asia-southeast1"
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = _gcp_hook.extras["project"]
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = keyfile_dict["private_key"]
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = keyfile_dict["client_email"]
