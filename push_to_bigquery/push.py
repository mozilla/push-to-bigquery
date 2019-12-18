from google.cloud.bigquery import Client
from google.oauth2 import service_account

from mo_json import json2value
from pyLibrary.convert import zip2bytes

from pyLibrary.env import http

from jx_bigquery import bigquery
from mo_logs import startup, constants, Log


def example(config):
    creds = service_account.Credentials.from_service_account_info(config.bigquery)
    client = Client(project=config.bigquery.project_id, credentials=creds)

    query = """
        SELECT name, SUM(number) as total_people
        FROM `bigquery-public-data.usa_names.usa_1910_2013`
        WHERE state = 'TX'
        GROUP BY name, state
        ORDER BY total_people DESC
        LIMIT 20
    """
    query_job = client.query(query)  # Make an API request.

    print("The query data:")
    for row in query_job:
        # Row values can be accessed by field name or index.
        print("name={}, count={}".format(row[0], row["total_people"]))


def push(config):

    data = zip2bytes(http.get("https://active-data-treeherder-normalized.s3-us-west-2.amazonaws.com/1600.0.json.gz").all_content)

    container = bigquery.Container(name="treeherder", kwargs=config.bigquery)
    index = container.get_or_create_index(name="jobs", schema={}, typed=True)
    index = container.create_or_replace_index(name="jobs", schema={}, typed=True)

    index.extend(map(lambda l: json2value(l.decode('utf8')), data.split("\n")))


def main():
    try:
        config = startup.read_settings()
        constants.set(config.constants)
        Log.start(config.debug)
        push(config)
    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
