from google.cloud import bigquery
from google.oauth2 import service_account

from mo_logs import startup, constants, Log


def example(config):
    creds = service_account.Credentials.from_service_account_info(config.bigquery)
    client = bigquery.Client(project=config.bigquery.project_id, credentials=creds)

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


def main():
    try:
        config = startup.read_settings()
        constants.set(config.constants)
        Log.start(config.debug)
        example(config)
    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
