from google.cloud.bigquery import Client
from google.oauth2 import service_account

from mo_json import json2value
from mo_threads import Queue, Thread
from pyLibrary.env import http

from pyLibrary.convert import zip2bytes

from mo_logs.strings import expand_template

from jx_bigquery import bigquery
from mo_logs import startup, constants, Log, Except


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

    container = bigquery.Dataset(config.destination)

    # index = container.get_or_create_table("testing", sharded=True)

    # index.merge_shards()

    index = container.get_or_create_table(config.destination)

    base_url = "https://active-data-treeherder-normalized.s3-us-west-2.amazonaws.com/{{major}}.{{minor}}.json.gz"
    major = 1776
    minor = 0

    NUM_THREADS = 4
    queue = Queue("data", max=NUM_THREADS)

    def extender(please_stop):
        while not please_stop:
            try:
                data = queue.pop(till=please_stop)
                try:
                    index.extend(json2value(l.decode('utf8')) for l in data.split(b"\n") if l)
                except Exception as e:
                    if "Request payload size exceeds the limit" in e:
                        lines = list(data.split(b"\n"))
                        cut = len(lines)//2
                        queue.add(b"\n".join(lines[:cut]), force=True)
                        queue.add(b"\n".join(lines[cut:]), force=True)
                        continue
                    Log.warning("could not add", cause=e)
            except Exception as e:
                Log.warning("Faliure", cause=e)
    threads = [
        Thread.run("extender"+str(i), extender)
        for i in range(NUM_THREADS)
    ]

    while True:
        url = expand_template(base_url, {"major": major, "minor": minor})
        Log.note("add {{url}}", url=url)
        try:
            data = zip2bytes(http.get(url, retry={"times": 3, "sleep": 2}).all_content)
            queue.add(data)
        except Exception as e:
            e = Except.wrap(e)
            if "Not a gzipped file" in e:
                minor = 0
                major += 1
                continue
            Log.warning("could not read {{url}}", url=url, cause=e)
        minor += 1


def main():
    try:
        config = startup.read_settings()
        constants.set(config.constants)
        Log.start(config.debug)
        push(config.push)
    except Exception as e:
        Log.error("Problem with etl", cause=e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
