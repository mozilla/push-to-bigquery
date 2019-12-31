from jx_bigquery import bigquery
from mo_logs import startup, constants, Log


def merge(config):
    container = bigquery.Dataset(config.destination)
    index = container.get_or_create_table(config.destination)
    index.merge_shards()


def main():
    try:
        config = startup.read_settings()
        constants.set(config.constants)
        Log.start(config.debug)
        merge(config.push)
    except Exception as e:
        Log.error("Problem with etl", cause=e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
