import snowflake.connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Connector
import requests
from pathlib import Path
import traceback
import datetime
import json
os


INIT_DELTA = datetime.timedelta(days=2)
BASE_API_URL = "https://newsapi.org/v2/everything"
TL_CONF = {"include_segments": False}
TL_URL = "https://app.tabulalingua.com/v0/standard/"
TS_FORMAT = "%Y-%m-%dT%H:%M:%S"
CWD = Path.cwd()

UTC = datetime.timezone.utc


def schema(configuration: dict):
    """Defines schema in the destination/store database. Not required if the
    proper schema can be inferred. I'm being explicit because A) I prefer to be explicit,
    and, B) I'm adding values to the source schema schema.
    """

    return [
        {
            "table": "article",
            "primary_key": ["source", "published_at"],
            "columns": {
                "source": "STRING",
                "published_at": "UTC_DATETIME",
                "author": "STRING",
                "title": "STRING",
                "description": "STRING",
                "content": "STRING",
                "url": "STRING",
                # Tabula Lingua analysis values
                "blue": "FLOAT",
                "red": "FLOAT",
                "p0": "FLOAT",
                "p1": "FLOAT",
                "p2": "FLOAT",
                "p3": "FLOAT",
                "p4": "FLOAT",
                "p5": "FLOAT",
                "p6": "FLOAT",
                "p7": "FLOAT",
            },
        }
    ]

# Utility functions


def get_last_published_at(conf):
    """In Fivetrans's code examples, they create queries based off a state object that
    stores the most recent call params. (Write this later -> "latest published_at")"""
    cnx = snowflake.connector.connect(
        user=conf["SNOWFLAKE_USER"],
        password=conf["SNOWFLAKE_PASSWORD"],
        account=conf["SNOWFLAKE_ACCOUNT"],
        warehouse=conf["SNOWFLAKE_WAREHOUSE"],
        database=conf["SNOWFLAKE_DATABASE"],
        schema=conf["SNOWFLAKE_SCHEMA"],
    )
    result = None
    try:
        table = "tester.article" if "/Users/chris" in str(CWD) else "article"
        q = f"select max(published_at) from {table}"
        result = cnx.cursor().execute(q)[0]
    except:
        pass

    if not result:
        return None
    return result.strftime(TS_FORMAT)


def camel(title):
    """String to camel case"""
    parts = title.split("_")
    reassembled = "".join([p.title() for p in parts])
    return reassembled[0].lower() + reassembled[1:]

# Main show


def update(configuration: dict, state: dict):
    """Main entry point expected by Fivetran. Can be a standalone function or part of a
    larger module. I'm using it as a setup conductor for now with some other function
    doing the API calls/persistance.
    """
    conf = configuration
    now = datetime.datetime.now(UTC)

    # Query start:
    # Mininum of (1) whatever Fivetrans stores in it's state, (2) last 'published_at'
    # value in the data warehouse, (3) Safety/fallback of NOW() - 2 days
    from_ts = state.get("to_ts")  # Stored by Fivetrans
    last_published_at = get_last_published_at(
        conf)  # Max published_at in datastore
    safety = (now - INIT_DELTA).strftime(TS_FORMAT)  # Fallback (two days ago)
    start_options = [from_ts, last_published_at, safety]
    query_start = min(filter(None, start_options))

    # Query end
    query_end = now.strftime(TS_FORMAT)

    # API query params
    params = {
        "from": query_start,
        "to": query_end,
        "page": "1",
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": conf["PAGE_SIZE"],
        "page": "1",
    }

    headers = {
        "Authorization": f"Bearer {conf['NEWS_API_KEY']}",
        "Accept": "application/json",
    }

    try:
        topics = conf["TOPIC"].split(",")
        for topic in topics:
            params["q"] = topic
            yield from sync_items(headers, params, state, topic, conf)
        new_state = {"to_ts": params["to"]}
        log.fine(f"state updated, new state: {repr(new_state)}")
    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = (
            f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        )
        raise RuntimeError(detailed_message)

    # Save the progress by checkpointing the state.
    yield op.checkpoint(state=new_state)


def sync_items(headers, params, state, topic, conf):
    response = requests.get(
        BASE_API_URL,
        headers=headers,
        params=params,
    )
    articles = response.json().get("articles")
    for article in articles:
        data = {
            "topic": topic,
            "source": article["source"]["name"],
            "published_at": article["publishedAt"],
            "author": article["author"],
            "title": article["title"],
            "description": article["description"],
            "content": article["content"],
            "url": article["url"],
        }

        # Call Tabula Lingua API to add linguistic analyses values to record:
        # - In prod, this should be a seperate service that connects to Snowflake
        #   and perhaps receives a trigger/webhook from this connector
        try:
            tl_key = conf["TABULA_KEY"]
            content = data.get("content").strip()
            if not content:
                err_msg = "Content is blank or doesn't exist in NewsAPI response"
                raise ValueError(err_msg)

            body = {"config": TL_CONF, "text": content}
            headers = {"Auth": tl_key, "accept": "application/json"}
            tl_response = requests.post(TL_URL, headers=headers, json=body)
            tl_response.raise_for_status()
            tl_data = tl_response.json()["data"]["document"]
            data["blue"] = tl_data["blue"]
            data["red"] = tl_data["red"]
            data["p0"] = tl_data["p_values"][0]
            data["p1"] = tl_data["p_values"][1]
            data["p2"] = tl_data["p_values"][2]
            data["p3"] = tl_data["p_values"][3]
            data["p4"] = tl_data["p_values"][4]
            data["p5"] = tl_data["p_values"][5]
            data["p6"] = tl_data["p_values"][6]
            data["p7"] = tl_data["p_values"][7]

        except Exception as err:
            # Log error response
            exception_class = err.__class__.__name__
            exception_message = str(err)
            stack_trace = traceback.format_exc()
            detailed_message = (
                f"TL Error: {exception_class} - {exception_message}\n"
                f"Stack Trace:\n{stack_trace}"
            )
            log.warning(detailed_message)

        yield op.upsert(table="article", data=data)

    yield op.checkpoint(state)


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    config_file = CWD / "configuration.json"
    if not config_file.exists():
        from gen_config import gen  # noqa
        gen()

    with open("configuration.json") as fp:
        config = json.load(fp)

    connector.debug(configuration=config)
