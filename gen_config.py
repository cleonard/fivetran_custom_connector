"""Create the configuration.json file required by Fivetran from .env"""

import json

from dotenv import dotenv_values


def gen():
    config = dotenv_values(".env")
    with open("configuration.json", "w") as fp:
        json.dump(config, fp)


if __name__ == "__main__":
    gen()
