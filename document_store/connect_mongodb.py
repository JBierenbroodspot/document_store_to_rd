# ------------------------------------------------------------------------------
#  SPDX-License-Identifier: BSD 3-Clause
#  Copyright (c) 2022 Jimmy Bierenbroodspot.
# ------------------------------------------------------------------------------
import os
import logging

import pymongo
from pymongo import database
from pymongo import errors
import dotenv


def connect_to_mongodb(hostname: str | None = None, port: str | None = None) -> None:
    mongo_client: pymongo.MongoClient

    mongo_port: str = port if port else os.getenv("MONGODB_PORT")
    mongo_host: str = hostname if hostname else os.getenv("MONGODB_HOSTNAME")

    mongo_database: pymongo.database.Database | None = None
    connection_string: str = f"mongodb://{mongo_host}:{mongo_port}"

    logging.info("Connecting to database...")

    try:
        mongo_client = pymongo.MongoClient(connection_string)
        mongo_database = mongo_client.op_is_op
    except pymongo.errors.ConnectionFailure as err:
        logging.error("Connection failed", err)

    logging.info(f"Connected to database {mongo_database.name}!")


if __name__ == '__main__':
    dotenv.load_dotenv()
    connect_to_mongodb()
