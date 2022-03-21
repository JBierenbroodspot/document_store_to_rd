# ----------------------------------------------------------------------------------------------------------------------
#  SPDX-License-Identifier: BSD 3-Clause                                                                               -
#  Copyright (c) 2022 Jimmy Bierenbroodspot.                                                                           -
# ----------------------------------------------------------------------------------------------------------------------
"""Connects to a mongodb database."""
import os
import logging

import pymongo
from pymongo import database
from pymongo import errors
import dotenv


def connect_to_mongodb(hostname: str | None = None,
                       port: str | None = None,
                       db_name: str | None = None) -> pymongo.database.Database:
    """Connects to a MongoDB database.

    :param hostname: The name of the host that provides your database.
    :param port: The port to your database provider (27017 if you're using localhost).
    :param db_name: The name of the database you want to select.
    :return: A pymongo.database.Database object containing
    """
    mongo_client: pymongo.MongoClient
    mongo_database: pymongo.database.Database

    mongo_port: str = port if port else os.getenv("MONGODB_PORT")
    mongo_host: str = hostname if hostname else os.getenv("MONGODB_HOSTNAME")
    mongo_db_name: str = db_name if db_name else os.getenv("DATABASE_NAME")

    connection_string: str = f"mongodb://{mongo_host}:{mongo_port}"

    logging.info("Connecting to database...")

    # Try connecting to the database
    try:
        mongo_client = pymongo.MongoClient(connection_string)
        mongo_database = mongo_client.get_database(mongo_db_name)
    except pymongo.errors.ConnectionFailure as err:
        # Catch a connection error, log it and throw the error again because program should seize when this happens.
        logging.error("Connection failed", err)
        raise(pymongo.errors.ConnectionFailure(err))

    logging.info(f"Connected to database {mongo_database.name}!")

    return mongo_database


if __name__ == '__main__':
    dotenv.load_dotenv()
    connect_to_mongodb()
