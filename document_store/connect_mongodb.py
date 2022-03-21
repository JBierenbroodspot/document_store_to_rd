# ------------------------------------------------------------------------------
#  SPDX-License-Identifier: BSD 3-Clause
#  Copyright (c) 2022 Jimmy Bierenbroodspot.
# ------------------------------------------------------------------------------
import typing
import pymongo
from pymongo import database
from pymongo import errors
import dotenv
import os


def main() -> None:
    mongo_client: pymongo.MongoClient
    mongo_database: pymongo.database.Database | None = None
    connection_string: str = f"mongodb://{MONGO_HOST}:{MONGO_PORT}"

    print("Connecting to database...")

    try:
        mongo_client = pymongo.MongoClient(connection_string)
        mongo_database = mongo_client.op_is_op
    except pymongo.errors.ConnectionFailure:
        print("Connection failed")

    print(f"Connected to database {mongo_database.name}!")


if __name__ == '__main__':
    dotenv.load_dotenv()
    MONGO_HOST = os.getenv("MONGODB_HOSTNAME")
    MONGO_PORT = os.getenv("MONGODB_PORT")

    main()
