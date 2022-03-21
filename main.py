# ----------------------------------------------------------------------------------------------------------------------
#  SPDX-License-Identifier: BSD 3-Clause                                                                               -
#  Copyright (c) 2022 Jimmy Bierenbroodspot.                                                                           -
# ----------------------------------------------------------------------------------------------------------------------
"""Main entry point of application."""
import typing
import logging

import dotenv
import pymongo
from pymongo import database  # Pymongo is imported again because IDE won't recognize database.

from document_store import mongodb_controller

dotenv.load_dotenv()


def main() -> None:
    mongo_database: pymongo.database.Database = mongodb_controller.connect_to_mongodb()
    products_collection: pymongo.collection.Collection = mongo_database.get_collection('products')
    sessions_collection: pymongo.collection.Collection = mongo_database.get_collection('sessions')
    sessions_collection: pymongo.collection.Collection = mongo_database.get_collection('visitors')

    _init_logging()


def _init_logging() -> None:
    """Initializes logging for every module used by this module."""
    logging.basicConfig(filename='document_store.log',
                        filemode='a+',
                        level='DEBUG',
                        datefmt='%d-%b-%y %H:%M:%S',
                        format='%(asctime)s - %(levelname)s - %(message)s')


if __name__ == '__main__':
    main()
