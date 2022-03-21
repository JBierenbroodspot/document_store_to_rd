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

from document_store import connect_mongodb

dotenv.load_dotenv()


def main() -> None:
    _init_logging()
    mongo_database: pymongo.database.Database = connect_mongodb.connect_to_mongodb()
    products_collection: pymongo.collection.Collection = mongo_database.get_collection('products')
    print(products_collection.find_one()['name'], products_collection.find_one()['price']['selling_price'])
    print(products_collection.find_one({'name': ''}))


def _init_logging() -> None:
    """Initializes logging for every module used by this module."""
    logging.basicConfig(filename='document_store.log',
                        filemode='a+',
                        level='DEBUG',
                        datefmt='%d-%b-%y %H:%M:%S',
                        format='%(asctime)s - %(levelname)s - %(message)s')


if __name__ == '__main__':
    main()
