# ----------------------------------------------------------------------------------------------------------------------
#  SPDX-License-Identifier: BSD 3-Clause                                                                               -
#  Copyright (c) 2022 Jimmy Bierenbroodspot.                                                                           -
# ----------------------------------------------------------------------------------------------------------------------
"""Main entry point of application."""
import os
import typing
import logging
import json

import dotenv
import pymongo
from pymongo import database  # Pymongo is imported again because IDE won't recognize database.

from document_store import mongodb_controller

dotenv.load_dotenv()


def main() -> None:
    _init_logging()

    controller: mongodb_controller.MongoDBController = mongodb_controller.MongoDBController()
    controller.connect()
    controller.set_current_database(os.getenv('DATABASE_NAME'))
    controller.set_fields()

    with open('data/schema.json', 'w+') as file:
        print(controller.fields)
        file.write(json.dumps(controller.fields))

    print(controller.fields)


def _init_logging() -> None:
    """Initializes logging for every module used by this module."""
    logging.basicConfig(filename='document_store.log',
                        filemode='a+',
                        level='DEBUG',
                        datefmt='%d-%b-%y %H:%M:%S',
                        format='%(asctime)s - %(levelname)s - %(message)s')


if __name__ == '__main__':
    main()
