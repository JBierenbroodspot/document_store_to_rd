# ----------------------------------------------------------------------------------------------------------------------
#  SPDX-License-Identifier: BSD 3-Clause                                                                               -
#  Copyright (c) 2022 Jimmy Bierenbroodspot.                                                                           -
# ----------------------------------------------------------------------------------------------------------------------
"""Connects to a mongodb database. This module can be used without dotenv, but it is recommended as it automates
things"""
import os
import logging
import typing

import pymongo
from pymongo import database
from pymongo import errors

import definitions


class MongoDBController:
    """Controller for connecting and read operations for a MongoDB database."""
    __metaclass__ = definitions.Singleton

    database: pymongo.database.Database
    client: pymongo.MongoClient
    connection_string: str

    def __init__(self, hostname: str | None = None, port: str | None = None) -> None:
        """Initializes connection string for MongoDB database connection."""
        mongo_port: str = port if port else os.getenv('MONGODB_PORT')
        mongo_host: str = hostname if hostname else os.getenv('MONGODB_HOSTNAME')

        self.connection_string: str = f'mongodb://{mongo_host}:{mongo_port}'

    def connect(self) -> pymongo.MongoClient:
        """Connects to a MongoDB client.

        :return: A MongoClient instance if connection was successful.
        """
        logging.info('Connecting to database...')

        try:
            self.client = pymongo.MongoClient(self.connection_string)
        except pymongo.errors.ConnectionFailure as err:
            logging.error('Connection failed', err)
            raise (pymongo.errors.ConnectionFailure(err))

        return self.client

    def set_current_database(self, database_name: str) -> None:
        """Sets currently selected database."""
        try:
            self.database = self.client.get_database(database_name)
        except pymongo.errors.PyMongoError as err:
            logging.error(f'invalid database name: {database_name}.')
            raise pymongo.errors.PyMongoError(err)

        logging.info(f'selected {database_name}.')

