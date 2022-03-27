# ----------------------------------------------------------------------------------------------------------------------
#  SPDX-License-Identifier: BSD 3-Clause                                                                               -
#  Copyright (c) 2022 Jimmy Bierenbroodspot.                                                                           -
# ----------------------------------------------------------------------------------------------------------------------
"""Connects to a mongodb database. This module can be used without dotenv, but it is recommended as it automates
things"""
from __future__ import annotations
import os
import logging
import typing
import json
import collections
from collections import abc

import pymongo
from pymongo import database
from pymongo import errors

import document_store.definitions as definitions


class MongoDBController:
    """Controller for connecting and read operations for a MongoDB database."""
    __metaclass__ = definitions.Singleton

    database: pymongo.database.Database
    client: pymongo.MongoClient
    connection_string: str
    fields: typing.Dict

    def __init__(self, hostname: typing.Union[str, None] = None, port: typing.Union[str, None] = None) -> None:
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
        """Sets currently selected database.

        :param database_name: Name of database to select.
        :return: None.
        """
        self.fields = {}  # Reset field names.

        try:
            self.database = self.client.get_database(database_name)
        except pymongo.errors.PyMongoError as err:
            logging.error(f'invalid database name: {database_name}.')
            raise pymongo.errors.PyMongoError(err)

        logging.info(f'selected {database_name}.')

    def set_fields(self, sample_size: int = 0) -> None:

        for collection in self.database.list_collection_names():
            documents: pymongo.collection.Cursor
            document: typing.Dict

            documents = self.database.get_collection(collection).find()

            if sample_size > 0:
                documents = documents.limit(sample_size)

            self.fields[collection] = DocumentObject(documents.next())  # Set first value so it can be updated.

            for count, document in enumerate(documents):
                print("New document")
                self.fields[collection].update(document)
                print("Processed", count, "items!")

            self.fields[collection] = self.fields[collection].as_json()


class DocumentObject:
    children: typing.Dict

    def __init__(self, document: typing.Dict) -> None:
        key: str
        value: typing.Union[typing.Any, typing.Dict, typing.Iterable]
        self.children = {}
        self.update(document)

    def update(self, document: typing.Dict) -> None:
        key: str
        value: typing.Union[typing.Any, typing.Dict, typing.Iterable]

        for key, value in document.items():
            if isinstance(value, dict):
                child_value: DocumentObject = self.children.get(key, {}).get("object")

                if child_value:
                    child_value.update(value)

                else:
                    self.children[key] = {}
                    self.children[key]["object"] = DocumentObject(value)

            elif isinstance(value, collections.abc.Iterable) and not isinstance(value, str):
                child_value: IterableObject = self.children.get(key, {}).get("list")

                if child_value:
                    child_value.update(value)

                else:
                    self.children[key] = {}
                    self.children[key]["list"] = IterableObject(value)

            else:
                child_value: TypeObject = self.children.get(key, {}).get("single_type")

                if child_value:
                    child_value.update(value)

                else:
                    self.children[key] = {}
                    self.children[key]["single_type"] = TypeObject(value)

    def as_json(self) -> typing.Dict:
        out: typing.Dict = {}

        for key, value in self.children.items():
            for key_2, value_2 in value.items():
                out[key] = {}
                out[key][key_2] = value_2.as_json()

        return out


class TypeObject:
    children: typing.Union[typing.Type, typing.List[typing.Type]]

    def __init__(self, untyped: typing.Any) -> None:
        self.children = type(untyped)

    def update(self, untyped: typing.Any) -> None:
        typed: typing.Type = type(untyped)

        if not isinstance(self.children, list):
            if self.children != typed:
                self.children = [self.children, typed]

        else:
            if typed not in self.children:
                self.children.append(typed)

    def as_json(self) -> typing.Union[str, typing.List[str]]:
        if not isinstance(self.children, list):
            return str(self.children)

        return [str(child) for child in self.children]


class IterableObject:
    children: typing.List

    def __init__(self, __iterable: typing.Iterable) -> None:
        self.children = []
        self.update(__iterable)

    def update(self, __iterable: typing.Iterable) -> None:
        for value in __iterable:
            if isinstance(value, dict):
                item: DocumentObject

                for item in self.children:
                    if isinstance(item, DocumentObject):
                        item.update(value)
                        break

                else:
                    self.children.append(DocumentObject(value))

            elif isinstance(value, collections.abc.Iterable) and not isinstance(value, str):
                item: IterableObject

                for item in self.children:
                    if isinstance(item, IterableObject):
                        item.update(value)
                        break

                else:
                    self.children.append(IterableObject(value))

            else:
                item: TypeObject

                for item in self.children:
                    if isinstance(item, TypeObject):
                        item.update(value)
                        break

                else:
                    self.children.append(TypeObject(value))

    def as_json(self) -> typing.List:
        return [item.as_json() for item in self.children]
