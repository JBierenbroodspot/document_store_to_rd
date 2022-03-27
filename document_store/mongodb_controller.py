# ----------------------------------------------------------------------------------------------------------------------
#  SPDX-License-Identifier: BSD 3-Clause                                                                               -
#  Copyright (c) 2022 Jimmy Bierenbroodspot.                                                                           -
# ----------------------------------------------------------------------------------------------------------------------
"""Connects to a mongodb database. This module can be used without dotenv, but it is recommended as it automates
things"""
from __future__ import annotations

import datetime
import os
import logging
import typing
import collections
from collections import abc
import abc

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
        """Scans every collection and finds out the following things: What fields every document has, the whether it is
        a nested datatype and the datatypes of every field recursively.

        :param sample_size: The amount of documents you want to scan for each collection.
        :return: None.
        """

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


class FieldBaseClass:
    """An abstract base class created to save a little bit of space by allowing to omit some docstrings."""
    __metaclass__ = abc.ABCMeta
    children: typing.Union[typing.Dict[str], typing.List, typing.Type]
    # Hashmap :)))
    # Defining every type here is arguably not the most solid way to do this, but it greatly improves performance by not
    # having to iterate over anything rather the value can be retrieved with a simple single expression.
    type_map: typing.Dict[typing.Type, str, typing.Type] = {
        dict: "object",
        collections.abc.Iterable: "list",
        list: "list",
        tuple: "list",
        set: "list",
        str: "single_type",
        int: "single_type",
        datetime.datetime: "single_type",
        type(None): "single_type",
        bool: "single_type",
        float: "single_type",
        bson.objectid.ObjectId: "single_type",
    }

    @abc.abstractmethod
    def update(self, __value: definitions.T) -> None:
        pass

    def as_json(self) -> typing.Dict[str]:
        """Converts object into a json-serializable object.

        :return: A json serializable dictionary.
        """
        pass


class DocumentObject(FieldBaseClass):
    """A type that contains a dictionary-like structure containing datatypes or more objects."""
    children: typing.Dict

    def __init__(self, document: typing.Dict) -> None:
        """Initializes children parameter with values from a dictionary.

        :param document: A dictionary.
        """
        key: str
        value: typing.Union[typing.Any, typing.Dict, typing.Iterable]

        self.children = {}
        self.update(document)

    def update(self, document: typing.Dict) -> None:
        """Scans a document and either appends a non-existing key to its children or updates an existing key with a new
        type.

        :param document:
        :return:
        """
        key: str
        value: typing.Union[typing.Any, typing.Dict, typing.Iterable]
        value_type: str
        child_value: FieldBaseClass

        for key, value in document.items():
            value_type = self.type_map[type(value)]
            child_value = self.children.get(key, {}).get(value_type)

            if child_value:
                child_value.update(value)
                continue

            self.children[key] = {}
            self.children[key][value_type] = bind_to_object(value_type)(value)

    def as_json(self) -> typing.Dict:
        out: typing.Dict = {}

        for key, value in self.children.items():
            for key_2, value_2 in value.items():
                out[key] = {}
                out[key][key_2] = value_2.as_json()

        return out


class TypeObject(FieldBaseClass):
    """And object used for storing a types that are not iterables. If types are stored in this object it suggests
    that a field containing this object is a single type as opposed to an iterable. It can either store a single type
    or a list of types."""
    children: typing.Union[typing.Type, typing.List[typing.Type]]

    def __init__(self, untyped: typing.Any) -> None:
        """Initializes with a single type.

        :param untyped: Any value that is not an instance of type.
        """
        self.children = type(untyped)

    def update(self, untyped: typing.Any) -> None:
        """Adds a type to itself if it does not exist already, otherwise it is ignored.

        :param untyped: Any value that is not an instance of type.
        :return: None.
        """
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


class IterableObject(FieldBaseClass):
    """Represents a field that is an array of other values."""
    children: typing.List

    def __init__(self, __iterable: typing.Iterable) -> None:
        """Initializes with a single iterable.

        :param __iterable: Any iterable.
        """
        self.children = []
        self.update(__iterable)

    def update(self, __iterable: typing.Iterable) -> None:
        """Updates the list with new types by updating the objects within the children list.

        :param __iterable: An iterable.
        :return:
        """
        value_type: typing.Type[IterableObject, TypeObject, DocumentObject]
        existing_item: typing.List[typing.Type[IterableObject, TypeObject, DocumentObject]]

        for value in __iterable:
            value_type = bind_to_object(self.type_map[type(value)])
            existing_item = [child for child in self.children if type(child) is value_type]

            if existing_item:
                existing_item[0].update(value)
                break

            self.children.append(value_type(value))

    def as_json(self) -> typing.List:
        return [item.as_json() for item in self.children]


def bind_to_object(object_string: str) -> typing.Type[typing.Union[TypeObject, IterableObject, DocumentObject]]:
    if object_string == "object":
        return DocumentObject
    elif object_string == "list":
        return IterableObject
    else:
        return TypeObject
