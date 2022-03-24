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

    def set_fields(self, skip: int = 1, sample_size: int = 0) -> None:

        for collection in self.database.list_collection_names():
            cursor = self.database.get_collection(collection).find()
            if sample_size > 0:
                cursor = cursor.limit(sample_size)
            self.fields[collection] = FieldNode(cursor, skip=skip).as_dict()


class FieldNode:
    """Stores fieldnames and datatypes of a MongoDB document store."""

    def __init__(self, cursor_object: typing.Optional[pymongo.collection.Cursor] = None,
                 dict_object: typing.Optional[typing.Dict] = None,
                 skip: int = 1) -> None:
        count: int
        cursor_item: typing.Dict[str, typing.Any]
        _dict: typing.Dict = collections.defaultdict(set)

        if not dict_object:
            print("Scanning...")
            for count, cursor_item in enumerate(cursor_object):
                if count % skip == 0:
                    self.scan_dict(cursor_item)
                    print('Processed', count, 'items!')
        else:
            self.scan_dict(dict_object)

    def scan_dict(self, cursor_item: typing.Dict[str, typing.Any]) -> None:
        """Scans a dictionary and adds an attribute for every unknown attribute. If a property exists the type of the
        value will be added to that attribute.

        :param cursor_item: A dictionary.
        :return: None.
        """
        field_name: str
        value: typing.Any
        item: typing.Any

        for field_name, value in cursor_item.items():
            if not hasattr(self, field_name):
                self._set_field(field_name, value)
            else:
                self._add_type(field_name, value)

    def as_dict(self) -> typing.Dict:
        _dict: typing.Dict = {}
        key: str
        value: typing.Union[typing.Any, FieldNode]

        for key, value in self.__dict__.items():
            _list = []

            for item in value:
                if type(item) == FieldNode:
                    _list.append(item.as_dict())
                elif type(item) == list:
                    _list.append([x.as_dict() if type(x) == FieldNode else x for x in item])
                else:
                    _list = item

            _dict[key] = _list

        return _dict

    def _set_field(self, _field_name: str, _value: typing.Any) -> None:
        value_type: typing.Type = type(_value)

        if value_type == dict:
            setattr(self, _field_name, [FieldNode(dict_object=_value)])
        elif value_type == list:
            setattr(self, _field_name, [list({str(type(item)) for item in _value})])
        else:
            setattr(self, _field_name, [str(value_type)])

    def _add_type(self, _field_name: str, _value: typing.Any) -> None:
        field_node: FieldNode
        field_list: typing.List
        values: typing.Union[FieldNode, typing.List]
        value_type: typing.Type = type(_value)

        if value_type == dict:
            self._add_dict_attribute(_field_name, _value)
        elif value_type == list:
            self._add_list_attribute(_field_name, _value)
        else:
            field_list = list(set(list(getattr(self, _field_name)) + [str(value_type)]))
            setattr(self, _field_name, field_list)

    def _add_dict_attribute(self, _attr_name: str, _value: typing.Dict) -> None:
        old_values: typing.List = getattr(self, _attr_name)

        for old_value in old_values:
            if type(old_value) == FieldNode:
                old_value._set_field(_attr_name, _value)
                break
        else:
            old_values.append(FieldNode(dict_object=_value))

        setattr(self, _attr_name, old_values)

    def _add_list_attribute(self, _attr_name: str, _value: typing.List) -> None:
        old_values: typing.List = getattr(self, _attr_name)
        typed_values: typing.List[str] = list({str(type(value)) for value in _value})

        for old_value in old_values:
            if type(old_value) == list:
                old_value.extend(typed_value for typed_value in typed_values if typed_value not in old_value)
                break
        else:
            old_values.append(typed_values)

        setattr(self, _attr_name, old_values)
