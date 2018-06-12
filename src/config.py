#!/usr/bin/env python3
import requests, mimetypes, json, os, sys, logging, datetime, traceback, time, hashlib
import yaml

class Config(object):
    def __init__(self, path=None):
        if path is None:
            raise Exception('Config file is not specified.')
        self.__path = path
        with open(path, 'r') as stream:
            self.__content = yaml.load(stream)
    
    def save(self, path=None):
        if path is None:
            path = self.__path
        with open(path, 'w') as stream:
            stream.write(yaml.dump(self.__content))
        self.__path = path
    
    def __getitem__(self, key):
        if key not in self.__content:
            return None
        return self.__content[key]