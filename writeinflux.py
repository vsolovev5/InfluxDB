# -*- coding: utf-8 -*-
import json
import os
import datetime

from influxdb import InfluxDBClient


class WriteInflux:

    def __init__(self, data):
        """Constructor"""
        self.data = data
        ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

        with open(ROOT_DIR+'/config.json') as f:
            data_config = json.load(f)

        config_influx = data_config.get('influx')
        self.config_influx = config_influx
        self.database = config_influx['database']
        if self.connect_influx():
            client = self.connect_influx()
            self.check_db(client)
            self.export_to_influxdb(client)

    def connect_influx(self):
        host = self.config_influx['host']
        port = self.config_influx['port']
        username = self.config_influx['username']
        password = self.config_influx['password']

        client = InfluxDBClient(host=host, port=port, username=username, password=password)
        try:
            client.ping()
        except Exception as e:
            print(e)
            return False
        return client

    def check_db(self, client):
        database_name = self.database
        if not ({'name': database_name} in client.get_list_database()):
            client.create_database(database_name)
        client.switch_database(database_name)

    def export_to_influxdb(self, client):
        """
        Запись в базу данных временных рядов
        :param polling_time_value:
        :return:
        """
        json_body = self.data
        print(json_body)
        client.write_points(json_body)

today = datetime.datetime.today()
time=int(today.strftime("%Y-%m-%d-%H.%M.%S")*1000000000)
json_text=[
    {
    "measurement":'Test',
    "time": time,
    "fields":{
        "field_name":"test"
    }
    }
]
json_body=json.load(json_text)
insert=WriteInflux(json_text)