# ISSUE: Assumes all database insert errors are caused by a duplicate primary key error.

import json
from datetime import datetime, timezone
import dataset
import sqlite3 # for error handling
import os
import data_manip

def main():
    # extract parts of data
    full_data = json.load(open("./takeout-20231116T175203Z-001/Takeout/Home App/HomeApp.json"))
    thermostat_data = full_data['Home App Data'][0]['full_structures'][0]['rooms_and_devices'][1]['devices']


    full_hist_data = json.load(open("./takeout-20231116T175203Z-001/Takeout/Home App/HomeHistory.json"))
    thermostat_hist_events = full_hist_data['structure_history'][0]['events']
    thermostat_hist_device_hist = full_hist_data['structure_history'][0]['device_history']

    # set up db
    db = dataset.connect('sqlite:///nest_data.db')
    tbl = db['thermostat_events']

    db.query("DROP TABLE IF EXISTS thermostat_events")

    for i, record in enumerate(thermostat_hist_events):
        
        # print(i,":  ",record['timestamp'])
        # print(record['timestamp'])

        # print(event)
        # breakpoint()

        res = {}

        res['id'] = i
        res['timestamp'] = record['timestamp']
        # res['observed_timestamp'] = record['event']['event_header']['observed_timestamp']
        # res['importance'] = record['event']['event_header']['event_importance']

        # res_subject = data_manip.flatten_json(record['event']['event_header']['subject'])
        # res_prod_agent = data_manip.flatten_json(record['event']['event_header']['producer_agent'])
        try:
            res_event_data = data_manip.flatten_json(record['event']['event_data'])
            res_event_data['type'] = res_event_data['@type']
            res_event_data.pop('@type')
        except KeyError:
            pass


        res.update(res_event_data)

        tbl.insert(res)



if __name__ == '__main__':
    main()

