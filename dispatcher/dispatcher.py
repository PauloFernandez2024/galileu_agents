import sys
import asyncio
import json
import time
import requests
from   pathlib import Path
from   datetime import datetime
import subprocess
import threading

BASE_DIR = Path("/usr/local/WOC")
sys.path.append(str(Path(BASE_DIR)))
from configuration import GetConfiguration
config = GetConfiguration()

from logger import setup_logger
log_path = config.config_data['logging_dispatcher']['file']
log_level = config.config_data['logging_dispatcher']['level']
logger = setup_logger(log_path=log_path, log_level=log_level)

WAIT_TIME = 300

status_event_executing = "OFF"
status_data_executing = "OFF"
status_assessment_executing = "ON"

last_data_collector_interval= 0
last_assessment_collector_interval = 0

event_file = str(Path("/usr/local/WOC") / "collectors" / "event_offsets.json")

def run_event_script(partner_name):
    global status_event_executing
    global str_event_json

    status_event_executing = "ON"
    try:
        with open(event_file, "r", encoding="utf-8") as f:
            str_event_json = f.read()
        cmd = [
            "python3",
            str(Path("/usr/local/WOC") / "collectors" / "event_collectors.py"),
            partner_name
        ]
        process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = process.communicate(input=str_event_json)
        if process.returncode == 0:
            str_event_json = stdout
            with open(event_file, "w", encoding="utf-8") as f:
                f.write(str_event_json)
    except Exception as err:
        logger.critical("Error run_event_script: " + str(err))
    status_event_executing = "OFF"


def run_data_script(partner_name, data_collector_interval):
    global status_data_executing
    global last_data_collector_interval
    status_data_executing = "ON"
    process = subprocess.Popen(["python3", str(Path("/usr/local/WOC") / "collectors" / "data_collectors.py"), partner_name,                                                                              str(last_data_collector_interval), str(data_collector_interval)])
    process.wait()
    last_data_collector_interval = int(time.time())
    status_data_executing = "OFF"


def run_assessment_script(partner_name, assessment_collector_interval):
    global status_assessment_executing
    global last_assessment_collector_interval
    status_assessment_executing = "ON"
    process = subprocess.Popen(["python3", str(Path("/usr/local/WOC") / "collectors" / "assessment_collectors.py"), partner_name,                                                                         str(last_assessment_collector_interval), str(assessment_collector_interval)])
    process.wait()
    last_assessment_collector_interval = int(time.time())
    status_assessment_executing = "OFF"


def check_tasks(partner_name, data_collector_interval, assessment_collector_interval):
    global last_data_collector_interval
    global last_assessment_collector_interval

    if status_event_executing == "OFF":
        thread = threading.Thread(target=run_event_script, args=(partner_name,))
        thread.start()

    if status_data_executing == "OFF":
        if last_data_collector_interval == 0:
            last_data_collector_interval = int(time.time()) - data_collector_interval
        last_data_collector_interval = last_data_collector_interval - 150
        thread = threading.Thread(target=run_data_script, args=(partner_name, data_collector_interval))
        thread.start()

    if status_assessment_executing == "OFF":
        if last_assessment_collector_interval == 0:
            last_assessment_collector_interval = int(time.time()) - assessment_collector_interval
        last_assessment_collector_interval -= 120
        thread = threading.Thread(target=run_assessment_script, args=(partner_name, assessment_collector_interval))
        thread.start()


if __name__ == "__main__":
    partner_name = config.config_data['customer'].get('partner_name')
    data_collector_interval = config.config_data['collectors_interval'].get('data_collector_interval')
    assessment_collector_interval = config.config_data['collectors_interval'].get('assessment_collector_interval')
    while True:
        now = int(time.time())
        check_tasks(partner_name, data_collector_interval, assessment_collector_interval)
        dif = int(time.time()) - now
        if dif < WAIT_TIME:
            time.sleep(WAIT_TIME - dif)
