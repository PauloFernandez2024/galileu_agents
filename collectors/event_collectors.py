# -*- coding: utf-8 -*-

import sys, os, getopt, time
from   urllib.parse import urlencode
from   datetime import datetime, timedelta, timezone
from   dateutil import parser
import requests
from   requests import Session, utils
from   confluent_kafka import Producer
from   confluent_kafka.admin import AdminClient, NewTopic
from   pathlib import Path
from   collections import Counter
import threading
from   concurrent.futures import ThreadPoolExecutor
import json
import time

PAGE_SIZE = 1000

class NoRebuildAuthSession(Session):
    def rebuild_auth(self, prepared_request, response):
        """
        This method is intentionally empty. Needed to prevent auth header stripping on redirect. More info:
        https://stackoverflow.com/questions/60358216/python-requests-post-request-dropping-authorization-header
        """

API_MAX_RETRIES         = 3
API_CONNECT_TIMEOUT     = 60
API_TRANSMIT_TIMEOUT    = 60
API_STATUS_RATE_LIMIT   = 429

#Set to True or False to enable/disable console logging of sent API requests

#Modify to customise what to print in tables when a field is None/empty
BLANK_FIELD             = ""

API_BASE_URL            = "https://api.meraki.com/api/v1"

str_event_json = ""
new_str_event_json = ""

bootstrap_servers = 'localhost:9092'
client_id = 'galileu-events-collector'

admin_conf = {'bootstrap.servers': bootstrap_servers}
producer_conf = {'bootstrap.servers': bootstrap_servers, 'client.id': client_id}

admin_client = AdminClient(admin_conf)
producer = Producer(producer_conf)


BASE_DIR = Path("/usr/local/WOC")
sys.path.append(str(Path(BASE_DIR)))
from configuration import GetConfiguration
config = GetConfiguration()

# logging
from logger import setup_logger
log_path = config.config_data['logging_event_collectors']['file']
log_level = config.config_data['logging_event_collectors']['level']
logger = setup_logger(log_path=log_path, log_level=log_level)


#######################################
#        Meraki Request Core          #
#######################################
def merakiRequest(p_apiKey, p_httpVerb, p_endpoint, p_additionalHeaders=None, p_queryItems=None, p_requestBody=None, p_retry=0):
    if p_retry > API_MAX_RETRIES:
        logger.error("Error: Reached max retries")
        return False, None, None, None

    bearerString = "Bearer " + p_apiKey
    headers = {"Authorization": bearerString}
    if not p_additionalHeaders is None:
        headers.update(p_additionalHeaders)

    query = ""
    if not p_queryItems is None:
        query = "?" + urlencode(p_queryItems)
    url = API_BASE_URL + p_endpoint + query

    verb = p_httpVerb.upper()

    session = NoRebuildAuthSession()

    try:
        if verb == "GET":
            r = session.get(
                url,
                headers =   headers,
                timeout =   (API_CONNECT_TIMEOUT, API_TRANSMIT_TIMEOUT)
            )
        elif verb == "PUT":
            if not p_requestBody is None:
                r = session.put(
                    url,
                    headers =   headers,
                    json    =   p_requestBody,
                    timeout =   (API_CONNECT_TIMEOUT, API_TRANSMIT_TIMEOUT)
                )
        elif verb == "POST":
            if not p_requestBody is None:
                r = session.post(
                    url,
                    headers =   headers,
                    json    =   p_requestBody,
                    timeout =   (API_CONNECT_TIMEOUT, API_TRANSMIT_TIMEOUT)
                )
        elif verb == "DELETE":
            r = session.delete(
                url,
                headers =   headers,
                timeout =   (API_CONNECT_TIMEOUT, API_TRANSMIT_TIMEOUT)
            )
        else:
            return False, None, None, None
    except Exception as e:
        logger.critical("event_collector: " +  str(e))
        return False, None, None, None

    success         = r.status_code in range (200, 299)
    errors          = None
    responseHeaders = None
    responseBody    = None

    if r.status_code == API_STATUS_RATE_LIMIT:
        logger.warning("Info: Hit max request rate. Retrying %s after %s seconds" % (p_retry+1, r.headers["Retry-After"]))
        time.sleep(int(r.headers["Retry-After"]))
        success, errors, responseHeaders, responseBody = merakiRequest(p_apiKey, p_httpVerb, p_endpoint, p_additionalHeaders,
            p_queryItems, p_requestBody, p_retry+1)
        return success, errors, responseHeaders, responseBody

    try:
        rjson = r.json()
    except:
        rjson = None

    if not rjson is None:
        if "errors" in rjson:
            errors = rjson["errors"]
            logger.warning("Error: " + url + " - " + str(errors))
        else:
            responseBody = rjson


    if "Link" in r.headers:
        parsedLinks = utils.parse_header_links(r.headers["Link"])
        for link in parsedLinks:
            if link["rel"] == "next":
                splitLink = link["url"].split("/api/v1")
                time.sleep(1)  # para evitar rate limiting
                success, errors, responseHeaders, nextBody = merakiRequest(p_apiKey, p_httpVerb, splitLink[1],
                    p_additionalHeaders=p_additionalHeaders, p_requestBody=p_requestBody)
                if success:
                    if not responseBody is None:
                        if 'items' in responseBody:
                            responseBody = { 'items': responseBody['items'] + nextBody['items'] }
                        else:
                            if 'message' in nextBody and 'No  matching events found' not in nextBody['message']:
                                responseBody = responseBody + nextBody
                else:
                    responseBody = None

    return success, errors, responseHeaders, responseBody


#######################################
#          getOrganizations           #
#######################################
def getOrganizations(p_apiKey):
    endpoint = "/organizations"
    success, errors, headers, response = merakiRequest(p_apiKey, "GET", endpoint)
    return success, errors, headers, response


#######################################
#            getNetworks              #
#######################################
def getNetworks(p_apiKey, p_orgId):
    endpoint = "/organizations/%s/networks" % p_orgId
    query = {"perPage": 1000}
    success, errors, headers, response = merakiRequest(p_apiKey, "GET", endpoint, p_queryItems=query)
    return success, errors, headers, response


#######################################
#            fetchAllEvents           #
#######################################
def fetchAllEvents(p_apiKey, net_id):
    endpoint = "/networks/%s/events" % net_id
    p_perpage = PAGE_SIZE
    query = {"includedEventTypes[]": "auto_rf_channel_change", "productType": "wireless", "perPage": p_perpage}
    success, errors, headers, response = merakiRequest(p_apiKey, "GET", endpoint, p_queryItems=query)
    if not success:
        return None
    return response


#######################################
#          delivery_report            #
#######################################
def delivery_report(err, msg):
    if err is not None:
        logger.warning(f'Delivery failed: {err}')
    else:
        logger.warning(f'Message delivered to {msg.topic()} [{msg.partition()}]')



#######################################
#           process_network           #
#######################################
def process_network(org, arg_apiKey):
    global new_str_event_json
    success, errors, headers, nets = getNetworks(arg_apiKey, org["id"])
    if not success:
        logger.error("event_collector: return error from getNetworks")
        return

    try:
        event_json = json.loads(str_event_json)
    except Exception as e:
        logger.error("Error while processing event history: " + str(e))
        return

    try:
        if nets:
            for net in nets:
               response = fetchAllEvents(arg_apiKey, net["id"])
               if response is not None:
                   if 'message' in response and response['message'] is not None:
                       if 'No  matching events found' in response['message']:
                           continue
                   topic = f"galileu.events.{net['id']}"
                   new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
                   fs = admin_client.create_topics([new_topic])
                   for topic, f in fs.items():
                       try:
                           f.result()
                       except Exception as e:
                           logger.critical(f"Topic {topic} already exists or any other error: {e}")

                   previous_events = event_json["previous_events"]
                   found = False
                   if previous_events:
                       for elem in previous_events:
                           if elem["net_id"] == net["id"]:
                               t0 = datetime.fromisoformat(elem["occurredAt"].replace("Z", "+00:00"))
                               found = True
                               break
                   if found == False:
                       t0 = datetime.now(timezone.utc) - timedelta(days=1)

                   events = response.get("events", [])
                   initial = True
                   for e in events:
                       occurred_at = datetime.fromisoformat(e["occurredAt"].replace("Z", "+00:00"))
                       if occurred_at > t0:
                           if initial:
                               if found:
                                   for elem in previous_events:
                                       if elem["net_id"] == net["id"]:
                                           elem["occurredAt"] = e["occurredAt"]
                                           break
                               else:
                                   previous_events.append({"net_id": net["id"], "occurredAt": e["occurredAt"]})
                               initial = False

                           band = e['eventData']['band']
                           if band == "2":
                               band = "2.4"
                           evt = { "occurredAt": e['occurredAt'], "apName": e['deviceName'], "apSerial": e['deviceSerial'],
                                   "oldChannel": e['eventData']['oldChan'],"newChannel": e['eventData']['newChan'],
                                   "band": band }
                           message = {
                                   "network_id": net["id"],
                                   "org_name": org["name"],
                                   "events": evt
                           }
                           try:
                               producer.produce(
                                   topic=topic,
                                   key=evt["apName"].encode('utf-8'),
                                   value=json.dumps(message).encode('utf-8'),
                                   callback=delivery_report
                               )
                           except BufferError:
                               producer.poll(1)
               else:
                   logger.error("No data received from fetchAllEvents")

        new_str_event_json = json.dumps(event_json)
    except Exception as e:
        logger.error("Error in process_network: " + str(e))
        return


def get_meraki_token(partner_name):
    try:
        payload = { 'partner_name': (None, partner_name) }
        url = "http://localhost/api/galileu/v1.0/woc/partner/get"
        response = requests.post(url, files=payload)
        if response.ok:
            resp = response.json()
            token = resp["meraki_token"]
            return token
        else:
            logger.critical("Error while getting meraki token")
            return None
    except Exception as err:
        logger.critical("Error while getting meraki token: " + str(err))
        return None


def main(partner_name):
    fail_op = False
    arg_apiKey = get_meraki_token(partner_name)
    if arg_apiKey is None:
        logger.critical("get_meraki_token returning None")
        fail_op = True
    if not fail_op:
        success, errors, headers, orgs = getOrganizations(arg_apiKey)
        if not success or not orgs:
            logger.error("event_collectors: Could not fetch organizations")
            fail_op = True
        else:
            for org in orgs:
                process_network(org, arg_apiKey)
            producer.flush()
    if new_str_event_json != "":
        print(new_str_event_json)
    else:
        print(str_event_json)
    exit(0)


if __name__ == '__main__':
    partner_name  = sys.argv[1]
    #arg_apiKey = "629ac0b03cbcbc7d7a2916ff7b2e2f298214744d"
    str_event_json = input()
    main(partner_name)
