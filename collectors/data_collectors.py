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
from   concurrent.futures import ThreadPoolExecutor, as_completed
from   functools import partial
from   itertools import product
import json

PAGE_SIZE = 1000
COLLECTION_INTERVAL = 300
PREVIOUS_MINUTES = 8

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


bootstrap_servers = 'localhost:9092'
client_id = 'galileu-data-collector'

admin_conf = {'bootstrap.servers': bootstrap_servers}
producer_conf = {'bootstrap.servers': bootstrap_servers, 'client.id': client_id}

admin_client = AdminClient(admin_conf)
producer = Producer(producer_conf)


BASE_DIR = Path("/usr/local/WOC")
sys.path.append(str(Path(BASE_DIR)))
from configuration import GetConfiguration
config = GetConfiguration()

from logger import setup_logger
log_path = config.config_data['logging_data_collectors']['file']
log_level = config.config_data['logging_data_collectors']['level']
logger = setup_logger(log_path=log_path, log_level=log_level)


#######################################
#        Meraki Request Core          #
#######################################
def merakiRequest(p_apiKey, p_httpVerb, p_endpoint, p_additionalHeaders=None, p_queryItems=None, p_requestBody=None, p_retry=0):
    if p_retry > API_MAX_RETRIES:
        logger.critical("Error: Reached max retries")
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
    except:
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
                success, errors, responseHeaders, nextBody = merakiRequest(p_apiKey, p_httpVerb, splitLink[1],
                    p_additionalHeaders=p_additionalHeaders, p_requestBody=p_requestBody)
                if success:
                    if not responseBody is None:
                        if 'items' in responseBody:
                            responseBody = { 'items': responseBody['items'] + nextBody['items'] }
                        else:
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
#          getAPsFromNetwork          #
#######################################
def getAPsFromNetwork(p_apiKey, p_netId):
    endpoint = "/networks/%s/devices" % p_netId
    p_perpage = 1000
    query = {"perPage": p_perpage}
    success, errors, headers, response = merakiRequest(p_apiKey, "GET", endpoint, p_queryItems=query)
    return success, errors, headers, response


#######################################
#      getNumberOfClientsPerAP        #
#######################################
def getNumberOfClientsPerAP(api_key, serial, t0):
    endpoint = f"/devices/{serial}/clients"
    query = {"t0": t0, "perPage": PAGE_SIZE}
    success, errors, headers, response = merakiRequest(api_key, "GET", endpoint, p_queryItems=query)
    return len(response)



#######################################
#            getPacketLoss            #
#######################################
def getPacketLoss(p_apiKey, org_id, net_id, serial, band, t0, t1):
    p_perpage = PAGE_SIZE
    interval = COLLECTION_INTERVAL
    endpoint = "/organizations/%s/wireless/devices/packetLoss/byDevice" % org_id
    query = {"networkIds[]": net_id, "serials[]": serial, "bands[]": band, "t0": t0, "t1": t1, "resolution": interval, "perPage": p_perpage}
    success, errors, headers, response = merakiRequest(p_apiKey, "GET", endpoint, p_queryItems=query)
    return response


#######################################
#        getWifiUtilization        #
#######################################
def getWifiUtilization(p_apiKey, org_id, net_id, serials, t0, t1):
    p_perpage = PAGE_SIZE
    interval = COLLECTION_INTERVAL
    endpoint = "/organizations/%s/wireless/devices/channelUtilization/byDevice" % org_id
    query = [
        ("networkIds[]", net_id),
        *[( "serials[]", s) for s in serials],
        ("t0", t0),
        ("t1", t1),
        ("interval", interval),
        ("perPage", p_perpage)
    ]
    success, errors, headers, response = merakiRequest(p_apiKey, "GET", endpoint, p_queryItems=query)
    send = False
    if response:
        for resp in response:
            if len(resp["byBand"]):
                send = True
                serial = resp["serial"]
                for byband in resp["byBand"]:
                   band = byband["band"]
                   packetloss =  getPacketLoss(p_apiKey, org_id, net_id, serial, band, t0, t1)
                   if packetloss:
                       resp["network"]["name"] = packetloss[0]["network"]["name"]
                       resp["name"] = packetloss[0]["device"]["name"]
                       total = packetloss[0]["upstream"]["total"]
                       lost = packetloss[0]["upstream"]["lost"]
                       total += packetloss[0]["downstream"]["total"]
                       lost += packetloss[0]["downstream"]["lost"]
                       if total:
                         lost = round((lost * 100 / total), 2)
                       byband["total"] = total
                       byband["lost"] = lost
    if send:
        return response
    else:
        return None



def delivery_report(err, msg):
    if err is not None:
        logger.warning(f'Delivery failed: {err}')
    else:
        logger.warning(f'Message delivered to {msg.topic()} [{msg.partition()}]')



#######################################
#        get_channel_utilization      #
#######################################
def get_channel_utilization(org, arg_apiKey, last_date, interval):
    topic = f"galileu.data.{org['id']}"
    new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
    fs = admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()
        except Exception as e:
            logger.critical(f"Topic {topic} already exists or any other error: {e}")

    success, errors, headers, nets = getNetworks(arg_apiKey, org["id"])
    if not success:
        return
    if nets:
        for net in nets:
            success, errors, headers, aps = getAPsFromNetwork(arg_apiKey, net["id"])
            if aps:
                now = datetime.now(timezone.utc).replace(microsecond=0)
                t1 = now.isoformat().replace("+00:00", "Z")
                #t0 = (now - timedelta(minutes=PREVIOUS_MINUTES)).isoformat().replace("+00:00", "Z")
                dt = datetime.fromtimestamp(last_date)
                t0 = dt.isoformat() + "Z"
                serials = [ap["serial"] for ap in aps]
                chanelutilization = getWifiUtilization(arg_apiKey, org["id"], net["id"], serials, t0, t1)
                if chanelutilization:
                    for ch in chanelutilization:
                        if ch["byBand"]:
                            ch["name"] = ch.get("name") or ch["serial"]
                            ch["nclients"] = getNumberOfClientsPerAP(arg_apiKey, ch["serial"], t0)
                            message = {
                                "org_id": org["id"],
                                "org_name": org["name"],
                                "chanelutilization": ch
                            }
                            try:
                                producer.produce(
                                    topic=topic,
                                    key=ch["name"].encode('utf-8'),
                                    value=json.dumps(message).encode('utf-8'),
                                    callback=delivery_report
                                )
                            except BufferError:
                                producer.poll(1)

#######################################
#          get_meraki_token           #
#######################################
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



def main(partner_name, last_date, interval):
    arg_apiKey = get_meraki_token(partner_name)
    if arg_apiKey is None:
        return
    success, errors, headers, orgs = getOrganizations(arg_apiKey)
    if not success or not orgs:
        logger.error("Could not fetch organizations")
        return

    funcs = [ get_channel_utilization ]
    common_kwargs = dict(arg_apiKey=arg_apiKey, last_date=last_date, interval=interval)

    futures = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        for func, org in product(funcs, orgs):
            job = partial(func, org, **common_kwargs)   
            fut = executor.submit(job)
            futures[fut] = (func.__name__, org)

        for fut in as_completed(futures):
            func_name, org = futures[fut]
            try:
                result = fut.result()
            except Exception as e:
                logger.critical("Error %s(%s): %s", func_name, org, e)

    producer.flush()


if __name__ == '__main__':
    partner_name  = sys.argv[1]
    last_date = int(sys.argv[2])
    interval = int(sys.argv[3])
    main(partner_name, last_date, interval)
