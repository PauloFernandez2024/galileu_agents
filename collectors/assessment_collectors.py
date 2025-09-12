# -*- coding: utf-8 -*-

import sys, os, getopt, time, json
from   urllib.parse import urlencode
from   datetime import datetime
import requests
from   requests import Session, utils
from   confluent_kafka import Producer
from   confluent_kafka.admin import AdminClient, NewTopic
from   pathlib import Path
import json

BASE_DIR = Path("/usr/local/WOC")
sys.path.append(str(Path(BASE_DIR)))
from configuration import GetConfiguration
config = GetConfiguration()

from logger import setup_logger
log_path = config.config_data['logging_assessment_collectors']['file']
log_level = config.config_data['logging_assessment_collectors']['level']
logger = setup_logger(log_path=log_path, log_level=log_level)


THREE_MONTHS = 30 * 24 * 3600

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

# Set to True or False to enable/disable console logging of sent API requests

# Modify to customise what to print in tables when a field is None/empty
BLANK_FIELD             = ""

API_BASE_URL            = "https://api.meraki.com/api/v1"

bootstrap_servers = 'localhost:9092'
client_id = 'galileu-assessment-collector'

admin_conf = {'bootstrap.servers': bootstrap_servers}
producer_conf = {'bootstrap.servers': bootstrap_servers, 'client.id': client_id}

admin_client = AdminClient(admin_conf)
producer = Producer(producer_conf)


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
#             AP Info                 #
#######################################
def getOrganizations(p_apiKey):
    endpoint = "/organizations"
    success, errors, headers, response = merakiRequest(p_apiKey, "GET", endpoint)
    return success, errors, headers, response


def getNetworks(p_apiKey, p_orgId):
    endpoint = "/organizations/%s/networks" % p_orgId
    query = {"perPage": 1000}
    success, errors, headers, response = merakiRequest(p_apiKey, "GET", endpoint, p_queryItems=query)
    return success, errors, headers, response


def getAPsFromNetwork(p_apiKey, p_orgId, p_netId, p_serial):
    endpoint = "/organizations/%s/devices" % p_orgId
    p_perpage = 1000
    query = {"networkIds[]": str(p_netId), "serial": p_serial, "perPage": p_perpage}
    success, errors, headers, response = merakiRequest(p_apiKey, "GET", endpoint, p_queryItems=query)
    return success, errors, headers, response


def getSsids(p_apiKey, p_orgId):
    endpoint = "/organizations/%s/wireless/ssids/statuses/byDevice" % p_orgId
    query = {"perPage": 500}
    success, errors, headers, response = merakiRequest(p_apiKey, "GET", endpoint, p_queryItems=query)
    return success, errors, headers, response


#######################################
#           getInitialData            #
#######################################
def getInitialData(p_apiKey):
    organizations = []
    success, errors, headers, orgs = getOrganizations(p_apiKey)
    for org in orgs:
        details = org['management']['details']
        organization = { "id": org["id"], "name": org["name"], "customerNumber": details[0]['value'], "networks": [] }
        orgHeaderNotPrinted = True
        nets = []
        success, errors, headers, response = getSsids(p_apiKey, org["id"])
        if response:
            items = response['items']
            for ssid in items:
                index = 0
                found = False
                for i in range(len(nets)):
                    if ssid['network']['id'] == nets[i]["id"]:
                        found = True
                        index = i
                        break
                if found == False:
                    nets.append( {"id": ssid['network']['id'], "name": ssid['network']['name'], "aps": []} )
                    index = len(nets) - 1

                net = nets[index]
                success, errors, headers, taps = getAPsFromNetwork(p_apiKey, org["id"], ssid['network']['id'], ssid['serial'])
                if taps:
                    for ap in taps:
                        if 'name' in ap and ap["name"] is not None:
                            ap_name = ap["name"]
                        else:
                            ap_name = ap["serial"]
                        net["aps"].append({"name": ap_name, "lat": ap["lat"], "lng": ap["lng"], "serial": ap["serial"], "model": ap["model"],
                                           "mac": ap["mac"], "firmware": ap["firmware"], "bssids": [] })
                last_ap = len(net["aps"]) - 1
                aps =  net["aps"][last_ap]
                basicServiceSets = ssid['basicServiceSets']
                for basicService in basicServiceSets:
                    aps["bssids"].append( { "bssid": basicService['bssid'],
                                            "ssid": { "ssidName": basicService['ssid']['name'], "ssidNumber": basicService['ssid']['number'],
                                                      "ssidEnabled": basicService['ssid']['enabled'], "ssidAdvertised": basicService['ssid']['advertised'] },
                                            "radio": { "radioBroadcasting": basicService['radio']['isBroadcasting'], "radioBand": basicService['radio']['band'],
                                                       "radioChannel": basicService['radio']['channel'], "radioChannelWidth": basicService['radio']['channelWidth'],
                                                       "radioPower": basicService['radio']['power'], "radioIndex": basicService['radio']['index'] } } )

            organization["networks"] = list(nets)
            organizations.append(organization)

    return organizations


def delivery_report(err, msg):
    if err is not None:
        logger.warning(f'Delivery failed: {err}')
    else:
        logger.warning(f'Message delivered to {msg.topic()} [{msg.partition()}]')



#######################################
#          get_meraki_token           #
#######################################
def get_meraki_token(client_name):
    try:
        payload = { 'partner_name': (None, client_name) }
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



#######################################
#               Main                  #
#######################################
def main(client_name):
    arg_apiKey = get_meraki_token(client_name)
    if arg_apiKey is None:
        return
    organizations = getInitialData(arg_apiKey)
    if organizations:
        topic_names = [f"galileu.assessment.{org['id']}" for org in organizations]
        # Creating topics
        new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topic_names]
        fs = admin_client.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()  # if error, go to except
            except Exception as e:
                logger.critical(f"Topic {topic} already exists or any other error: {e}")

        for org in organizations:
            topic = f"galileu.assessment.{org['id']}"
            for network in org["networks"]:
                network_id = network["id"]
                message = {
                    "org_id": org["id"],
                    "org_name": org["name"],
                    "customerNumber": org["customerNumber"],
                    "network": network
                }
                try:
                    producer.produce(
                        topic=topic,
                        key=network_id.encode('utf-8'),
                        value=json.dumps(message).encode('utf-8'),
                        callback=delivery_report
                    )
                except BufferError:
                    producer.poll(1)

                producer.poll(0)

    producer.flush()


if __name__ == '__main__':
    client_name  = sys.argv[1]
    main(client_name)
