# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {}
# META   }
# META }

# CELL ********************

! python --version

! pip install azure-eventhub==5.11.5 faker==24.2.0 pyodbc==5.1.0 --upgrade --force --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from azure.eventhub import EventHubProducerClient, EventData
import os
import socket
import random
import sempy.fabric as fabric

from random import randrange

# Get connection string for a given Eventstream
def get_eventstream_connection_string(eventstream_name, eventstream_source_name):
    workspace_id = fabric.resolve_workspace_id()
    
    #Get Eventstream Id
    eventstream_id = fabric.resolve_item_id(eventstream_name)
    
    # Get Source Id
    client = fabric.FabricRestClient()
    url = f"v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/topology"
    response = client.get(url)
    for src in response.json().get("sources", []):
        if src.get("name") == eventstream_source_name and src.get("type") == "CustomEndpoint":
            eventstream_source_id = src.get("id")

    # Get connection string
    url = f"v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/sources/{eventstream_source_id}/connection"
    response = client.get(url)
    eventstream_connection_string = response.json()['accessKeys']['primaryConnectionString']
    return eventstream_connection_string

eventHubConnString = get_eventstream_connection_string(
        eventstream_name="Webevents_ES", 
        eventstream_source_name="WebEventsCustomSource"
    )

producer_events = EventHubProducerClient.from_connection_string(conn_str=eventHubConnString)

hostname = socket.gethostname()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from faker import Faker
from enum import Enum
import datetime

# class syntax
class EVENT_TYPE(Enum):
    CLICK = 1
    IMPRESSION = 2

productIds = [707,708,711,712,714,715,716,717,718,722,738,739,742,743,747,748,779,780,781,782,783,784,792,793,794,795,796,797,798,799,800,801,808,809,810,813,822,835,836,838,858,859,860,864,865,867,868,869,870,873,874,875,876,877,880,881,883,884,885,886,889,891,892,893,894,895,896,899,900,904,905,907,908,909,910,913,916,917,918,920,924,925,926,935,936,937,938,939,940,944,945,947,948,949,951,952,953,954,955,956,957,958,959,960,961,962,963,964,965,966,967,968,969,970,971,972,973,974,975,976,977,978,979,980,981,982,983,984,985,986,987,988,989,990,991,992,993,994,996,997,998,999]


def generateImpressionEvent(isAnomaly, productId):
    faker = Faker()

    event = {}
    event["eventType"] = EVENT_TYPE.IMPRESSION.name
    event["eventID"] = faker.uuid4()
   
    start_date = datetime.date(year=2024, month=1, day=1)
    end_date = datetime.datetime.now()
    event["eventDate"] = faker.date_time_between_dates(start_date, end_date).isoformat()

    if productId:
        event["productId"] = str(productId)
    else:
        event["productId"] = faker.random_element(productIds)

    randomizeUnsupported = randrange(100)
    userAgent = {}
    userAgent["platform"] = faker.random_element(["Windows", "Mac", "Linux", "iOS", "Android"])
    if randomizeUnsupported <2:
        userAgent["browser"] = "Unsupported"
    else:
        if userAgent["platform"] == "Windows":
            userAgent["browser"] = faker.random_element(["Edge", "Chrome", "Firefox", "Safari"])
        elif userAgent["platform"] == "Mac":
            userAgent["browser"] = faker.random_element(["Chrome", "Firefox", "Safari"])
        elif userAgent["platform"] == "Linux":
            userAgent["browser"] = faker.random_element(["Chrome", "Firefox"])
        elif userAgent["platform"] == "iOS":
            userAgent["browser"] = faker.random_element(["Safari", "Chrome"])
        elif userAgent["platform"] == "Android":
            userAgent["browser"] = faker.random_element(["Chrome", "Firefox"])
        userAgent["browserVersion"] = faker.random_element(["10.2", "13.6", "8.6", "8.5", "11.2", "14.6", "6.6", "4.5"])
    event["userAgent"] = userAgent

    event["device"] = faker.random_element(["mobile", "computer", "tablet", "mobile", "computer"])
    event["ip_address"] = faker.ipv4()

    # Adding related products
    extraPayload = []
    for i in range(randrange(1, 10)):  # Random number of related products between 1 and 4
        relatedproduct = {
                "relatedProductId": str(faker.random_element([708, 711, 712, 714, 715])),
                "relatedProductName": faker.word(),
                "relatedProductCategory": faker.random_element(["Electronics", "Books", "Clothing", "Home", "Toys"])
        }
        extraPayload.append(relatedproduct)
    event["extraPayload"] = extraPayload

    # only set the referer for CLICK events
    refererPayload = {}
    event["referer"] = refererPayload
    
    if isAnomaly:
        event["page_loading_seconds"] = faker.random_number(4)/100
    else:
        event["page_loading_seconds"] = faker.random_number(4)/1000
    return event


def generateClickEvent(impressionEvent, isAnomaly):
    faker = Faker()

    event = {}
    event["eventType"] = EVENT_TYPE.CLICK.name
    event["eventID"] = impressionEvent["eventID"]
    event["eventDate"] = impressionEvent["eventDate"]
    event["productId"] = impressionEvent["productId"]
    event["userAgent"] = impressionEvent["userAgent"]
    event["device"] = impressionEvent["device"]
    event["ip_address"] = impressionEvent["ip_address"]
    # Adding clickpath
    extraPayload = []
    for i in range(randrange(1, 10)):  # Random number of clicks between 1 and 10
        clickpath = {
            "clickType": faker.random_element(["button", "link", "image", "text"]),
            "url": faker.url(),
            "title": faker.random_element(["Brakes", "Helmets", "Battery", "Mirror", "Lights"])
        }
        extraPayload.append(clickpath)
    event["extraPayload"] = extraPayload

    # only set the referer for CLICK events, refererPayload differs by campaign type
    refererPayload = {}    
    refererPayload["url"] = faker.uri()
    refererPayload["campaignType"] = faker.random_element( ["organic", "bing", "google", "facebook", "instagram", "twitter", "pinterest", "email", "affiliate"])
    match refererPayload["campaignType"]:
        case "bing"| "google"| "facebook"| "instagram":
            refererPayload["medium"] = "cpc"
            refererPayload["adId"] = faker.uuid4()
            refererPayload["adGroup"] = faker.uuid4()
            refererPayload["adTitle"] = faker.sentence()
        case "twitter":
            refererPayload["medium"] = "cpc"
            refererPayload["adId"] = faker.uuid4()
        case "pinterest":
            refererPayload["medium"] = "cpc"
            refererPayload["adId"] = faker.uuid4()
        case "email":
            refererPayload["medium"] = "email"
            refererPayload["campaignId"] = faker.uuid4()
            refererPayload["emailId"] = faker.email()
        case "affiliate":
            refererPayload["medium"] = "affiliate"
            refererPayload["affiliateId"] = faker.uuid4()
    event["referer"] = refererPayload
    
    if isAnomaly:
        event["page_loading_seconds"] = faker.random_number(4)/100
    else:
        event["page_loading_seconds"] = faker.random_number(4)/1000
    return event

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def sendToEventsHub(jsonEvent, producer):
    eventString = json.dumps(jsonEvent)
    # print(eventString) 
    event_data_batch = producer.create_batch() 
    event_data_batch.add(EventData(eventString)) 
    producer.send_batch(event_data_batch)

def generateEvents(isAnomaly = False, productId = None):
    try:
        while True:
            impressionEvent = generateImpressionEvent(isAnomaly, productId)    
            sendToEventsHub(impressionEvent, producer_events)
            if random.randint(1, 100) > 80:
                clickEvent = generateClickEvent(impressionEvent, isAnomaly)    
                sendToEventsHub(clickEvent, producer_events)
    except KeyboardInterrupt:
        producer_events.close()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime
print(datetime.datetime.now())
generateEvents(False, None)
print(datetime.datetime.now())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
