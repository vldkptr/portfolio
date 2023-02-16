import pandas as pd
import numpy as np
from sqlalchemy import create_engine, DateTime, String, Integer, Float
import psycopg2
import base64
import hashlib
import hmac
from datetime import datetime, timedelta, date
import requests
from requests.auth import HTTPBasicAuth
import json
from decouple import config

today = str(
    (datetime.today() + timedelta(hours=10)).strftime("%d.%m.%Y %H:%M:%S")
) + str(" (GMT+10)")

engine_gp = config("engine_gp", default="")


def execute_GP(query):
    """Postgres fetching data function
    Arguments:
    query - str, sql query
    Return:
    pandas DataFrame containing the result of the query, the names of the columns from the query"""

    engine = create_engine(engine_gp)

    df = pd.read_sql_query(sql=query, con=engine)

    engine.dispose()

    return df


def getRecepients(name, nameuuid):
    """Function for generating a dictionary of recipients
    Arguments:
    name = str, full name of the employee,
    nameuuid = str, uuid of the employee
    Return:
    dict"""

    recepient = []
    recepient.append({"name": str(name), "address": str(nameuuid), "type": 0})
    return recepient


def getOrderLink(link, uuid):
    """Function for getting a hyperlink to the order document.
    Arguments:
    link - str, Document entity,
    uuid - uuid link
    Return:
    str of url"""
    order = (
        "[" + str(link) + "]" + "(https://companysite.com/?Orderid-" + str(uuid) + ")"
    )
    return order


def getreceiptLink(receipt_link, receipt_uuid):
    """Function for getting a hyperlink to the receipt document.
    Arguments:
    receipt_link - str, document entity,
    receipt_uuid - str, uuid link
    Return:
    str of url"""
    receipt = (
        "["
        + str(receipt_link)
        + "]"
        + "(http://companysite.com/?Receiptid-"
        + str(receipt_uuid)
        + ")"
    )
    return receipt


def fillRequest(subject, body, recepient):
    """Function for generating json request.
    Arguments:
    Subject - str, message subject,
    Body - str, message text,
    recipient - dict, list of recipients
    Return:
    json"""
    request = {}
    request.update(
        {
            "senderName": "System",
            "content": {"subject": str(subject), "body": str(body)},
            "toRecipients": recepient,
            "ccRecipients": [
                {
                    "name": "Patrakhin Vladislav",
                    "address": "patrakhin@corpmail.com",
                    "type": 0,
                },
                {
                    "name": "Ivanov Ivan", 
                    "address": "ivanovi@corpmail.com", 
                    "type": 0
                },
            ],
            "priority": 2,
        }
    )
    return request


def sendMessageNotepad(request):
    """A function to send a message
    Arguments:
    request - json, messages arguments
    """

    login = config("apilogin", default="")
    key = config("apikey", default="")

    byte_key = myencode_method(key)
    message = myencode_method(login)
    encoded = myencode_method(byte_key, message)
    # print(encoded)

    URL = "https://companysite.com/api"  #
    HEADERS = {"Content-Type": "text/json", "Messaging-Type": "User"}

    r = requests.post(
        url=URL,
        headers=HEADERS,
        auth=HTTPBasicAuth(login, encoded),
        json=request,
        verify=False,
        timeout=15000,
    )

    r_json = ""
    try:
        r_json = r.json()
        print(r_json)
    except json.decoder.JSONDecodeError:
        print("Ошибка декодирования")


PG_PRIM_ENGINE_PATR = config("PG_PRIM_ENGINE_PATR", default="")


def execute_patrakhin(query):
    """Postgres logs fetching data function
    Arguments:
    query - str, SQL query
    Return:
    pandas DataFrame containing the result of the query, the names of the columns from the query"""

    engine = create_engine(PG_PRIM_ENGINE_PATR)

    df = pd.read_sql_query(sql=query, con=engine)
    engine.dispose()

    return df


def paint_red_text(text):
    """A function to paint text red in html style
    Agruments: text - str, text should be painted red
    Return: str, text with red color"""
    text = f'<span style="color:red">{text}</span>'
    return text


print("Time is:", today)
print("Module 'notifier_conn' has been loaded")
