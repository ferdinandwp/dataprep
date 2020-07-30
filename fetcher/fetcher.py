import pandas as pd
import xml.etree.ElementTree as ET
import feedparser
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.runtime.client_request import ClientRequest
from office365.runtime.utilities.request_options import RequestOptions


def ParseAndAddToDict(dataList, key, d):
    if key not in d:
        d[key] = []

    for data in dataList:
        root = ET.fromstring(data.content)
        for elem in root.iter():
            if elem.tag[-len(key) :].lower() == key.lower():
                x = elem.tag.lower().find(key.lower())
                if elem.tag[x - 1] == "}":
                    d[key].append(elem.text)


def GetListOfEntries(dataList):
    l = set()
    data = dataList[0]
    root = ET.fromstring(data.content)

    for elem in root.iter():
        if not (
            elem.tag.lower().find("metadata") != -1
            or elem.tag.lower().find("w3.org") != -1
        ):
            lastoccurence = elem.tag.rfind("}")
            if not elem.tag[lastoccurence + 1 :] in l:
                l.add(elem.tag[lastoccurence + 1 :])

    return l


def ParseAndAddAllToDict(dataList, d):
    all_entries_list = GetListOfEntries(dataList)
    for entrykey in all_entries_list:
        ParseAndAddToDict(dataList, entrykey, d)


def GetAllData(auth_creds, request, uri):
    MSP_ROOT_URL = auth_creds[0]
    MSP_USERNAME = auth_creds[1]
    MSP_PASS = auth_creds[2]
    dList = []
    skip = 0
    options = RequestOptions(MSP_ROOT_URL + uri + "?$skip=" + str(skip))
    data = request.execute_request_direct(options)
    dList.append(data)
    dataLen = len(feedparser.parse(data.text)["entries"])

    while dataLen != 0:
        skip += dataLen
        options = RequestOptions(MSP_ROOT_URL + uri + "?$skip=" + str(skip))
        data = request.execute_request_direct(options)
        dList.append(data)
        dataLen = len(feedparser.parse(data.text)["entries"])

    return dList


def FetchMSPData(auth_creds, endpoint, entrylist):

    print("Fetching data for: ", endpoint)
    MSP_ROOT_URL = auth_creds[0]
    MSP_USERNAME = auth_creds[1]
    MSP_PASS = auth_creds[2]
    ctx_auth = AuthenticationContext(MSP_ROOT_URL)
    if ctx_auth.acquire_token_for_user(MSP_USERNAME, MSP_PASS):
        request = ClientRequest(ctx_auth)

        dataList = GetAllData(
            auth_creds, request, "/sites/pwa/_api/projectdata/{0}".format(endpoint)
        )

        d = {}

        if len(entrylist) == 1 and entrylist[0] == "all":
            ParseAndAddAllToDict(dataList, d)
        else:
            for entry in entrylist:
                ParseAndAddToDict(dataList, entry, d)

        data_df = pd.DataFrame(d)
        print("Success data fetching for: ", endpoint)
        return data_df

    else:
        print(ctx_auth.get_last_error())


def FetchMSPAvailableFields(auth_creds):
    MSP_ROOT_URL = auth_creds[0]
    MSP_USERNAME = auth_creds[1]
    MSP_PASS = auth_creds[2]
    ctx_auth = AuthenticationContext(MSP_ROOT_URL)
    if ctx_auth.acquire_token_for_user(MSP_USERNAME, MSP_PASS):
        request = ClientRequest(ctx_auth)
        options = RequestOptions(MSP_ROOT_URL + "/sites/pwa/_api/projectdata")
        data = request.execute_request_direct(options)
        root = ET.fromstring(data.content)
        l = set()
        for elem in root.iter():
            if elem.text:
                if not (elem.text == "None" or elem.text == "Default"):
                    l.add(elem.text)
    return l


def FetchDataAsync(auth_creds, endpoint, entrylist, q):
    q.put(FetchMSPData(auth_creds, endpoint, entrylist))