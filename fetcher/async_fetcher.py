from .fetcher import FetchDataAsync, FetchMSPAvailableFields

from multiprocessing import Queue, Process

import time


def FetchMSPDataAsync(auth_creds, endpoint_list):
    start_time2 = time.time()
    print("------- Getting available fields in MSP Project Online -------")
    available_fields = FetchMSPAvailableFields(auth_creds)

    if not isinstance(endpoint_list, list):
        raise Exception("You must pass a list to FetchDataAsync function")

    print("------- Verifying requested fields exist -------")
    for endpoint in endpoint_list:
        if endpoint not in available_fields:
            print("MSP Project field: ", endpoint, " doesn't exist")
            print("Here are the available fields: ")
            print(available_fields)
            raise Exception("Field not available in msp project online")

    print("------- SUCCESS: requested fields exist in MSP Project Online -------")
    endpoint_queue_map = {}

    for endpoint in endpoint_list:
        if endpoint not in endpoint_queue_map:
            endpoint_queue_map[endpoint] = Queue()

    process_list = []

    for endpoint in endpoint_queue_map:
        process_list.append(
            Process(
                target=FetchDataAsync,
                args=(auth_creds, endpoint, ["all"], endpoint_queue_map[endpoint]),
            )
        )

    print("------- Starting the fetching processes -------")
    for job in process_list:
        job.start()

    endpoint_df_map = {}

    for endpoint in endpoint_queue_map:
        endpoint_df_map[endpoint] = endpoint_queue_map[endpoint].get()

    print(
        "************* Async Fetch took --- %s seconds --- to execute *************"
        % (time.time() - start_time2)
    )

    return endpoint_df_map
