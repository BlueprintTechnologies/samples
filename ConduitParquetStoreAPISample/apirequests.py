import requests
import json
import sys





def get_token(conduit_url, conduit_user, conduit_password):
    data = {
        "email": conduit_user,
        "password": conduit_password
    }
    headers = {
        "Content-type": "application/json",
        "Accept": "*/*"
    }
    response = requests.post(f"{conduit_url}/auth", data=json.dumps(data), headers=headers)

    print("The status code for the authentication request is:", response.status_code)
    if response.status_code == 200: print("The authentication was successful")
    assert response.status_code == 200, 'The authentication failed. Cannot continue'
    if response.status_code != 200: sys.exit()

    global token
    token = response.json()['jwtToken']
    print("The access token is:", token)
    return token

def get_all_parquet_entities(conduit_url, token):
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"{conduit_url}/api/parquet-store", headers=headers)

    print("The status code for getting all parquet entities request is:", response.status_code)
    if response.status_code == 200: print("The parquet entities were acquired successfully")
    assert response.status_code == 200, "The parquet entities were not acquired. Cannot continue"
    if response.status_code != 200: sys.exit()

    json_response = response.json()
    pretty_response = json.dumps(json_response, indent=4)
    response_dict = json.loads(pretty_response)
    return response_dict

def trigger_materialization_job(conduit_url, token, connector_name, ext_table_name):
    headers = {"Authorization": f"Bearer {token}",
               "Accept": "*/*"
               }
    response = requests.post(f"{conduit_url}/api/parquet-store/{connector_name}/{ext_table_name}",headers=headers)

    print("The status code for triggering the materialization request is:", response.status_code)
    if response.status_code == 200: print("The materialization was triggered successfully")
    assert response.status_code == 200, "The materialisation was not triggered. Cannot continue"
    if response.status_code != 200: sys.exit()

def get_parquet_store_entity_for_triggered_job(conduit_url, token, connector_name, ext_table_name):
    headers = {"Authorization": f"Bearer {token}",
               "Accept": "*/*"
               }
    response = requests.get(f"{conduit_url}/api/parquet-store/{connector_name}/{ext_table_name}",headers=headers)

    print("The status code for getting the parquet store entity for the triggered job is:", response.status_code)
    assert response.status_code == 200, "Information about the previously triggered job was not acquired"
    if response.status_code != 200: sys.exit("Information was not acquired. Cannot continue")

    complete_response = response.json()
    return complete_response

def cancel_materialization_job(conduit_url, token, connector_name, ext_table_name):
    headers = {"Authorization": f"Bearer {token}",
               "Accept": "*/*"
               }
    response = requests.delete(f"{conduit_url}/api/parquet-store/cancel/{connector_name}/{ext_table_name}", headers=headers)

    print("The status code for cancelling the running job request is:", response.status_code)
    if response.status_code == 200: print("The job was successfully cancelled")
    assert response.status_code == 200, "The job was not cancelled"

def get_parquet_store_entity(conduit_url, token, connector_name, ext_table_name):
    headers = {"Authorization": f"Bearer {token}",
               "Accept": "*/*"
               }
    response = requests.get(f"{conduit_url}/api/parquet-store/{connector_name}/{ext_table_name}", headers=headers)

    print("The status code for getting information about the successfully materialized dataset is:", response.status_code)
    if response.status_code == 200: print("Information about the finished job was successfully acquired")
    assert response.status_code == 200, 'Information about the finished job was not acquired'
    if response.status_code != 200: sys.exit("Information was not acquired. Cannot continue")

    json_response2 = response.json()
    return json_response2

def delete_parquet_store_entity(conduit_url, token, connector_name, ext_table_name):
    headers = {"Authorization": f"Bearer {token}",
               "Accept": "*/*"
               }
    response = requests.delete(f"{conduit_url}/api/parquet-store/{connector_name}/{ext_table_name}", headers=headers)

    print("The status code for deleting the parquet store entity request is:", response.status_code)
    if response.status_code == 200: print("Parquet entity was successfully deleted")
    assert response.status_code == 200, 'The parquet entity was not deleted'