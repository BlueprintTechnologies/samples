import requests
import json
import time
import apirequests

conduit_user = "<Enter your Conduit account username here>"
conduit_password = "<Enter your Conduit account password here>"
conduit_url = "<Enter Conduit url here>"

def test_complete_parquet_scenario():

        #authenticate & get token

        print('Making an authentication request in order to get the access token...')
        token = apirequests.get_token(conduit_url, conduit_user, conduit_password)

        #get all parquet entities with their most relevant info

        print('Making a request to get all parquet entities, looking for one that is not materialized...')
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{conduit_url}/api/parquet-store", headers = headers)

        print('The status code for getting all parquet store entities request is:', response.status_code)
        if response.status_code == 200: print("The parquet entities were acquired successfully")
        assert response.status_code == 200, 'The parquet entities were not acquired. Test FAILED'

        json_response = response.json()
        pretty_response = json.dumps(json_response, indent=4)
        response_dict = json.loads(pretty_response)
        what_is_needed = []
        for each_entry in response_dict:
            what_is_needed.append({
                'connectortype': each_entry['connectorType'],
                'connectorname': each_entry['connectorName'],
                'tablename': each_entry['tableName'],
                'correlationid': each_entry['correlationId'],
                'cacheenabled': each_entry['cacheEnabled'],
                'status': each_entry['status'],
                'starttime': each_entry['startTime'],
                'finishtime': each_entry['finishTime'],
                'autorefresh': each_entry['autoRefresh'],
                'materialized': each_entry['materialized']
            })
        print("These are all the parquet entities with their most relevant information:")
        for details in what_is_needed:
            print(details)

        #check if there is an unmaterialized dataset, and if yes, trigger materialization

        #check if there is an unmaterialized dataset

        unmaterialised = None
        for every_entry in response_dict:
             if every_entry['materialized'] is False:
                unmaterialised = every_entry
                print("This is one unmaterialized dataset:", unmaterialised)
                break
        assert unmaterialised != None, 'There is no unmaterialised dataset. Cannot continue'

        #trigger materialization if an unmaterialised dataset was found

        new_unmaterialised = list(unmaterialised.items())
        connector_type = (new_unmaterialised[2][1])
        connector_name = (new_unmaterialised[3][1])
        ext_table_name = (new_unmaterialised[5][1])
        dataset_name = (new_unmaterialised[4][1])
        print("The connector type is:", connector_type)
        print("The connector name is:", connector_name)
        print("The dataset name is:", dataset_name)
        print("The extended table name is:", ext_table_name)

        print(f"Triggering materialization for the unmaterialized {dataset_name} dataset...")
        headers = {"Authorization": f"Bearer {apirequests.token}",
                   "Accept": "*/*"
                   }
        response = requests.post(f"{conduit_url}/api/parquet-store/{connector_name}/{ext_table_name}", headers=headers)

        print('The status code for triggering the materialization job request is:', response.status_code)
        if response.status_code == 200: print("The materialization was triggered successfully")
        assert response.status_code == 200, 'The materialisation was not triggered. Test FAILED'

        time.sleep(10)

        #get info about the parquet entity for which materialisation has been previously triggered

        print('Getting information about the materialization job previously triggered...')
        headers = {"Authorization": f"Bearer {apirequests.token}",
                   "Accept": "*/*"
                   }
        response = requests.get(f"{conduit_url}/api/parquet-store/{connector_name}/{ext_table_name}", headers=headers)

        print('The status code for getting the parquet store entity for the triggered job is:', response.status_code)
        assert response.status_code == 200, 'Information about the previously triggered job was not acquired. Cannot continue'

        complete_response = response.json()
        entity_status = complete_response.get('status')
        print('The status of the job is:', entity_status)
        corr_id = complete_response.get('correlationId')
        print('The correlation id is:', corr_id)

        #cancel job or get info and then delete the dataset, depending on status

        #cancel running job

        if entity_status == 'Running':
            print("The job is running. It will be cancelled")
            headers = {"Authorization": f"Bearer {apirequests.token}",
                       "Accept": "*/*"
                       }
            response = requests.delete( f"{conduit_url}/api/parquet-store/cancel/{connector_name}/{ext_table_name}",headers=headers)

            print('The status code for cancelling the running job request is:', response.status_code)
            if response.status_code == 200: print("The job was sucessfully cancelled")
            assert response.status_code == 200, 'The job was not cancelled. Test FAILED'

        elif entity_status == 'Finished':
            print("The job is finished. Information about it will be acquired and then, the parquet entity will be deleted")

        #get info about finished job

            print("Getting information about the successfully materialized dataset...")
            headers = {"Authorization": f"Bearer {apirequests.token}",
                       "Accept": "*/*"
                       }
            response = requests.get(f"{conduit_url}/api/parquet-store/{connector_name}/{ext_table_name}", headers=headers)

            print('The status code for getting information about the successfully materialized dataset is:', response.status_code)
            if response.status_code == 200: print("Information about the finished job was successfully acquired")
            assert response.status_code == 200, 'Information about the materialized parquet entity was not acquired. Test FAILED'

            json_response2 = response.json()
            pretty_response_list = []
            pretty_response_list.append(json_response2)
            useful_info = []
            for each_info in pretty_response_list:
                useful_info.append({
                    'connectortype': each_info['connectorType'],
                    'connectorname': each_info['connectorName'],
                    'tablename': each_info['tableName'],
                    'correlationid': each_info['correlationId'],
                    'status': each_info['status'],
                    'path': each_info['path'],
                    'numberoffiles': each_info['numberOfFiles'],
                    'starttime': each_info['startTime'],
                    'finishtime': each_info['finishTime'],
                    'duration': each_info['duration'],
                    'expectedexpirationsec': each_info['expectedExpirationSec'],
                    'autorefresh': each_info['autoRefresh'],
                    'materialized': each_info['materialized']
                })
            print('This is the most relevant information about the newly materialized parquet entity:')
            useful_info_pretty = json.dumps((useful_info), indent=4)
            print(useful_info_pretty)

            #delete dataset

            print("Making a request to delete the parquet entity...")
            headers = {"Authorization": f"Bearer {apirequests.token}",
                       "Accept": "*/*"
                       }
            response = requests.delete(f"{conduit_url}/api/parquet-store/{connector_name}/{ext_table_name}", headers=headers)

            print('The status code for deleting the parquet entity request is:', response.status_code)
            if response.status_code == 200: print("The parquet entity was successfully deleted")
            assert response.status_code == 200, 'The parquet entity was not deleted. Test FAILED'

        else:
            different = True
            print("The job either failed or is already cancelled.")
            assert (different != True), "Test FAILED"




