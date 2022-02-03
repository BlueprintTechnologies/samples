
import json
import time
import sys
import apirequests


conduit_user = "Enter here your Conduit account user"
conduit_password = "Enter here your Conduit account password"
conduit_url = "Enter here your Conduit url"

#authenticate & get token

print ("Making an authentication request in order to get the access token...")
token = apirequests.get_token(conduit_url, conduit_user, conduit_password)


#list all parquet store entities with their most relevant information

print ("Making a request to get all parquet store entities, looking for one that is not materialized...")
response_dict = apirequests.get_all_parquet_entities(conduit_url, token)

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
         'materialized': each_entry['materialized'],
        })
print("These are all the parquet store entities with their most relevant information:")
for details in what_is_needed:
     print(details)

#check if there is an unmaterialized dataset with a valid destination, and if there is one found, trigger a materialization job for it

#check if there is an unmaterialized dataset with a valid destination

unmaterialised = None
for every_entry in response_dict:
        if every_entry['materialized'] is False:
            unmaterialised = every_entry
            print("This is one unmaterialized dataset:", unmaterialised)
            new_unmaterialised = list(unmaterialised.items())
            dataset_name = (new_unmaterialised[4][1])
            connector_name = (new_unmaterialised[3][1])
            print(f"Checking if there is a valid destination set for {dataset_name} dataset...")
            json_response3 = apirequests.get_metadata(conduit_url, token, connector_name)
            defaultdestination = json_response3.get('parquetDefaultSink')
            print("The destination id is:", defaultdestination)
            if defaultdestination is not None:
                response4 = apirequests.get_destination_config(conduit_url, token, defaultdestination)
                if response4 != 200:
                    print("The destination was not successfully acquired. Another dataset needs to be chosen")
                    unmaterialised = None
                else:
                    connector_type = (new_unmaterialised[2][1])
                    ext_table_name = (new_unmaterialised[5][1])
                    print("The connector type is:", connector_type)
                    print("The connector name is:", connector_name)
                    print("The dataset name is:", dataset_name)
                    print("The extended table name is:", ext_table_name)
                    break

            else:
                print("There is no destination set for this dataset, materialization will not work. Another dataset needs to be chosen")
                unmaterialised = None
if unmaterialised is None:
   sys.exit("There is no unmaterialised dataset. Cannot continue")


# trigger materialization if an unmaterialised dataset has been found

print(f"Triggering materialization job for the unmaterialized {dataset_name} dataset...")
apirequests.trigger_materialization_job(conduit_url, token, connector_name, ext_table_name)

time.sleep(10)

#get info about the parquet entity for which materialisation has been previously triggered

print ("Getting information about the materialization job previously triggered...")
complete_response = apirequests.get_parquet_store_entity_for_triggered_job(conduit_url, token, connector_name, ext_table_name)

entity_status = complete_response.get('status')
print ("The status of the job is:", entity_status)
corr_id = complete_response.get('correlationId')
print("The correlation id is:", corr_id)

#cancel the job or get complete info and then delete the enitity, depending on status

#cancel the running job

if entity_status == 'Running':
    print ("The job is running. It will be cancelled")
    apirequests.cancel_materialization_job(conduit_url, token, connector_name, ext_table_name)

elif entity_status == 'Finished':
    print("The job is finished. Information about the parquet store entity will be acquired and then, the entity will be deleted")

#get info about finished job

    print("Getting information about the successfully materialized dataset...")
    json_response2 = apirequests.get_parquet_store_entity(conduit_url, token, connector_name, ext_table_name)

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
    print("This is the most relevant information about the newly materialized parquet entity:")
    useful_info_pretty = json.dumps((useful_info), indent=4)
    print (useful_info_pretty)

#delete parquet store entity

    print("Making a request to delete the parquet entity...")
    apirequests.delete_parquet_store_entity(conduit_url, token, connector_name, ext_table_name)

else:
    print("The job either failed or is already cancelled.")
