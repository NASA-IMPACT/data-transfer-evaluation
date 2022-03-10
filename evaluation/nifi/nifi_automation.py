from time import sleep
import requests
import urllib3
import string
import random
from datetime import datetime
import yaml

class NifiAutomation:

    def __init__(self, config_yaml) -> None:
        with open(config_yaml) as f:
            self.config = yaml.load(f)

    def run_automation(self):

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        def random_string(string_length=10):
            """Generate a random string of fixed length """
            letters = string.ascii_lowercase
            return ''.join(random.choice(letters) for i in range(string_length))


        nifi_url = 'https://localhost:8443/nifi-api'
        template_file = "nifi/nifi-s3.xml"
        log_file_location = "/proj/MFT/nifi-1.15.3/logs/nifi-app.log"

        source_token = self.config['source_token']
        source_secret = self.config['source_secret']
        source_s3_endpoint = self.config['source_s3_endpoint']
        source_s3_bucket = self.config['source_s3_bucket']
        source_s3_region = self.config['source_s3_region']

        dest_token = self.config['dest_token']
        dest_secret = self.config['dest_secret']
        dest_s3_endpoint = self.config['dest_s3_endpoint']
        dest_s3_bucket = self.config['dest_s3_bucket']
        dest_s3_region = self.config['dest_s3_region']

        total_files = int(self.config['total_files'])
        session_uuid = random_string(10)
        print("Session UUID: " + session_uuid)

        r = requests.get(nifi_url + '/flow/process-groups/root?uiOnly=true', verify=False)
        process_group_id = r.json()['processGroupFlow']['id']

        # Stopping the process group before resetting the template

        print("Stopping process group")
        update_process_group_json = {"id":process_group_id,"state":"STOPPED","disconnectedNodeAcknowledged":"false"}
        r = requests.put(nifi_url + '/flow/process-groups/' + process_group_id, json=update_process_group_json, verify=False)
        print("Status", r.status_code)


        # Deleting the existing flow

        r = requests.get(nifi_url + '/flow/process-groups/' + process_group_id , verify=False)

        process_groups_json = r.json()
        old_connections = process_groups_json['processGroupFlow']['flow']['connections']

        for connection in old_connections:
            print("Deleting connection " + connection['id'])
            r = requests.delete(nifi_url + '/connections/' + connection['id'] , params={"version": str(connection['revision']['version'])}, verify=False)
            print("Status", r.status_code)

        old_processors = process_groups_json['processGroupFlow']['flow']['processors']
        for processor in old_processors:
            print("Deleting processor ", processor['id'])
            r = requests.delete(nifi_url + '/processors/' + processor['id'], params={"version": str(processor['revision']['version'])}, verify=False)
            print("Status", r.status_code)

        # Deletes all template files

        r = requests.get(nifi_url + '/flow/templates', verify=False)
        templates = r.json()['templates']

        for template in templates:
            print("Deleting template ", template['id'], template['template']['name'])
            requests.delete(nifi_url + '/templates/' + template['id'], verify=False)

            # Upload the template file

        template_upload_json = {'template':(template_file, open(template_file, 'rb'), "multipart/form-data")}
        r = requests.post(nifi_url + '/process-groups/' + process_group_id + '/templates/upload', files=template_upload_json, verify=False)


        print('Template successfully uploaded')
        id_pos = r.text.find("<id>")
        id_end_pos = r.text.find("</id>")
        template_id = r.text[id_pos+4:id_end_pos]
        print('Template id: ' + template_id)

        template_load_json = {"templateId":template_id,"originX":611.2981057221397,"originY":85.35885905334999,"disconnectedNodeAcknowledged":"false"}
        r = requests.post(nifi_url + '/process-groups/' + process_group_id + '/template-instance', json=template_load_json, verify=False)


        template_json = r.json()
        processor_name_map = {}
        for processor in template_json['flow']['processors']:
            processor_name_map[processor['component']['name']] = processor

        # Updating the credentials

        list_s3_processor = processor_name_map['ListS3']
        list_s3_update_json = {"component":{"id":list_s3_processor['id'],"name":"ListS3","config":{"schedulingPeriod":"0 sec","executionNode":"PRIMARY","penaltyDuration":"30 sec","yieldDuration":"1 sec","bulletinLevel":"WARN","schedulingStrategy":"TIMER_DRIVEN","comments":"","autoTerminatedRelationships":[],"properties":{"Access Key":source_token,"Secret Key":source_secret}},"state":"STOPPED"},"revision":{"version":list_s3_processor['revision']['version']},"disconnectedNodeAcknowledged":"false"}

        print("Updating ListS3 processor")
        r = requests.put(nifi_url + '/processors/' + list_s3_processor['id'], json=list_s3_update_json, verify=False)
        print("Status", r.status_code)


        fetch_s3_processor = processor_name_map['FetchS3Object']
        fetch_s3_update_json = {"component":{"id":fetch_s3_processor['id'],"name":"FetchS3Object","config":{"concurrentlySchedulableTaskCount":"1","schedulingPeriod":"0 sec","executionNode":"ALL","penaltyDuration":"30 sec","yieldDuration":"1 sec","bulletinLevel":"WARN","schedulingStrategy":"TIMER_DRIVEN","comments":"","runDurationMillis":0,"autoTerminatedRelationships":["failure"],"properties":{"Access Key":source_token,"Secret Key":source_secret}},"state":"STOPPED"},"revision":{"version":fetch_s3_processor['revision']['version']},"disconnectedNodeAcknowledged":"false"}

        print("Updating FetchS3Object processor")
        r = requests.put(nifi_url + '/processors/' + fetch_s3_processor['id'], json=fetch_s3_update_json, verify=False)
        print("Status", r.status_code)


        put_s3_processor = processor_name_map['PutS3Object']
        put_s3_update_json = {"component":{"id":put_s3_processor["id"],"name":"PutS3Object","config":{"concurrentlySchedulableTaskCount":"1","schedulingPeriod":"0 sec","executionNode":"ALL","penaltyDuration":"30 sec","yieldDuration":"1 sec","bulletinLevel":"WARN","schedulingStrategy":"TIMER_DRIVEN","comments":"","runDurationMillis":0,"autoTerminatedRelationships":["failure"],"properties":{"Access Key":dest_token,"Secret Key":dest_secret}},"state":"STOPPED"},"revision":{"version":put_s3_processor['revision']['version']},"disconnectedNodeAcknowledged":"false"}

        print("Updating PutS3Object processor")
        r = requests.put(nifi_url + '/processors/' + put_s3_processor['id'], json=put_s3_update_json, verify=False)
        print("Status", r.status_code)


        print("Updating Start trnsfer log processor")
        start_log_processor = processor_name_map['Started transfer']
        start_log_update_json = {"component":{"id":start_log_processor['id'],"name":"Started transfer","config":{"concurrentlySchedulableTaskCount":"1","schedulingPeriod":"0 sec","executionNode":"ALL","penaltyDuration":"30 sec","yieldDuration":"1 sec","bulletinLevel":"WARN","schedulingStrategy":"TIMER_DRIVEN","comments":"","runDurationMillis":0,"autoTerminatedRelationships":["success"],"properties":{"log-message":"Starting the data transfer " + session_uuid + " ${filename}"}},"state":"STOPPED"},"revision":{"version":start_log_processor['revision']['version']},"disconnectedNodeAcknowledged":"false"}
        r = requests.put(nifi_url + '/processors/' + start_log_processor['id'], json=start_log_update_json, verify=False)
        print("Status", r.status_code)


        print("Updating Start trnsfer log processor")
        complete_log_processor = processor_name_map['Completed transfer']
        complete_log_update_json = {"component":{"id":complete_log_processor['id'],"name":"Completed transfer","config":{"concurrentlySchedulableTaskCount":"1","schedulingPeriod":"0 sec","executionNode":"ALL","penaltyDuration":"30 sec","yieldDuration":"1 sec","bulletinLevel":"WARN","schedulingStrategy":"TIMER_DRIVEN","comments":"","runDurationMillis":0,"autoTerminatedRelationships":["success"],"properties":{"log-message":"Completed the transfer " + session_uuid + " ${filename}"}},"state":"STOPPED"},"revision":{"version":complete_log_processor['revision']['version']},"disconnectedNodeAcknowledged":"false"}
        r = requests.put(nifi_url + '/processors/' + complete_log_processor['id'], json=complete_log_update_json, verify=False)
        print("Updating Complete trnsfer log processor")

        # Starting the process group
        #
        print("Starting process group")
        update_process_group_json = {"id":process_group_id,"state":"RUNNING","disconnectedNodeAcknowledged":"false"}
        r = requests.put(nifi_url + '/flow/process-groups/' + process_group_id, json=update_process_group_json, verify=False)
        print("Status", r.status_code)


        start_time_map = {}
        end_time_map = {}

        while(1):
            sleep(5)


            #read file line by line
            with open(log_file_location, 'r') as f:
                for line in f:
                    if line.find(session_uuid) != -1:
                        print(line)
                        utc_time = datetime.strptime(line.split(",")[0], "%Y-%m-%d %H:%M:%S")
                        file_name = line.split(session_uuid)[1].strip()
                        startLine = line.split(session_uuid)[0].find("Starting") > -1
                        if startLine:
                            start_time_map[file_name] = utc_time
                        else:
                            end_time_map[file_name] = utc_time

                if len(end_time_map) == total_files:
                    break


        final_time_array = []

        print("Finalizing Results")

        for key, start_time in start_time_map.items():
            end_time = end_time_map[key]
            print(key, (end_time - start_time).total_seconds())
            final_time_array.append([(start_time - datetime(1970, 1, 1)).total_seconds(), (end_time - datetime(1970, 1, 1)).total_seconds()])

        return final_time_array
