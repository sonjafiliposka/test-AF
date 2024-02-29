#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""The Bastion DAG."""
from __future__ import annotations

import datetime
import requests
import pendulum
import json
from pathlib import Path

from requests.auth import HTTPBasicAuth

from airflow.models.dag import DAG
from airflow.exceptions import AirflowFailException

from datetime import datetime
from datetime import timedelta
from airflow.decorators import task
from base64 import b64decode, b64encode

with DAG(
    dag_id="bastion_send_key_task",
    start_date=pendulum.datetime(2024, 2, 22, tz="UTC"),
    schedule_interval=None,
    catchup=False,
    tags=["bastion"],
    params={"user_id": "-1", "end_date": "", "reference_id": ""},
) as dag:


    # [START read config]
    @task
    def read_config():
        print(Path.cwd())
        f = open("config/bastion_send_key.json")
        config = json.load(f)
        return {"x-booked-librebooking_url": config["librebooking_url"],
                "x-booked-librebooking_auth": config["librebooking_auth"],
		"x-booked-bastion_url": config["bastion_url"],
                "x-booked-bastion_auth": config["bastion_auth"]
	}
    # [END read config]

    # [START librebooking authentication function]
    @task
    def librebooking_authentication(conf_file):
        split = conf_file['x-booked-librebooking_auth'].strip().split(' ')
        username, password = b64decode(split[0]).decode().split(':', 1)

        json_data = {
            'username': username,
            'password': password,
        }

        print("Username ", username)
        print("Password ", password)


        headers = {'Content-Type': 'application/json',}

        response = requests.post(conf_file['x-booked-librebooking_url'] + '/Authentication/Authenticate',
                                 json=json_data, headers=headers)
        json_response = response.json()

        print("UserID ", json_response["userId"])
        print("SessionToken ", json_response["sessionToken"])

        return {"x-booked-userid": json_response["userId"], "x-booked-sessiontoken": json_response["sessionToken"]}
    # [END librebooking authentication function]


    # [START librebooking get user key]
    @task
    def librebooking_get_user_key(auth, conf_file, dag_run: DagRun | None = None):
        userid = auth['x-booked-userid']
        session_token = auth['x-booked-sessiontoken']

        print("UserSessionID ", userid)
        print("UserSessionToken ", session_token)

        headers = {'x-booked-userid': userid,
                   'x-booked-sessiontoken': session_token,}

        print ("User ID " + dag_run.conf["user_id"])
        print("Headers ", headers)

        response = requests.get(conf_file['x-booked-librebooking_url'] + '/Users/' + dag_run.conf["user_id"], headers=headers)
        json_response = response.json()

        ssh_key = json_response['customAttributes'][0]['value']

        return {"ssh_key": ssh_key}
    # [END librebooking get user key]


    # [START bastion send user key]
    @task
    def bastion_send_user_key(ssh_key, conf_file, dag_run: DagRun | None = None):
        key = ssh_key['ssh_key']
        reference_id = dag_run.conf["reference_id"]
        separator = '== '
        key_to_send = key.rsplit(separator, 1)[0] + separator + reference_id
        now = datetime.now()
        enable_date = now.replace(microsecond=0).isoformat() + "Z"
        print("Enabled date ", enable_date)

        now_plus_one_day = now + timedelta(days=1)
        disable_date = dag_run.conf["end_date"]
        print("Disable date ", disable_date)

        split = conf_file['x-booked-bastion_auth'].strip().split(' ')
        username, password = b64decode(split[0]).decode().split(':', 1)

        json_data = {
            'enable_date': enable_date,
            'disable_date': disable_date,
            'active': False,
            'str_key': key_to_send,
            'note': 'send from airflow',
        }

        headers = {'Content-Type': 'application/json',}

        print(json_data)
        response = requests.post(conf_file['x-booked-bastion_url'],
                                 headers=headers,
                                 json=json_data,
                                 auth = HTTPBasicAuth(username, password))
        print(response)
        print(response.content)
        if response.status_code != 201:
            raise AirflowFailException

    # [END bastion send user key]
    conf_file = read_config()
    libre_auth = librebooking_authentication(conf_file)
    ssh_key = librebooking_get_user_key(libre_auth, conf_file)
    bastion_send_user_key(ssh_key, conf_file)

if __name__ == "__main__":
    dag.test()
