import json
import requests
import pandas as pd
import os
from io import StringIO
import time

class Client(object):

    def __init__(self, user=None, psswd=None, storage = None, quiet=False, timeout=60, session=requests.Session()):
        self.user = user
        self.psswd = psswd
        self.session = session
        self.storage = storage
        token_response = self.session.post("https://datadis.es/nikola-auth/tokens/login",{'username':user,'password':psswd})
        if token_response.status_code == 200:
            self.token = token_response.text
            self.session.headers = {'Authorization': f'Bearer {self.token}'}
        else:
            raise Exception(f"Failed to retrieve token: {token_response.status_code}")


    def retrieve(self, name, request, target=None, bucket = None):
        while True:
            try:
                result = requests.get(name, params=request, headers=self.session.headers)
                if 'page' in request:
                    datadis_dataset = []
                    more_data = True
                    page = request['page']
                    while more_data:
                        request['page'] = page
                        result = requests.get(name, params=request, headers=self.session.headers)
                        if len(result.json()) != 0:
                            datadis_dataset.extend(json.loads(result.text.encode("latin_1").decode("utf_8")))
                            page += 1
                        else:
                            more_data = False
                    data = datadis_dataset
                else:
                    result = requests.get(name, params=request, headers=self.session.headers)
                    data = json.loads(result.text.encode("latin_1").decode("utf_8"))

                if bucket is not None:
                    self.storage.create_bucket(Bucket=bucket)
                    csv_buffer = StringIO()
                    pd.DataFrame(data).to_csv(csv_buffer)
                    self.storage.put_object(Bucket=bucket, Key=target, Body=csv_buffer.getvalue())
                elif target is not None:
                    os.makedirs(os.path.dirname(target),exist_ok=True)
                    pd.DataFrame(data).to_csv(target)
            
            except Exception:
                print("Error retrieving the data, trying again!")
                time.sleep(0.1)
            else:
                break

        return data

