import json
import requests
import pandas as pd


class Client(object):
    
    def __init__(self, token_response=None, session=requests.Session()):
        if token_response:
            self.token = token_response.text
            self.session.headers = {'Authorization': f'Bearer {self.token}'}
        else:
            raise Exception(f"Failed to retrieve token: {token_response.status_code}")


    def retrieve(self, name, request, target=None):
        result = self.session.get(name, params=request)
        data = json.loads(result.text.encode("latin_1").decode("utf_8"))
        if 'page' in request:
            datadis_dataset = []
            more_data = True
            while more_data:
                if len(result.json()) != 0:
                    datadis_dataset.extend(json.loads(result.text.encode("latin_1").decode("utf_8")))
                    request['page'] += 1 
                    result = self.session.get(name, params=request)
                else:
                    more_data = False
            data = datadis_dataset

        if target is not None:
            pd.DataFrame(data).to_csv(target)

        return result
