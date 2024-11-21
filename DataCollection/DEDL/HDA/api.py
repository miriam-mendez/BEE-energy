from tqdm import tqdm
import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


class Client(object):
    def __init__(self, user=None, psswd=None, storage = None, session=requests.Session()):
        self.username = user
        self.password = psswd
        self.session = session

        self.storage = storage
        token_response = self.session.post(
            'https://identity.data.destination-earth.eu/auth/realms/dedl/protocol/openid-connect/token',
            data = {'grant_type': 'password','scope' : 'openid', 'client_id' : 'hda-public', 'username' : self.username, 'password' : self.password},
            headers = {'Content-Type' : 'application/x-www-form-urlencoded'}
        )
        if token_response.status_code == 200:
            self.token = token_response.json()["access_token"]
            self.session.headers = {'Authorization': f'Bearer {self.token}'}
        else:
            raise Exception(f"Failed to retrieve token: {token_response.status_code}")
        

    def retrieve(self,query,datechoice=None,filename=None,bucket=None):
        
        if datechoice is not None: query["datetime"] = datechoice

        response = requests.post("https://hda.data.destination-earth.eu/stac/search", 
                                 headers=self.session.headers, json=query)
        print(response.status_code)
        product = response.json()["features"][0]
        download_url = product["assets"]["downloadLink"]["href"]       

        direct_download_url=''
        response = self.session.get(download_url)
        if (response.status_code == 200):
            direct_download_url = product['assets']['downloadLink']['href']
        while(response.status_code == 500):
            response = self.session.get(download_url)
            print(response.status_code)
            print(response.json())
            time.sleep(0.1)
            
        try:
            # we poll as long as the data is not ready
            retry = Retry(
                total=4,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry)
            self.session.mount('https://', adapter)
            if direct_download_url == '':
                while url := response.headers.get("Location"):
                    print(f"order status: {response.json()['status']}")
                    response = self.session.get(url, stream=True)
                    response.raise_for_status()
            
        except Exception as e:
            print(e)
        # Check if Content-Disposition header is present
            
        if "Content-Disposition" not in response.headers:
            print(response.headers)
            raise Exception("Content-Disposition header not found in response. Must be something wrong.")

        total_size = int(response.headers.get("content-length", 0))
        print("##########")
        print(response.headers)


        with tqdm(total=total_size, unit="B", unit_scale=True) as progress_bar:
            with open(filename, 'wb') as f:
                for data in response.iter_content(1024):
                    progress_bar.update(len(data))
                    f.write(data)
