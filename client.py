from utils import parse_config
import requests
config = parse_config()
ports = config['client_ports']
ports = [int(i) for i in ports]
# url = 'http://localhost:2000/submit'

while True:
    command = input('Enter command: ')
    data = {'data': command,'index':'0'}
    for port in ports:
        url = f'http://localhost:{port}/submit'
        try:
            response = requests.post(url, data=data)
            break
        except:
            pass
            # print("======Trying next port available=======")
    print(response.text)
