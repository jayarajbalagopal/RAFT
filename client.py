from utils import parse_config
import requests
config = parse_config()
ports = config['client_ports']
ports = [int(i) for i in ports]
# url = 'http://localhost:2000/submit'

while True:
    # key = input('Enter key (or "exit" to quit): ')
    # if key == 'exit':
    #     break
    command = input('Enter command: ')
    data = {'data': command}
    for port in ports:
        url = f'http://localhost:{port}/submit'
        try:
            response = requests.post(url, data=data)
            break
        except:
            print("======Trying next port available=======")
    print(response.text)
