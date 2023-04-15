import requests

url = 'http://localhost:2000/submit'

while True:
    # key = input('Enter key (or "exit" to quit): ')
    # if key == 'exit':
    #     break
    command = input('Enter command: ')
    data = {'data': command}
    response = requests.post(url, data=data)
    print(response.text)
