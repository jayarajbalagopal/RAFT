import requests

url = 'http://localhost:2000/submit'

while True:
    key = input('Enter key (or "exit" to quit): ')
    if key == 'exit':
        break
    value = input('Enter value: ')
    data = {key: value}
    response = requests.post(url, data=data)
    print(response.text)
