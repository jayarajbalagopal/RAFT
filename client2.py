import time
from utils import parse_config
import requests
from flask import Flask, request
import threading

config = parse_config()
ports = config['client_ports']
ports = [int(i) for i in ports]
# url = 'http://localhost:2000/submit'
commands_list=[]
acked_upto=0
def send_commands():
    while True:
        if(acked_upto<len(commands_list)):
            print(f"acked_upto: {acked_upto},{commands_list}")
            data = {'data': commands_list[acked_upto],'index':acked_upto}
            # print(data)
            for port in ports:
                url = f'http://localhost:{port}/submit'
                try:
                    response = requests.post(url, data=data)
                    print(response.text)

                    break
                except:
                    pass
                    # print("======Trying next port available=======")
        # print("sleeping for t sec")
        else:
            break

        time.sleep(4)



app = Flask(__name__)

def update_commands(data):
    global acked_upto
    acked_upto = data

@app.route('/', methods=['POST'])
def handle_post_request():
    data = request.json
    print(data)
    update_commands(data['commit_len'])
    return f'client ack updated successfully{data["commit_len"]}'

def run_server():
    print("starting flask server")
    app.run(debug=False,port=config['client_ack_port'])

thread = threading.Thread(target=run_server)
thread.start()

while True:
    command = input('Enter command: ')
    commands_list.append(command)
    command_thread=threading.Thread(target=send_commands)
    command_thread.start()








