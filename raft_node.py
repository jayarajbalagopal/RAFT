import os
import pickle
from flask import Flask, request
import zmq
import threading
import logging
import requests
import zmq
import time
import random
from multiprocessing import Queue
from utils import parse_config
from messages import (AppendEntryArgs, AppendEntryReply,
					RequestVoteArgs, RequestVoteReply)


logger = logging.getLogger('raft_logger')
config = parse_config()


class RaftNode:
	def __init__(self, node_id, node_addresses,ports):
		# Address and ID information
		self.node_id = node_id
		self.node_addresses = node_addresses
		self.node_address = self.node_addresses[self.node_id]
		self.node_ids = list(range(len(self.node_addresses)))
		self.peers = self.node_ids[:self.node_id] + self.node_ids[self.node_id+1:]
		




		# Raft variables
		self.state = "FOLLOWER"
		self.term = 0
		# self.log = [{'term': 0, 'command': 'None'}]
		self.log=[]
		self.commit_index = 0
		self.conflict_term = None
		self.conflict_index = None
		self.last_log_index = -1
		self.last_log_term = -1
		self.acked_length = [0 for _ in range(len(self.node_addresses))]
		self.sent_length = [0 for _ in range(len(self.node_addresses))]

		# Election parameters
		self.leader_id = -1
		self.voted_for = None
		self.votes_received = 0
		self.vote_request_threads = []
		self.majority = ((len(self.peers) + 1) // 2) + 1

		#load from state file if it exists
		logfolder = "state"

		filepath_check = os.path.join(logfolder, f"{self.node_id}.state")
		if os.path.exists(filepath_check):
			self.restore_state()

		#flask server
		self.flask_ports=ports
		self.target_port = ports[node_id]
		self.app=Flask(__name__)
		self.app.route('/submit', methods=['POST'])(self.submit)


		# Messaging context
		self.context = zmq.Context()
		self.listen_socket = self.context.socket(zmq.REP)
		self.listen_socket.bind("tcp://{}".format(self.node_address))
		self.poller = zmq.Poller()
		self.poller.register(self.listen_socket, zmq.POLLIN)


		# Node startup log
		logger.info("Node {}, initialized as {}".format(self.node_id, self.state))
		logger.info(f"{self.__dict__}")
	def start(self):


		#flask server thread for client connection
		server_thread = threading.Thread(target=self.run_server,args=(self.target_port,))
		server_thread.start()

		# Start the node listening thread
		logger.info("Node {}, starting async listen channel".format(self.node_id))
		self.listen_thread = threading.Thread(target=self.async_listen)
		self.listen_thread.start()

		# Init the timeout, Node starting out as a follower
		self.set_randomized_timeout()

		# Start the timeout thread
		logger.info("Node {}, starting async timeout thread".format(self.node_id))
		self.timeout_thread = threading.Thread(target=self.async_timeout_thread)
		self.timeout_thread.start()

		
	def submit(self):
		command_recvd = request.form.to_dict()
		data = command_recvd['data']
		ind=int(command_recvd["index"])
		logger.info(f"Data recieved from client: {command_recvd}")
		if(self.state=="LEADER"):
			if ind==len(self.log):
				logger.info(f"Data: {data} received in leader id {self.node_id}")
				self.log.append({"term": self.term,"command":data})
				self.acked_length[self.node_id]=len(self.log)
				self.sent_length[self.node_id]=len(self.log)
				self.last_log_index=len(self.log)
				self.last_log_term=self.log[-1]['term']
				logger.info(f"Leader Log : \n {self.log}")

		else:
			logger.info(f"Data: {data} received in follower id {self.node_id}")
			if(self.leader_id!=-1):
				port=self.flask_ports[self.leader_id]
				try:
					url = f'http://localhost:{port}/submit'
				except:
					logger.info("Leader not available")
				# data_leader = {'data': data}
				response = requests.post(url, data=command_recvd)
			else:
				logger.info("Leader not elected yet")
		return 'Data received'


	def run_server(self,target_port):
		self.app.run(debug=False, port=target_port)

	def async_listen(self):
		while True:
			# Wait for a message
			socks = dict(self.poller.poll())
			if self.listen_socket in socks and socks[self.listen_socket] == zmq.POLLIN:
				message = self.listen_socket.recv_pyobj()
				
				# Recived a append entry message
				if isinstance(message, AppendEntryArgs):
					logger.info("Node {}, received append entry/heartbeat".format(self.node_id))
					
					reply = self.log_check(message)
					self.listen_socket.send_pyobj(reply)

				# Received a vote request
				if isinstance(message, RequestVoteArgs):
					peer_id = message.candidate_id
					logger.info("Node {}, received vote request from {} ->\n {}".format(self.node_id, peer_id,message.__dict__))

					reply = self.vote(message)
					self.listen_socket.send_pyobj(reply)

	def async_timeout_thread(self):
		# Only followers will need to wait for heartbeat, they check for timeout and start election
		while self.state != "LEADER":
			cur_time = time.time()
			delta = self.timeout - cur_time
			if delta <= 0:
				# Transition to follower before starting election.
				self.state = "FOLLOWER"
				self.reset_election_params()
				
				# Timeout, Set a new timeout and start the election.
				self.set_randomized_timeout()
				self.start_election()

				# Wait for the remaining time
				cur_time = time.time()
				delta = self.timeout - cur_time
				if delta > 0:
					time.sleep(delta)
			else:
				# Wait for the remaining time
				time.sleep(delta)
		self.start_heartbeat()
    
	def handle_reponse(self, message):
		# Response to vote request
		if isinstance(message, RequestVoteReply):
			logger.info("Node {}, received vote reply from{}".format(self.node_id,message.voter_id))
			# Received a positive vote, increment vote count
			logger.info("VoteReply {self.state,message.vote_granted,message.term,self.term}")
			logger.info(f"VoteReply {self.state,message.vote_granted,message.term,self.term}")
			if self.state=="CANDIDATE" and message.vote_granted and message.term==self.term:
				self.votes_received += 1
			# Received majority, covert to leader, reset election stats, start sending heartbeat to all followers.
				if self.votes_received >= self.majority:
					logger.info("Node {}, received majority, moving to LEADER".format(self.node_id))
					self.state = "LEADER"
					for peer in self.peers:
						self.sent_length[peer]=len(self.log)
					# self.sent_length[]=
					self.reset_election_params()
					self.start_heartbeat()
			elif message.term > self.term:
				self.term=message.term
				self.state="FOLLOWER"
				self.voted_for=None
				self.set_randomized_timeout()

		# Response to append entry
		if isinstance(message, AppendEntryReply):
			logger.info("Node {}, Append entry reply".format(self.node_id))
			
			logger.info(f"terms->{message.term, self.term}")

			if message.term == self.term and self.state=="LEADER":
				logging.info(f"Processing ack from {message.follower_id}")
				self.process_ack(message.follower_id, message.term,message.acked_len, message.success)

			if message.term > self.term:
				logger.info(f"Higher Term received stepping down")
				self.term = message.term
				if self.state == "CANDIDATE":
					self.state = "FOLLOWER"
				self.voted_for=None
			

	

	def send_message(self, target_node_id, message, timeout): 
		# Send message and wait for reply
		socket = self.context.socket(zmq.REQ)
		socket.setsockopt(zmq.LINGER, 0)
		peer_address = self.node_addresses[target_node_id]
		socket.connect("tcp://{}".format(peer_address))

		cur_time = time.time()
		delta = timeout - cur_time
		if delta > 0:
			try:
				socket.send_pyobj(message)
			except:
				logger.error("Node {}, failed to send message".format(self.node_id))
			else:
				cur_time = time.time()
				delta = timeout - cur_time
				if delta > 0:
					socket.setsockopt(zmq.RCVTIMEO, int(delta * 1000))
					try:
						reply = socket.recv_pyobj()
						self.handle_reponse(reply)
					except Exception as e:
						logger.error(f"Node {self.node_id}, failed to get a response for message sent\nError:{e}")
				else:
					logger.error("Node {}, timeout, failed to receive reply".format(self.node_id))
		else:
			logger.error("Node {}, timeout, failed to send message".format(self.node_id))

		socket.close()

	def start_election(self):
		logger.info("Node {}, starting election".format(self.node_id))

		# Transition to candidate state
		self.state = "CANDIDATE"
		logger.info("Node {}, switched to CANDIDATE".format(self.node_id))

		# Increment term
		self.term += 1

		# Vote for himself
		self.votes_received = 1
		self.voted_for = self.node_id
		
		# Request for votes from peers
		sending_threads = []
		for node_id in self.peers:
			vote_request = RequestVoteArgs(self.term, self.node_id, self.last_log_index, self.last_log_term)
			thread = threading.Thread(target=self.send_message, args=(node_id, vote_request, self.timeout))
			sending_threads.append(thread)
			thread.start()
		for thread in sending_threads:
			thread.join()

	def start_heartbeat(self):
		# Start thread for each follower to send heartbeat in a loop
		peer_threads = []
		for node_id in self.peers:
			peer_heartbeat_thread = threading.Thread(target=self.async_heartbeat, args=(node_id,))
			peer_threads.append(peer_heartbeat_thread)
			peer_heartbeat_thread.start()

		for thread in peer_threads:
			thread.join()

	def async_heartbeat(self, target_node_id):
		# A thread that sends heartbeat to all followers.
		while self.state == "LEADER":
			logger.info("Node {}, sending heartbeat to {}".format(self.node_id, target_node_id))
			start = time.time()
			if len(self.log)>0:
				prev_log_index=self.sent_length[target_node_id]
				entries=self.log[prev_log_index:]
				prev_log_term=0
				# print(self.log,prev_log_index)
				if(prev_log_index>=0):
					prev_log_term=self.log[prev_log_index-1]["term"]
				message = AppendEntryArgs(self.term, self.node_id, entries, prev_log_index, prev_log_term, self.commit_index,self.sent_length,self.acked_length)
				logger.info(f"Sending append entry msg to {target_node_id}")
				logger.info("{self.term, self.node_id, entries, prev_log_index, prev_log_term, self.commit_index}")
				logger.info(f"sending append entry msg to {target_node_id} :{self.term, self.node_id, entries, prev_log_index, prev_log_term, self.commit_index}")
			else :
				#Empty append entry as heartbeat
				message = AppendEntryArgs(self.term, self.node_id, [], 0, 0, self.commit_index,self.sent_length,self.acked_length)
				logger.info(f"Sending empty append entry heartbeat to {target_node_id}")
			timeout = start + (config['heartbeat_delay'] / 1000)
			self.send_message(target_node_id, message, timeout)
			delta = time.time() - start
			if delta > 0:
				time.sleep((config['heartbeat_delay']- delta) / 1000)
		# socket.close()



	def log_check(self, message):
		# Reset timer, because leader is alive, append entry send only by leader.
		self.set_randomized_timeout()
		self.reset_election_params()
		logger.info("Message Entries:{message.prev_log_index,message.leader_commit,message.entries}")
		logger.info(f"Node id {self.node_id} ->Message Entries:{message.term,message.prev_log_index,message.leader_commit,message.entries}")
		logger.info(f"{message.__dict__}")
		# self.sent_length=[max(x1,x2) for x1,x2 in zip(self.sent_length,message.sent_length)]
		# self.acked_length=[max(y1,y2) for y1,y2 in zip(self.acked_length,message.acked_length)]
		if(message.term>=self.term):
			self.term=message.term
			self.voted_for=None
			self.state="FOLLOWER"
			self.leader_id=message.leader_id
	
		# if (message.term==self.term and self.state=="CANDIDATE"):
		# 	self.state="FOLLOWER"
		# 	self.leader_id=message.leader_id
		

		index_consistency = len(self.log)>=message.prev_log_index
		if(len(self.log)==0):
			term_consistency=True
		else:
			term_consistency=(message.prev_log_term==self.log[-1]["term"])
		logOK=(index_consistency)and(term_consistency)
		logger.info("message.term,self.term,logOK,len(self.log),message.prev_log_index")
		logger.info(f"{message.term,self.term,logOK,len(self.log),message.prev_log_index}")


		if(message.term==self.term and logOK):
			# adding entry
			logger.info("LogOK at {self.node_id} \n Message Entries:{message.prev_log_index,message.leader_commit,message.entries}")
			logger.info(f"LogOK at {self.node_id} \n Message Entries:{message.prev_log_index,message.leader_commit,message.entries}")
			self.append_entry(message.prev_log_index,message.leader_commit,message.entries)

			acked_len=message.prev_log_index+len(message.entries)
			reply=AppendEntryReply(self.node_id,self.term,acked_len,True)
			return reply

		if(len(self.log)==0):
			reply=AppendEntryReply(self.node_id,self.term,0,True)
			return reply


		reply = AppendEntryReply(self.node_id,self.term,0, False)
		return reply
	
	

	def append_entry(self,prev_log_index,leader_commit,entries):

		if(len(entries)>0 and len(self.log)>prev_log_index):
			if(prev_log_index!=-1 and self.log[prev_log_index]["term"]!=entries[0]["term"]):
				#log truncation due to inconsistency
				self.log=self.log[:prev_log_index]

				logger.info(f"Log truncated at {self.node_id} : {self.log}")
		
		if(prev_log_index+len(entries)>len(self.log)):
			new_entry_index=len(self.log)-prev_log_index
			self.log+=entries[new_entry_index:]
			self.last_log_index=len(self.log)
			self.last_log_term=self.log[-1]["term"]
			logger.info(f"Log append at {self.node_id} : {self.log}")
		logger.info("{leader_commit,self.commit_index}")
		logger.info(f"{leader_commit,self.commit_index}")

		#commit upto which point leader have committed
		if(leader_commit>self.commit_index):
			logger.info(f"Node {self.node_id} Commit Log: \n {self.log[:leader_commit]}")
			logfolder = "logs"
			if not os.path.exists(logfolder):
				os.makedirs(logfolder)
			filepath = os.path.join(logfolder, f"{self.node_id}.txt")
			logger.info(f"filepath:{filepath}")
			
			with open(filepath, "a") as fl:
				current_commit = self.log[self.commit_index:leader_commit]
				for i in current_commit:
					fl.writelines(f"{i}\n")

			self.commit_index=leader_commit
			self.save_state()
	


	def process_ack(self,follower_id, term,acked_len, success):
		# print()
		logger.info("process ack=>,{follower_id, term,acked_len, success,self.acked_length[follower_id]}")
		logger.info(f"process ack=>,{follower_id, term,acked_len, success,self.acked_length[follower_id]}")


		if(success==True and acked_len>=self.acked_length[follower_id]):
			self.sent_length[follower_id]=acked_len
			self.acked_length[follower_id]=acked_len
			logger.info("Success ack commiting,{len(self.log)}")
			logger.info(f"Success ack commiting,{len(self.log)}")
			if(len(self.log)>0):
				self.commit_log_entries()
		elif self.sent_length[follower_id]>0:
			#trying with a lower index to get the follower in sync
			self.sent_length[follower_id]-=1
			logger.info("Unsuccess ack retrying with,{self.sent_length[follower_id]}")
			logger.info(f"Unsuccess ack retrying with,{self.sent_length[follower_id]}")

			#new message will be sent with the next heartbeat
	

	def commit_log_entries(self):
		logger.info("commit log entries=>")
		max_ready=0
		for i in range(len(self.log),0,-1):
			#leader is always ready at the latest index
			k=1
			logger.info("check max_ready,{i,self.acked_length,self.peers}")
			logger.info(f"check max_ready,{i,self.acked_length,self.peers}")
			for follower_id in self.peers:
				if self.acked_length[follower_id]>=i:
					k+=1
			if(k>=self.majority):
				logger.info("got majority ack,{i,k}")
				logger.info(f"got majority ack,{i,k}")
				max_ready=i
				break
			logger.info("didnt get majority ack")

		logger.info(f"commit log entries<=,{max_ready,self.log,self.commit_index,self.term}")
		logger.info(f"commit log entries<=,{max_ready,self.log,self.commit_index,self.term}")
		
		if max_ready>0 and max_ready>self.commit_index and self.log[max_ready-1]['term']==self.term :
			logger.info(f"Commit Log: Leader ID {self.node_id}:\n{self.log[self.commit_index:max_ready]}")

			logfolder = "logs"
			if not os.path.exists(logfolder):
				os.makedirs(logfolder)
			filepath = os.path.join(logfolder, f"{self.node_id}.txt")
			# logger.info(f"filepath leader:{filepath}")
			# f=open(filepath, "a")
			# f.close()
			with open(filepath, "a") as fl:
				current_commits = self.log[self.commit_index:max_ready]
				for commit in current_commits:
					fl.writelines(f"{commit}\n")
			
			
			self.commit_index=max_ready
			url=f"http://localhost:{config['client_ack_port']}/"
			client_commit_update={"commit_len":self.commit_index}
			logger.info(f"Sending commit ack to:{url} ->{client_commit_update}")
			resp=requests.post(url,json=client_commit_update)
			print(resp)
			self.save_state()




	def vote(self, message):
		# You have a higher term
		logger.info("Voting req in {self.node_id}->{message.term,message.candidate_id, message.last_log_index, message.last_log_term}")
		logger.info(f"Voting req in {self.node_id}->{message.term,message.candidate_id, message.last_log_index, message.last_log_term}")
		logger.info(f"Voting req in {self.node_id}->{self.term,self.node_id, self.last_log_index, self.last_log_term}")
		logger.info(f"{self.node_id} voted for {self.voted_for}")
		
		# if message.term < self.term:
		# 	reply = RequestVoteReply(self.node_id,self.term, False)
		# 	return reply

		# # Vote already granted
		# if self.voted_for != None and self.voted_for != message.candidate_id:
		# 	reply = RequestVoteReply(self.node_id,self.term, False)
		# 	return reply

		# # Your logs are more upto date
		# if message.last_log_term < self.last_log_term:
		# 	reply = RequestVoteReply(self.node_id,self.term, False)
		# 	return reply

		# # Logs are same, therefore compare term
		# if message.last_log_term == self.last_log_term and message.last_log_index < self.last_log_index:
		# 	reply = RequestVoteReply(self.node_id,self.term, False)
		# 	return reply

		log_length_ok=(message.last_log_term>self.last_log_term) or ((message.last_log_term==self.last_log_term) and (message.last_log_index >= self.last_log_index) )
		log_term_ok=(message.term > self.term) or ( (message.term==self.term) and (self.voted_for==None or self.voted_for==message.candidate_id) )

		# Vote for the requesting candidate
		if log_length_ok and log_term_ok:
			self.voted_for = message.candidate_id
			self.term = message.term
			reply = RequestVoteReply(self.node_id,self.term, True)
			logger.info("Node {}, voted for {}".format(self.node_id, self.voted_for))
		else:
			reply = RequestVoteReply(self.node_id,self.term, False)
			logger.info("Node {}, cant vote for {}".format(self.node_id, self.voted_for))



		return reply 

	def set_randomized_timeout(self):
		# Reset the election timeout value
		cur_time = time.time()
		t_low = config['timeout_low']
		t_high = config['timeout_high']
		delta = random.randrange(t_low, t_high) / 1000
		self.timeout = cur_time + delta

	def reset_election_params(self):
		# Reset, granted and received votes 
		self.voted_for = None
		self.votes_received = 0

	def save_state(self):
		logfolder = "state"
		if not os.path.exists(logfolder):
			os.makedirs(logfolder)
		filepath = os.path.join(logfolder, f"{self.node_id}.state")
		class_dict={}
		saved_states=['term','log','commit_index','last_log_index','last_log_term']
		for k,v in self.__dict__.items():
			if k in saved_states:
				class_dict[k]=v
		# not_picklable=['app','context','listen_socket','poller','timeout_thread','listen_thread']
		# for key in not_picklable:
		# 	del class_dict[key]
		logger.info(f"saving state->{class_dict}")
		with open(filepath, 'wb') as f:
			pickle.dump(class_dict, f)

	def restore_state(self):
		logfolder = "state"
		filepath = os.path.join(logfolder, f"{self.node_id}.state")		
		with open(filepath, 'rb') as f:
			state = pickle.load(f)
			self.__dict__.update(state)
		logger.info(f"Server {self.node_id} rebooted.Current Log:{self.log}")
