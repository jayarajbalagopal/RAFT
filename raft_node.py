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
# app = Flask(__name__)

# @app.route('/submit', methods=['POST'])
# def submit():
#     data = request.form.to_dict()['data']
#     print(data)
#     return 'Data received'


# def run_server(target_port):
#     app.run(debug=False, port=target_port)
def l(arg):
	n=[name for name,i in locals().items() if i==arg]
	return n[0]
def f(*args):
	s=""
	for arg in args:
		s+=f" {l(arg)} => {arg} "
	logger.info(s)

class RaftNode:
	def __init__(self, node_id, node_addresses,ports):
		# Address and ID information
		self.node_id = node_id
		self.node_addresses = node_addresses
		self.node_address = self.node_addresses[self.node_id]
		self.node_ids = list(range(len(self.node_addresses)))
		self.peers = self.node_ids[:self.node_id] + self.node_ids[self.node_id+1:]
		
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
		self.sent_length = [-1 for _ in range(len(self.node_addresses))]

		# Election parameters
		self.leader_id = None
		self.voted_for = None
		self.votes_received = 0
		self.vote_request_threads = []
		self.majority = ((len(self.peers) + 1) // 2) + 1

		# Node startup log
		logger.info("Node {}, initialized as {}".format(self.node_id, self.state))
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
		data = request.form.to_dict()['data']
		if(self.state=="LEADER"):
			logger.info(f"Data received in leader id {self.node_id}")
			self.log.append({"term": self.term,"command":data})
			self.acked_length[self.node_id]=len(self.log)
			logger.info(f"Leader Log : \n {self.log}")

		else:
			logger.info(f"Data received in follower id {self.node_id}")
			url = 'http://localhost:2000/submit'
			data_leader = {'data': data}
			response = requests.post(url, data=data_leader)
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
					logger.info("Node {}, received append entry".format(self.node_id))
					reply = self.log_check(message)
					self.listen_socket.send_pyobj(reply)

				# Received a vote request
				if isinstance(message, RequestVoteArgs):
					peer_id = message.candidate_id
					logger.info("Node {}, received vote request from {}".format(self.node_id, peer_id))
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
			logger.info("Node {}, received vote reply".format(self.node_id))
			# Received a positive vote, increment vote count
			if message.vote_granted:
				self.votes_received += 1
			# Received majority, covert to leader, reset election stats, start sending heartbeat to all followers.
			if self.votes_received >= self.majority:
				logger.info("Node {}, received majority, moving to LEADER".format(self.node_id))
				self.state = "LEADER"
				self.reset_election_params()
				self.start_heartbeat()

		# Response to append entry
		if isinstance(message, AppendEntryReply):
			logger.info("Node {}, Append entry reply".format(self.node_id))
			
			logger.info(f"terms->{message.term, self.term}")

			if message.term == self.term and self.state=="LEADER":
				logging.info(f"processing ack from {message.follower_id}")
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
					except:
						logger.error("Node {}, failed to get a response for message sent".format(self.node_id))
				else:
					logger.error("Node {}, timeout, failed to receive reply".format(self.node_id))
		else:
			logger.error("Node {}, timeout, failed to send message".format(self.node_id))

		socket.close()

	def start_election(self):
		logger.info("Node {}, staring election".format(self.node_id))

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
				message = AppendEntryArgs(self.term, self.node_id, entries, prev_log_index, prev_log_term, self.commit_index)
				logger.info(f"sending append entry msg :{self.term, self.node_id, entries, prev_log_index, prev_log_term, self.commit_index}")
			else :
				message = AppendEntryArgs(self.term, self.node_id, [], -1, -1, self.commit_index)
				logger.info(f"sending empty append entry heartbeat ")
			timeout = start + (config['heartbeat_delay'] / 1000)
			self.send_message(target_node_id, message, timeout)
			delta = time.time() - start
			if delta > 0:
				time.sleep((config['heartbeat_delay']- delta) / 1000)
		# socket.close()


	""" NOT COMPLETE """
	def log_check(self, message):
		# Reset timer, because leader is alive, append entry send only by leader.
		self.set_randomized_timeout()
		logger.info(f"Node id {self.node_id} ->Message Entries:{message.prev_log_index,message.leader_commit,message.entries}")
		if(message.term>self.term):
			self.term=message.term
			self.voted_for=None
			self.state="FOLLOWER"
			self.leader_id=message.leader_id
	
		if (message.term==self.term and self.state=="CANDIDATE"):
			self.state="FOLLOWER"
			self.leader_id=message.leader_id
		

		index_consistency = len(self.log)>=message.prev_log_index
		if(len(self.log)==0):
			term_consistency=True
		else:
			print("term consistency check->",message.prev_log_term,self.log[-1])
			print(self.log)
			term_consistency=(message.prev_log_term==self.log[-1]["term"])
		logOK=(index_consistency)and(term_consistency)
		
		print(message.term,self.term,logOK)
		print(len(self.log),message.prev_log_index)


		if(message.term==self.term and logOK):
			# adding entry
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
			new_entry_index=len(self.log)-prev_log_index-1
			self.log+=entries[new_entry_index:]
			logger.info(f"Log append at {self.node_id} : {self.log}")

		if(leader_commit>self.commit_index):
			logger.info(f"Node {self.node_id} Commit Log: \n {self.log[:leader_commit]}")
			self.commit_index=leader_commit
	


	def process_ack(self,follower_id, term,acked_len, success):
		# print()
		f("process ack=>",follower_id, term,acked_len, success,self.acked_length[follower_id])


		if(success==True and acked_len>=self.acked_length[follower_id]):
			self.sent_length[follower_id]=acked_len
			self.acked_length[follower_id]=acked_len
			f("Success ack commiting",len(self.log))
			if(len(self.log)>0):
				self.commit_log_entries()
		elif self.sent_length[follower_id]>0:
			#trying with a lower index to get the follower in sync
			self.sent_length[follower_id]-=1
			f("Unsuccess ack retrying with",self.sent_length[follower_id])

			#new message will be sent with the next heartbeat
	

	def commit_log_entries(self):
		f("commit log entries=>")
		max_ready=0
		for i in range(len(self.log),0,-1):
			k=0
			f("check max_ready",i,self.acked_length,self.peers)
			for follower_id in self.peers:
				if self.acked_length[follower_id]>=i:
					k+=1
			f("follower ack length checked",i,k)
			if(k>=self.majority):
				f("got majority ack",i,k)
				max_ready=i
				break
			f("didnt get majority ack")

		f("commit log entries<=",max_ready,self.log,self.commit_index,self.term)

		if max_ready>0 and max_ready>self.commit_index and self.log[max_ready-1]['term']==self.term :
			logger.info(f"Log commited Leader ID {self.node_id}:\n{self.log[self.commit_index:max_ready]}")
			self.commit_index=max_ready
		f("check5")




	def vote(self, message):
		# You have a higher term
		if message.term < self.term:
			reply = RequestVoteReply(self.term, False)
			return reply

		# Vote already granted
		if self.voted_for != None and self.voted_for != message.candidate_id:
			reply = RequestVoteReply(self.term, False)
			return reply

		# Your logs are more upto date
		if message.last_log_term < self.last_log_term:
			reply = RequestVoteReply(self.term, False)
			return reply

		# Logs are same, therefore compare term
		if message.last_log_term == self.last_log_term and message.last_log_index < self.last_log_index:
			reply = RequestVoteReply(self.term, False)
			return reply

		logger.info("Node {}, voted for {}".format(self.node_id, message.candidate_id))
		# Vote for the requesting candidate
		self.voted_for = message.candidate_id
		self.term = message.term
		reply = RequestVoteReply(self.term, True)
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

