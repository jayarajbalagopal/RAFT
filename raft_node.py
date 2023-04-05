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
	def __init__(self, node_id, node_addresses):
		# Address and ID information
		self.node_id = node_id
		self.node_addresses = node_addresses
		self.node_address = self.node_addresses[self.node_id]
		self.node_ids = list(range(len(self.node_addresses)))
		self.peers = self.node_ids[:self.node_id] + self.node_ids[self.node_id+1:]

		# Messaging context
		self.context = zmq.Context()
		self.socket = self.context.socket(zmq.REP)
		self.socket.bind("tcp://{}".format(self.node_address))
		self.poller = zmq.Poller()
		self.poller.register(self.socket, zmq.POLLIN)

		# Raft variables
		self.state = "FOLLOWER"
		self.term = 0
		self.log = [{'term': 0, 'command': 'None'}]
		self.commit_index = 0
		self.conflict_term = None
		self.conflict_index = None
		self.last_log_index = 0
		self.last_log_term = 0

		# Election parameters
		self.voted_for = -1
		self.votes_received = 0
		self.vote_request_threads = []

		self.majority = ((len(self.peers) + 1) // 2) + 1

		# Node startup log
		logger.info("Node {}, initialized as {}".format(self.node_id, self.state))

	def start(self):
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

	def async_listen(self):
		while True:
			# Wait for a message
			socks = dict(self.poller.poll())
			if self.socket in socks and socks[self.socket] == zmq.POLLIN:
				message = self.socket.recv_pyobj()

				# Recived a append entry message
				if isinstance(message, AppendEntryArgs):
					logger.info("Node {}, received append entry".format(self.node_id))
					reply = self.append_entries(message)
					self.socket.send_pyobj(reply)

				# Received a vote request
				if isinstance(message, RequestVoteArgs):
					peer_id = message.candidate_id
					logger.info("Node {}, received vote request from {}".format(self.node_id, peer_id))
					reply = self.vote(message)
					self.socket.send_pyobj(reply)

	def async_timeout_thread(self):
		# Only followers will need to wait for heartbeat, they check for timeout and start election
		while self.state != "LEADER":
			time.sleep(2)
			cur_time = time.time()
			delta = self.timeout - cur_time
			if delta <= 0:
				# Timeout, Set a new timeout and start the election. Transition to follower before starting election.
				self.set_randomized_timeout()
				self.state = "FOLLOWER"
				self.reset_election_params()
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
			self.async_send_message(node_id, vote_request)

	def async_send_message(self, target_node_id, message):
		socket = self.context.socket(zmq.REQ)
		peer_address = self.node_addresses[target_node_id]
		socket.connect("tcp://{}".format(peer_address))
		try:
			socket.send_pyobj(message, zmq.NOBLOCK)
		except:
			logger.error("Node {}, failed to send message".format(self.node_id))
		else:
			poller = zmq.Poller()
			poller.register(socket, zmq.POLLIN)
			events = dict(poller.poll(50))
			if socket in events and events[socket] == zmq.POLLIN:
				reply = socket.recv_pyobj(zmq.NOBLOCK)
				self.handle_reponse(reply)
			else:
				logger.error("Node {}, failed to receive message".format(self.node_id))
		# sending_thread = threading.Thread(target=self.send_message, args=(target_node_id, message))
		# sending_thread.start()
	
	def send_message(self, target_node_id, message):
		# Send message and wait for reply
		socket = self.context.socket(zmq.REQ)
		peer_address = self.node_addresses[target_node_id]
		socket.connect("tcp://{}".format(peer_address))
		socket.send_pyobj(message)
		reply = socket.recv_pyobj()
		self.handle_reponse(reply)
		socket.close()

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
			if message.term > self.term:
				self.term = message.term
				if self.state == "CANDIDATE":
					self.state = "FOLLOWER"

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
			message = AppendEntryArgs(self.term, self.node_id, None, None, None, self.commit_index)
			self.send_message(target_node_id, message)
			delta = time.time() - start
			time.sleep((config['heartbeat_delay']- delta) / 1000)

	""" NOT COMPLETE """
	def append_entries(self, message):
		# Reset timer, because leader is alive, append entry send only by leader.
		self.set_randomized_timeout()
		reply = AppendEntryReply(self.term, True)
		return reply

	def vote(self, message):
		# You have a higher term
		if message.term < self.term:
			reply = RequestVoteReply(self.term, False)
			return reply

		# Vote already granted
		if self.voted_for != -1 and self.voted_for != message.candidate_id:
			reply = RequestVoteReply(self.term, False)
			return reply

		# Your logs are more upto date
		if message.last_log_term < self.last_log_term:
			reply = RequestVoteReply(self.term, False)
			return reply

		# Logs are same, therefor compare term
		if message.last_log_term == self.last_log_term and message.last_log_index < self.last_log_index:
			reply = RequestVoteReply(self.term, False)
			return reply

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
		self.vote_granted = -1
		self.votes_received = 0

