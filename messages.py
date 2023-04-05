import logging
from utils import parse_config


logger = logging.getLogger('raft_logger')
config = parse_config()

class AppendEntryArgs:
    def __init__(self, term, leader_id, entries, prev_log_index, prev_log_term, leader_commit):
        self.term = term
        self.leader_id = leader_id
        self.entries = entries
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.leader_commit = leader_commit

class AppendEntryReply:
    def __init__(self, term, success):
        self.term = term
        self.success = success

class RequestVoteArgs:
    def __init__(self, term, candidate_id, last_log_index, last_log_term):
        self.term = term
        self.candidate_id = candidate_id
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term

class RequestVoteReply:
    def __init__(self, term, vote_granted):
        self.term = term
        self.vote_granted = vote_granted