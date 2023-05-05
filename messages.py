import logging
from utils import parse_config


logger = logging.getLogger('raft_logger')
config = parse_config()

class AppendEntryArgs:
    def __init__(self, term, leader_id, entries, prev_log_index, prev_log_term, leader_commit,sent_length,acked_len):
        self.term = term
        self.leader_id = leader_id
        self.entries = entries
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.leader_commit = leader_commit
        self.sent_length=sent_length
        self.acked_length=acked_len

class AppendEntryReply:
    def __init__(self,follower_id, term,acked_len, success):
        self.follower_id=follower_id
        self.term = term
        self.acked_len=acked_len
        self.success = success

class RequestVoteArgs:
    def __init__(self, term, candidate_id, last_log_index, last_log_term):
        self.term = term
        self.candidate_id = candidate_id
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term

class RequestVoteReply:
    def __init__(self,voter_id, term, vote_granted):
        self.voter_id=voter_id
        self.term = term
        self.vote_granted = vote_granted