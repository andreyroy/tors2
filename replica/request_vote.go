package replica

import (
	"encoding/json"
	"io"
	"net/http"
)

type requestVoteRequest struct {
	Term      int
	Candidate string

	LogLength   int
	LogLastTerm int
}

func parseRequestVote(r *http.Request) (*requestVoteRequest, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	var data requestVoteRequest
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}
	return &data, nil
}

func (r *replicaImpl) handleRequestVote(w http.ResponseWriter, req *http.Request) error {
	data, err := parseRequestVote(req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return err
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	// r.outputLog("Got request vote, info: ", data)
	if data.Term < r.pInfo.Term {
		w.WriteHeader(http.StatusNonAuthoritativeInfo)
		return nil
	}
	if data.Term == r.pInfo.Term && r.pInfo.Leader != "" {
		w.WriteHeader(http.StatusNonAuthoritativeInfo)
		return nil
	}
	if data.LogLength < (*r.log).Length() {
		w.WriteHeader(http.StatusNonAuthoritativeInfo)
		return nil
	}
	if data.LogLength == (*r.log).Length() && data.LogLastTerm < (*r.log).LastTerm() {
		w.WriteHeader(http.StatusNonAuthoritativeInfo)
		return nil
	}
	if r.leaderWorker.Works() {
		r.leaderWorker.Stop()
	}
	r.resetTimer()
	r.pInfo.Leader = data.Candidate
	r.pInfo.Term = data.Term
	r.pInfo.dump()
	w.WriteHeader(http.StatusOK)
	return nil
}
