package replica

import (
	"encoding/json"
	"io"
	"net/http"
)

type appendEntriesRequest struct {
	Leader string

	Term    int
	Entries []LogEntry

	PrevIndex int
	PrevTerm  int

	Applied int
}

func (r *replicaImpl) handleAppendEntries(w http.ResponseWriter, req *http.Request) error {
	data, err := parseAppendEntries(req)
	if err != nil {
		panic(err.Error())
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if data.Term < r.pInfo.Term {
		w.WriteHeader(http.StatusNonAuthoritativeInfo)
		return nil
	}
	if data.Term > r.pInfo.Term {
		r.leaderWorker.Stop()
		r.pInfo.Term = data.Term
		r.pInfo.Leader = data.Leader
		r.pInfo.dump()
	}
	r.resetTimer()
	if data.PrevIndex >= (*r.log).Length() || data.PrevTerm != (*r.log).TermAt(data.PrevIndex) {
		r.outputLog("leader prev index -", data.PrevIndex, "local log length -", (*r.log).Length())
		w.WriteHeader(http.StatusNotFound)
		return nil
	}
	(*r.log).Update(data.Entries, data.PrevIndex+1)
	(*r.log).Apply(&r.storage, data.Applied)
	w.WriteHeader(http.StatusOK)
	return nil
}

func parseAppendEntries(r *http.Request) (*appendEntriesRequest, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	var data appendEntriesRequest
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}
	return &data, nil
}