package replica

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type ClientRequest struct {
	Op    string
	Key   string
	Value string
}

func parseClientRequest(req *http.Request) (*ClientRequest, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	fmt.Println("Got client request:", string(body))
	var data ClientRequest
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}
	return &data, err
}

func (r *replicaImpl) handleClientRead(w http.ResponseWriter, data *ClientRequest) {
	if data.Op != "read" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	val, err := r.storage.Read(data.Key)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Write([]byte(val))
}

func (r *replicaImpl) handleClientRequestAsLeader(w http.ResponseWriter, req *http.Request) {
	data, err := parseClientRequest(req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		r.outputLog("Client request parsing error:", err.Error())
		return
	}
	if req.Method == "GET" {
		r.outputLog("Got GET:", data)
		r.handleClientRead(w, data)
		return
	}

	r.outputLog("Got not GET:", data)

	r.mutex.RLock()
	logEntry := LogEntry{r.pInfo.Term, data.Op, data.Key, data.Value}
	logLength := (*r.log).Length()
	r.mutex.RUnlock()
	(*r.log).Update([]LogEntry{logEntry}, logLength)
	w.WriteHeader(http.StatusAccepted)

	go r.leaderWorker.Sync(logLength)
}

func (r *replicaImpl) handleClientRequestAsFollower(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		if r.pInfo.Leader != "" {
			if r.pInfo.Leader == r.addr {
				time.Sleep(time.Second)
			}
			r.outputLog("Redirect to", r.pInfo.Leader)
			url := fmt.Sprintf("%s/apply", r.pInfo.Leader)
			resp, err := http.Post(url, "application/json", req.Body)
			if err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			data, err := io.ReadAll(resp.Body)
			w.Write(data)
			if err != nil {
				panic(err.Error())
			}
			return
		}
		w.Header().Add("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	data, err := parseClientRequest(req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	r.handleClientRead(w, data)
}

func (r *replicaImpl) handleClientRequest(w http.ResponseWriter, req *http.Request) {
	if r.leaderWorker.Works() {
		r.handleClientRequestAsLeader(w, req)
	} else {
		r.handleClientRequestAsFollower(w, req)
	}
}
