package replica

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"
)

import (
	"distsys/raft/storage"
)

type Replica interface {
	Start(wg *sync.WaitGroup)
	Stop()
	Addr() string
}

func CreateReplica(port int, all []int) Replica {
	var others []string
	for _, v := range all {
		if v != port {
			others = append(others, fmt.Sprintf("http://localhost:%d", v))
		}
	}
	addr := fmt.Sprintf("http://localhost:%d", port)
	log := GetLog(fmt.Sprintf("data/log_%d", port))
	storage := storage.GetStorage(fmt.Sprintf("data/db_%d", port))
	r := replicaImpl{
		srv: nil,

		storage: storage,
		log:     &log,

		pInfo: persistentInfo{Filename: fmt.Sprintf("data/persistent_%d", port), Term: 0, Leader: ""},

		others: others,
		addr:   addr,

		leaderWorker:  InitLeader(addr, others, &log, &storage),
		electionTimer: nil,
	}
	srvMux := http.NewServeMux()
	srvMux.HandleFunc("/append_entries", func(w http.ResponseWriter, req *http.Request) {
		r.handleAppendEntries(w, req)
	})
	srvMux.HandleFunc("/request_vote", func(w http.ResponseWriter, req *http.Request) {
		r.handleRequestVote(w, req)
	})
	srvMux.HandleFunc("/apply", func(w http.ResponseWriter, req *http.Request) {
		r.handleClientRequest(w, req)
	})
	r.srv = &http.Server{Addr: addr[7:], Handler: srvMux}

	r.pInfo.read()

	return &r
}

type replicaImpl struct {
	srv *http.Server

	storage storage.Storage
	log     *Log

	mutex sync.RWMutex

	pInfo persistentInfo

	others []string // urls
	addr   string

	leaderWorker Leader

	electionTimer *time.Timer
}

type persistentInfo struct {
	Filename string
	Term     int
	Leader   string
}

func (info *persistentInfo) read() {
	data, err := os.ReadFile(info.Filename)
	if err != nil {
		if os.IsNotExist(err) {
			info.Leader = ""
			info.Term = 0
			return
		}
		panic(err.Error())
	}
	var parsed persistentInfo
	err = json.Unmarshal(data, &parsed)
	if err != nil {
		panic(err.Error())
	}
	info.Term = parsed.Term
	info.Leader = parsed.Leader
}

func (info persistentInfo) dump() {
	data, err := json.Marshal(info)
	if err != nil {
		panic(err.Error())
	}
	os.WriteFile(info.Filename, data, 0666)
}

func (r *replicaImpl) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.runTimer()
	if r.pInfo.Leader == r.addr {
		r.electionTimer.Stop()
		r.leaderWorker.Start(r.pInfo.Term)
		go func() {
			time.Sleep(time.Second)
			go r.leaderWorker.Sync((*r.log).Length() - 1)
		}()
	}
	go func() {
		err := r.srv.ListenAndServe()
		fmt.Println(err.Error())
		wg.Done()
	}()
}

func (r *replicaImpl) Stop() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.leaderWorker.Stop()
	r.electionTimer.Stop()
	r.srv.Shutdown(context.Background())
}

const minElectionTime = 3 * heartbeatTime

func (r *replicaImpl) runTimer() {
	t := time.Duration(rand.Intn(minElectionTime) + 2*minElectionTime)
	r.electionTimer = time.AfterFunc(t*time.Millisecond, func() {
		r.tryElect()
	})
}

func (r *replicaImpl) resetTimer() {
	r.electionTimer.Stop()
	r.runTimer()
}

func (r *replicaImpl) tryElect() {
	votes := make(chan struct{}, len(r.others))
	votes_number := (len(r.others) + 1) / 2

	refuses := make(chan struct{}, len(r.others))
	refuses_number := len(r.others)/2 + 1

	stop := make(chan struct{}, 2)

	r.mutex.Lock()
	r.pInfo.Term += 1
	r.pInfo.Leader = r.addr
	r.pInfo.dump()
	req := requestVoteRequest{Term: r.pInfo.Term, Candidate: r.addr, LogLength: (*r.log).Length(), LogLastTerm: (*r.log).LastTerm()}
	r.mutex.Unlock()

	data, err := json.Marshal(req)
	if err != nil {
		panic(err.Error())
	}

	for _, url := range r.others {
		go func(url string, data []byte) {
			resp, err := http.Post(url+"/request_vote", "application/json", bytes.NewReader(data))
			if err != nil || resp.StatusCode != http.StatusOK {
				refuses <- struct{}{}
				return
			}
			votes <- struct{}{}
		}(url, data)
	}
	election := make(chan bool, 1)
	go func() {
		for i := 0; i < votes_number; i++ {
			select {
			case <-stop:
				return
			case <-votes:
			}
		}
		election <- true
	}()
	go func() {
		for i := 0; i < refuses_number; i++ {
			select {
			case <-stop:
				return
			case <-refuses:
			}
		}
		election <- false
	}()
	go func() {
		r.mutex.Lock()
		defer r.mutex.Unlock()
		r.electionTimer.Stop()
	}()
	elected := <-election
	stop <- struct{}{}
	r.outputLog("Elected:", elected)
	if elected {
		r.becomeLeader()
	}
}

func (r *replicaImpl) becomeLeader() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.electionTimer.Stop()
	r.leaderWorker.Start(r.pInfo.Term)
}

func (r *replicaImpl) Addr() string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.addr
}

func (r *replicaImpl) outputLog(a ...any) {
	a = append([]any{r.addr[17:] + " -"}, a...)
	fmt.Println(a...)
}
