package replica

import (
	"bytes"
	"distsys/raft/storage"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Leader interface {
	Start(term int)
	Sync(index int)
	Works() bool
	Stop()
}

const heartbeatTime = 500 // seconds

func InitLeader(addr string, others []string, log *Log, storage *storage.Storage) Leader {
	return &leaderImpl{
		log:     log,
		storage: storage,

		addr:   addr,
		term:   0,
		others: others,

		heartbeat:      nil,
		stopHeartbeats: make(chan bool),
		sync:           nil,
		stopSync:       make(chan bool),
	}
}

const retryTime = 1 * time.Second

type syncInfo struct {
	index int
	sync  chan<- bool
}

type leaderImpl struct {
	log     *Log
	storage *storage.Storage

	addr   string
	term   int
	others []string

	works bool
	mutex sync.RWMutex

	heartbeat      *time.Ticker
	stopHeartbeats chan bool

	sync     chan syncInfo
	stopSync chan bool
}

func (l *leaderImpl) Start(term int) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.term = term
	l.sync = make(chan syncInfo)
	l.heartbeat = time.NewTicker(heartbeatTime * time.Millisecond)

	l.startSendHeartBeats() // goroutine
	l.handleSyncs() // goroutine

	l.works = true
}

func (l *leaderImpl) Stop() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.works {
		l.works = false

		l.stopHeartbeats <- true
		l.heartbeat.Stop()
		l.stopSync <- true
		close(l.sync)
	}
}

func (l *leaderImpl) Works() bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.works
}

func (l *leaderImpl) Sync(index int) {
	l.mutex.RLock()
	if !l.works {
		l.mutex.RUnlock()
		return
	}
	synced := make(chan bool, 1) 
	l.sync <- syncInfo{index, synced}
	l.mutex.RUnlock()
	if <-synced {
		(*l.log).Apply(l.storage, index+1)
	}
}

func (l *leaderImpl) startSendHeartBeats() {
	go func() {
		for {
			select {
			case <-l.stopHeartbeats:
				return
			case <-l.heartbeat.C:
			}
			l.sendHeartBeats()
		}
	}()
}

func (l *leaderImpl) sendHeartBeats() {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	fmt.Println(l.addr[17:], "- heartbeats, term", l.term) // getting port
	req := appendEntriesRequest{Leader: l.addr, Term: l.term, Entries: []LogEntry{}, PrevIndex: (*l.log).Length() - 1, PrevTerm: (*l.log).LastTerm(), Applied: (*l.log).Applied()} // empty_entries for heartbeat
	data, err := json.Marshal(req)
	if err != nil {
		panic(err.Error())
	}
	for _, url := range l.others {
		go func(url string, data []byte) {
			resp, err := http.Post(fmt.Sprintf("%s/append_entries", url), "application/json", bytes.NewReader(data))
			if err != nil {
				return
			}
			if resp.StatusCode == http.StatusNotFound && (*l.log).Length() > 0 {
				go l.Sync((*l.log).Length() - 1)
			}
		}(url, data)
	}
}

func (l *leaderImpl) handleSyncs() {
	go func() {
		for {
			select {
			case <-l.stopSync:
				return
			case info := <-l.sync:
				result := make(chan bool, len(l.others))
				for _, url := range l.others {
					go func(url string) {
						result <- l.updateReplica(fmt.Sprintf("%s/append_entries", url), info.index)
					}(url)
				}
				success := 0
				threshold := len(l.others) / 2
				for range l.others {
					if <-result {
						success++
					}
					if success >= threshold {
						break
					}
				}
				info.sync <- (success >= threshold)
			}
		}
	}()
}

func (l *leaderImpl) updateReplica(url string, index int) bool {
	for i := index; i >= 0; i-- {
		updated, stillLeader := l.sendUpdateLog(url, i)
		if !stillLeader {
			l.Stop()
			return false
		}
		if updated {
			break
		}
	}
	return true
}

func (l *leaderImpl) sendUpdateLog(url string, since int) (bool, bool) {
	entries := (*l.log).Since(since)
	req := appendEntriesRequest{Leader: l.addr, Term: l.term, Entries: entries, PrevIndex: since - 1, PrevTerm: (*l.log).TermAt(since - 1), Applied: (*l.log).Applied()}
	data, err := json.Marshal(req)
	if err != nil {
		panic(err.Error())
	}
	var resp *http.Response
	for {
		resp, err = http.Post(url, "application/json", bytes.NewReader(data))
		if err == nil {
			break
		}
		time.Sleep(retryTime)
	}
	if resp.StatusCode == http.StatusNonAuthoritativeInfo {
		return false, false
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		panic("unknown status code")
	}
	return resp.StatusCode == http.StatusOK, true
}
