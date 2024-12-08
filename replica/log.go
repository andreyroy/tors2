package replica

import (
	"distsys/raft/storage"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

type LogEntry struct {
	Term  int
	Op    string
	Key   string
	Value string
}

func apply(l *LogEntry, s *storage.Storage) error {
	switch l.Op {
	case "create":
		(*s).Create(l.Key, l.Value)
	case "update":
		(*s).Update(l.Key, l.Value)
	case "delete":
		(*s).Delete(l.Key)
	default:
		return errors.New("unknown operation type")
	}
	return nil
}

type Log interface {
	Length() int
	LastTerm() int
	Applied() int

	At(index int) *LogEntry
	TermAt(index int) int
	Since(index int) []LogEntry

	Update(newLog []LogEntry, start int)

	Apply(s *storage.Storage, newApplied int)
}

func GetLog(filename string) Log {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		data := []LogEntry{}
		content, err := json.Marshal(data)
		if err != nil {
			panic(err.Error())
		}
		os.WriteFile(filename, content, 0666)
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	var logEntries []LogEntry
	err = json.Unmarshal(data, &logEntries)
	if err != nil {
		panic(err)
	}
	return &logImpl{filename, sync.RWMutex{}, 0, logEntries}
}

type logImpl struct {
	filename string
	mutex    sync.RWMutex
	applied  int
	log      []LogEntry
}

func (log *logImpl) At(index int) *LogEntry {
	if index < 0 {
		return nil
	}
	log.mutex.RLock()
	defer log.mutex.RUnlock()
	return &log.log[index]
}

func (log *logImpl) TermAt(index int) int {
	log.mutex.RLock()
	defer log.mutex.RUnlock()
	if index < 0 {
		return -1
	}
	return log.log[index].Term
}

func (log *logImpl) Since(index int) []LogEntry {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	return log.log[index:]
}

func (log *logImpl) Length() int {
	log.mutex.RLock()
	defer log.mutex.RUnlock()
	return len(log.log)
}

func (log *logImpl) LastTerm() int {
	log.mutex.RLock()
	defer log.mutex.RUnlock()
	if len(log.log) == 0 {
		return -1
	}
	return log.log[len(log.log)-1].Term
}

func (log *logImpl) Applied() int {
	log.mutex.RLock()
	defer log.mutex.RUnlock()
	return log.applied
}

func (log *logImpl) Update(newLog []LogEntry, start int) {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	if len(log.log) < start {
		panic("Can't add")
	}
	log.log = log.log[:start]
	log.log = append(log.log, newLog...)
	data, err := json.Marshal(log.log)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile(log.filename, data, 0666)
	if err != nil {
		panic(err)
	}
}

func (log *logImpl) Apply(s *storage.Storage, newApplied int) {
	log.mutex.RLock()
	defer log.mutex.RUnlock()
	if newApplied < log.applied {
		fmt.Println("Warning: Already applied")
	}
	if newApplied > len(log.log) {
		panic("Can't apply")
	}
	go func() {
		log.mutex.Lock()
		defer log.mutex.Unlock()
		for ; log.applied < newApplied; log.applied++ {
			apply(&log.log[log.applied], s)
		}
	}()
}
