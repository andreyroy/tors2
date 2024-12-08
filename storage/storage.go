package storage

import (
	"encoding/json"
	"errors"
	"os"
)

type Storage interface {
	Create(key string, value string)
	Read(key string) (string, error)
	Update(key string, value string)
	Delete(key string)
	Get() map[string]string
}

func GetStorage(filename string) Storage {
	data := map[string]string{}
	content, err := json.Marshal(data)
	if err != nil {
		panic(err.Error())
	}
	os.WriteFile(filename, content, 0666)
	return storageImpl{filename, data}
}

type storageImpl struct {
	Filename string
	Data     map[string]string
}

func (s storageImpl) Get() map[string]string {
	return s.Data
}

func (s storageImpl) Dump() {
	result, err := json.Marshal(s.Data)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile(s.Filename, result, 0666)
	if err != nil {
		panic(err)
	}
}

func (s storageImpl) Create(key string, value string) {
	s.Data[key] = value
	s.Dump()
}

func (s storageImpl) Read(key string) (string, error) {
	val, ok := s.Data[key]
	if !ok {
		return "", errors.New("key not found")
	} else {
		return val, nil
	}
}

func (s storageImpl) Update(key string, value string) {
	s.Data[key] = value
	s.Dump()
}

func (s storageImpl) Delete(key string) {
	delete(s.Data, key)
	s.Dump()
}