package kvstore

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

type FSM struct {
	kv *KVStore
	mu sync.Mutex //huc
}

func NewFSM() *FSM {
	return &FSM{kv: NewKVStore()}
}

func (f *FSM) Get(key string) (string, bool) {
	return f.kv.Get(key)
}

// 应用到状态机
func (f *FSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	if log.Type != raft.LogCommand {
		return nil
	}

	cmd, e := DecodeCommand(log.Data)
	if e != nil {
		return e
	}
	return f.kv.Apply(cmd)
}

// 快照，实现了raft.FSMSnapshot接口
type Snapshot struct {
	data map[string]string
}

func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(s.data)
	if err != nil {
		sink.Cancel()
	}
	return sink.Close()
}
func (s *Snapshot) Release() {} //释放快照资源暂时未编写

// ben
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	clone := make(map[string]string)
	for k, v := range f.kv.data {
		clone[k] = v
	}
	return &Snapshot{data: clone}, nil //当前的数据拷贝到实现了快照的结构体里
}
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data := make(map[string]string)
	err := json.NewDecoder(rc).Decode(&data)
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.kv.data = data
	return nil
}
