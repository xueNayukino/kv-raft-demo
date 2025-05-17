package kvstore

import (
	"encoding/json"
	"sync"
)

// 定义操作；类型
const (
	OpPut    = "put"
	OpDelete = "delete"
	OpGet    = "get"
)

// 命令结构体cmd
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// kv存储
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// 创建一个新的KV存储
func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// 定义caozuofnagafa
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock() //读锁，并发
	defer kv.mu.RUnlock()

	val, ok := kv.data[key]
	return val, ok
}
func (kv *KVStore) Put(key string, value string) {
	kv.mu.Lock() //	写锁
	defer kv.mu.Unlock()

	kv.data[key] = value
}
func (kv *KVStore) Delete(key string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.data, key)
}

func (kv *KVStore) Apply(cmd Command) interface{} {
	switch cmd.Op {
	case OpPut:
		kv.Put(cmd.Key, cmd.Value)
		return nil
	case OpDelete:
		kv.Delete(cmd.Key)
		return nil
	case OpGet: //get操作有返回
		val, _ := kv.Get(cmd.Key)
		return val
	default:
		return nil
	}
}

// 序列化
func EncodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}
func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}
