package server

import (
	"encoding/json"
	"log"
	"net/http"
	"rrraft/kvstore"
	"rrraft/raft"
)

// HTTP服务，暴露API
type Server struct {
	node *raft.Node
}

func NewServer(n *raft.Node) *Server {
	return &Server{node: n}
}

// 启动函数
func (s *Server) Start(addr string) error {
	http.HandleFunc("/kv", s.handleKV)
	http.HandleFunc("/join", s.handleJoin)
	http.HandleFunc("/status", s.handleStatus)
	return http.ListenAndServe(addr, nil)
}
func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key is required", http.StatusBadRequest)
			return
		}
		val, ok := s.node.Get(key)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Write([]byte(val))
		return
	}

	if r.Method == http.MethodPost {
		var cmd kvstore.Command
		err := json.NewDecoder(r.Body).Decode(&cmd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		//写请求需要提交到RAFT，，所有写操作都通过 Raft 协议复制。
		resp, err := s.node.Apply(cmd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"result":  resp,
		})
		return
	}
	http.Error(w, "unsupported method", http.StatusMethodNotAllowed)
}
func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "unsupported method", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ID   string `json:"id"`
		Addr string `json:"addr"`
	}
	//解析请求体中的JSON数据，填充到结构体里
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("收到加入请求: 节点ID=%s, 地址=%s", req.ID, req.Addr)

	//!!!!!!!!
	if err := s.node.AddServer(req.ID, req.Addr); err != nil {
		log.Printf("添加服务器失败: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	//!!!!!

	log.Printf("节点 %s 已成功加入集群", req.ID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
	})
}

// 返回节点状态
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "unsupported method", http.StatusMethodNotAllowed)
		return
	}

	// 获取节点信息
	nodeID := s.node.GetNodeID()

	// 获取集群状态
	clusterInfo, err := s.node.GetClusterInfo()

	status := map[string]interface{}{
		"node_id":   nodeID,
		"is_leader": s.node.IsLeader(),
		"leader":    s.node.Leader(),
	}

	// 如果成功获取了集群信息，添加到状态中
	if err == nil && clusterInfo != nil {
		status["cluster_info"] = clusterInfo
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}
