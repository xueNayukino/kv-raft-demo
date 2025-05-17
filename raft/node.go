package raft

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"rrraft/kvstore"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// 节点
type Node struct {
	nodeID      string
	raft        *raft.Raft
	fsm         *kvstore.FSM
	transport   *raft.NetworkTransport //处理节点之间的网络通信，包括心跳、日志条目传输、快照传输等。
	config      *raft.Config
	logStore    raft.LogStore
	stableStore raft.StableStore
	snapStore   raft.SnapshotStore
}

// 创建一个节点
func NewNode(nodeID string, dataDir string, addr string, bootstrap bool) (*Node, error) {
	fsm := kvstore.NewFSM()

	//建目录
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	//将其解析为TCPaddr结构体
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	//node.transport
	transport, err := raft.NewTCPTransport(tcpAddr.String(), tcpAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	//三个存储
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "logs.bolt"))
	if err != nil {
		return nil, err
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "stable.bolt"))
	if err != nil {
		return nil, err
	}
	//快照先建目录
	snapDir := filepath.Join(dataDir, "snapshots")
	if err := os.MkdirAll(snapDir, 0755); err != nil {
		return nil, err
	}
	snapStore, err := raft.NewFileSnapshotStore(snapDir, 3, os.Stdout)
	if err != nil {
		return nil, err
	}
	//配置 config
	config := raft.DefaultConfig() //创建节点默认的配置对象
	config.LocalID = raft.ServerID(nodeID)
	// 增加超时设置，使节点能更快地检测到领导者失效
	config.HeartbeatTimeout = 1000 * time.Millisecond
	config.ElectionTimeout = 1000 * time.Millisecond
	config.LeaderLeaseTimeout = 500 * time.Millisecond
	config.CommitTimeout = 200 * time.Millisecond

	//创建node里的raft字段，也是一个默认的raft节点，包含node里的剩下字段
	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		return nil, err
	}

	//bootstrap判断是否建立集群，是不是引导节点
	if bootstrap {
		log.Printf("引导节点 %s 创建新集群", nodeID)
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(nodeID),
					Address: transport.LocalAddr(),
				},
			},
		}

		// 使用Raft实例的BootstrapCluster方法
		// 这是在已经创建了Raft实例后进行引导
		f := r.BootstrapCluster(configuration)
		if err := f.Error(); err != nil {
			log.Printf("警告: 引导集群失败: %v", err)
		}
	}

	return &Node{nodeID, r, fsm, transport, config, logStore, stableStore, snapStore}, nil
}

// 获取节点ID
func (n *Node) GetNodeID() string {
	return n.nodeID
}

// 获取集群信息
func (n *Node) GetClusterInfo() (map[string]interface{}, error) {
	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return nil, err
	}

	configuration := configFuture.Configuration()

	// 获取节点信息
	servers := make([]map[string]string, 0, len(configuration.Servers))
	for _, server := range configuration.Servers {
		serverInfo := map[string]string{
			"id":       string(server.ID),
			"address":  string(server.Address),
			"suffrage": fmt.Sprintf("%v", server.Suffrage),
		}
		servers = append(servers, serverInfo)
	}

	// 集群状态
	stats := n.raft.Stats()

	// 将统计信息添加到结果中
	result := map[string]interface{}{
		"servers":        servers,
		"current_term":   stats["term"],
		"last_log_index": stats["last_log_index"],
		"last_log_term":  stats["last_log_term"],
		"commit_index":   stats["commit_index"],
		"applied_index":  stats["applied_index"],
		"fsm_pending":    stats["fsm_pending"],
		"state":          stats["state"],
	}

	return result, nil
}

// Apply提交命令给RAFT
func (n *Node) Apply(cmd kvstore.Command) (interface{}, error) {
	//判断是不是leader节点
	if n.raft.State() != raft.Leader {
		return nil, errors.New("raft is not leader")
	}

	data, err := kvstore.EncodeCommand(cmd)
	if err != nil {
		return nil, err
	}

	future := n.raft.Apply(data, 5*time.Second) // 增加超时时间
	if err := future.Error(); err != nil {
		return nil, err
	}
	return future.Response(), nil
}

func (n *Node) Get(key string) (string, bool) {
	return n.fsm.Get(key)
}

// 添加服务器到集群！！！！！
func (n *Node) AddServer(serverID string, serverAddr string) error {
	log.Printf("添加节点 %s (%s) 到集群", serverID, serverAddr)

	// 检查是否是领导者
	if n.raft.State() != raft.Leader {
		return errors.New("raft is not leader")
	}

	// 使用AddVoter添加节点到集群
	future := n.raft.AddVoter(
		raft.ServerID(serverID),
		raft.ServerAddress(serverAddr),
		0,              // 用0表示最新的日志索引
		30*time.Second, // 增加超时时间
	)

	err := future.Error()
	if err != nil {
		log.Printf("添加节点 %s 失败: %v", serverID, err)
		return err
	}

	log.Printf("成功添加节点 %s 到集群", serverID)
	return nil
}

// 移除节点
func (n *Node) RemoveServer(serverID string) error {
	if n.raft.State() != raft.Leader {
		return errors.New("raft is not leader")
	}

	future := n.raft.RemoveServer(raft.ServerID(serverID), 0, 30*time.Second)
	return future.Error()
}

func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

func (n *Node) Leader() string {
	return string(n.raft.Leader())
}

// 关闭节点
func (n *Node) Shutdown() error {
	log.Println("正在关闭Raft节点...")

	// 如果是领导者，先转让领导权
	if n.IsLeader() {
		log.Println("当前节点是领导者，尝试优雅转让领导权...")
		n.raft.LeadershipTransfer()
		time.Sleep(1 * time.Second) // 给转让一些时间
	}

	// 关闭Raft
	future := n.raft.Shutdown()
	err := future.Error()

	// 关闭存储
	if closer, ok := n.logStore.(interface{ Close() error }); ok {
		closer.Close()
	}
	if closer, ok := n.stableStore.(interface{ Close() error }); ok {
		closer.Close()
	}

	// 关闭传输层
	n.transport.Close()

	if err != nil {
		log.Printf("关闭Raft出错: %v", err)
	} else {
		log.Println("Raft节点已成功关闭")
	}

	return err
}
