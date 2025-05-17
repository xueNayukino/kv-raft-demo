package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rrraft/raft"
	"rrraft/server"
)

func main() {

	var (
		nodeID    = flag.String("id", "node1", "节点ID")
		raftAddr  = flag.String("raft-addr", "127.0.0.1:12000", "RAFT协议通信地址")
		httpAddr  = flag.String("http-addr", "127.0.0.1:8000", "HTTP服务地址")
		dataDir   = flag.String("data-dir", "data", "数据目录")
		bootstrap = flag.Bool("bootstrap", false, "是否创建新集群")
		joinAddr  = flag.String("join", "", "加入到集群的地址")
	)

	flag.Parse() // 解析命令行参数

	nodeDataDir := fmt.Sprintf("%s/%s", *dataDir, *nodeID)
	if err := os.MkdirAll(nodeDataDir, 0755); err != nil { //权限0755： 用户：读、写、执行（7 = 111二进制）。 组和其他用户：读、执行（5 = 101二进制）。
		log.Fatalf("无法创建数据目录: %v", err)
	}

	node, err := raft.NewNode(*nodeID, nodeDataDir, *raftAddr, *bootstrap)
	if err != nil {
		log.Fatalf("无法创建RAFT节点: %v", err)
	}

	httpServer := server.NewServer(node)

	// 启动HTTP服务
	go func() {
		log.Printf("启动HTTP服务于 %s", *httpAddr)
		if err := httpServer.Start(*httpAddr); err != nil {
			log.Fatalf("HTTP服务错误: %v", err)
		}
	}()

	// 等待HTTP服务启动
	time.Sleep(1 * time.Second)

	// 如果指定了join参数，则尝试加入集群
	if *joinAddr != "" && !*bootstrap {
		log.Printf("尝试加入集群 %s...", *joinAddr)

		// 构造加入请求
		joinURL := fmt.Sprintf("http://%s/join", *joinAddr)
		joinData := map[string]string{
			"id":   *nodeID,
			"addr": *raftAddr,
		}

		jsonData, err := json.Marshal(joinData)
		if err != nil {
			log.Fatalf("序列化加入请求失败: %v", err)
		}

		// 发送加入请求
		resp, err := http.Post(joinURL, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			log.Printf("警告: 加入集群失败: %v", err)
		} else {
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				log.Printf("警告: 服务器返回非200状态码: %d", resp.StatusCode)
			} else {
				log.Printf("成功加入集群")
			}
		}
	}

	//signal.Notify: 注册接收os.Interrupt（Ctrl+C）和syscall.SIGTERM（终止信号）信号。
	signalCh := make(chan os.Signal, 1) //chan os.Signal: 通道的类型是os.Signal，用于传递操作系统信号。
	//signal.Notify: 用于注册一个或多个信号到指定的通道。
	//signalCh: 这是之前创建的通道，用于接收信号。
	//os.Interrupt: 表示中断信号，通常由用户按下Ctrl+C触发。
	//syscall.SIGTERM: 表示终止信号，通常用于请求程序正常终止。
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	<-signalCh

	if err := node.Shutdown(); err != nil {
		log.Fatalf("RAFT节点关闭错误: %v", err)
	}

	log.Println("服务已关闭") //它会在输出中自动添加时间戳和日志级别（默认为无级别，但可以根据需要配置）。 输出的格式通常是：YYYY/MM/DD HH:MM:SS message。
}
