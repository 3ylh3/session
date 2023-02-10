package session

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	StorageServersMu sync.RWMutex
	StorageServers   = make(map[string]StorageServer)
)

const (
	GET_SESSION_ERROR int = 1
	SESSION_NOT_FIND  int = 2
	SESSION_EXPIRED   int = 3
	LOCK_ERROR        int = 4
)

// Session Session结构体,对外暴露操作方法
type Session struct {
	StorageServer StorageServer
	ticker        *time.Ticker
}

// SessionInfo session公共父实体
type SessionInfo struct {
	// JSESSIONID
	JSESSIONID string `json:"JSESSIONID"`
	// 创建时间
	CreateTime string `json:"createTime"`
	// 过期时间
	ExpireTime string `json:"expireTime"`
}

// StorageServer 接口，屏蔽session存储服务端增删改查具体实现
type StorageServer interface {
	// InitServer 根据传入url和密码，初始化存储server，例如初始化etcd client
	InitServer(url string, password string) error
	// Close 关闭server
	Close() error
	// Put 将kv数据存入存储server
	Put(key string, data string) error
	// Get 从存储server中获取指定key的数据
	Get(key string) (string, error)
	// Delete 删除指定key
	Delete(key string) error
	// ListKeysByPrefix 列出指定前缀的key列表
	ListKeysByPrefix(prefix string) ([]string, error)
	// NewLocker 获取指定key的分布式锁
	NewLocker(key string) (sync.Locker, error)
}

type SessionError struct {
	ErrorCode    int
	ErrorMessage string
}

func (s SessionError) Error() string {
	return s.ErrorMessage
}

// Register 注册StorageServer实现类
func Register(name string, server StorageServer) {
	StorageServersMu.Lock()
	defer StorageServersMu.Unlock()
	if server == nil {
		panic("Register storage server is null")
	}
	if _, exist := StorageServers[name]; exist {
		panic("Register called twice for storage server type " + name)
	}
	StorageServers[name] = server
}

// Init 初始化
func Init(name string, url string, password string) *Session {
	StorageServersMu.RLock()
	defer StorageServersMu.RUnlock()
	server, ok := StorageServers[name]
	if !ok {
		panic("unknown storage server " + name + ",forgotten import?")
	}
	err := server.InitServer(url, password)
	if err != nil {
		panic("init error:" + err.Error())
	}
	// 定时每小时检查是否有异常状态的任务
	ticker := time.NewTicker(1 * time.Hour)
	go checkSession(ticker, server)
	// 捕获信号，在异常退出时释放资源
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	s := &Session{
		StorageServer: server,
		ticker:        ticker,
	}
	go release(s, c)
	return s
}

// Create 创建新session
func (s *Session) Create(expire int64) (string, error) {
	// 生成JSESSIONID
	id, err := uuid.NewUUID()
	if err != nil {
		return "", fmt.Errorf("generate JSESSIONID error:%v\n", err)
	}
	// 计算过期时间
	createTime := time.Now()
	expireUnix := createTime.Unix() + expire
	expireTime := time.Unix(expireUnix, 0)
	sessionInfo := &SessionInfo{
		JSESSIONID: id.String(),
		CreateTime: createTime.Format("2006-01-02 15:04:05"),
		ExpireTime: expireTime.Format("2006-01-02 15:04:05"),
	}
	// 解析对象
	bytes, err := json.Marshal(sessionInfo)
	if err != nil {
		return "", err
	}
	err = s.StorageServer.Put("/session/"+id.String(), string(bytes))
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// Get 获取session信息
func (s *Session) Get(JSESSIONID string) (string, error) {
	if JSESSIONID == "" {
		return "", SessionError{
			ErrorCode:    GET_SESSION_ERROR,
			ErrorMessage: "JSESSIONID is null",
		}
	}
	sessionStr, err := s.StorageServer.Get("/session/" + JSESSIONID)
	if err != nil {
		return "", SessionError{
			ErrorCode:    GET_SESSION_ERROR,
			ErrorMessage: err.Error(),
		}
	}
	if sessionStr == "" {
		return "", SessionError{
			ErrorCode:    SESSION_NOT_FIND,
			ErrorMessage: "session not find",
		}
	}
	// 判断session是否过期
	sessionInfo := &SessionInfo{}
	err = json.Unmarshal([]byte(sessionStr), sessionInfo)
	if err != nil {
		return "", SessionError{
			ErrorCode:    GET_SESSION_ERROR,
			ErrorMessage: err.Error(),
		}
	}
	expireTime, err := time.ParseInLocation("2006-01-02 15:04:05", sessionInfo.ExpireTime, time.Local)
	if err != nil {
		return "", SessionError{
			ErrorCode:    GET_SESSION_ERROR,
			ErrorMessage: "parse expire time error",
		}
	}
	if time.Now().Unix() >= expireTime.Unix() {
		// session过期
		// 异步删除session信息
		go s.StorageServer.Delete("/session/" + JSESSIONID)
		return "", SessionError{
			ErrorCode:    SESSION_EXPIRED,
			ErrorMessage: "session is expired",
		}
	}
	return sessionStr, nil
}

// Put 向session中添加内容
func (s *Session) Put(JSESSIONID string, key string, value interface{}) error {
	// 加分布式锁
	lock, err := s.StorageServer.NewLocker("/session/" + JSESSIONID)
	if err != nil {
		return &SessionError{ErrorCode: LOCK_ERROR, ErrorMessage: "lock error:" + err.Error()}
	}
	lock.Lock()
	defer lock.Unlock()
	// 获取session信息
	sessionStr, err := s.Get(JSESSIONID)
	if err != nil {
		return err
	}
	m := make(map[string]interface{})
	err = json.Unmarshal([]byte(sessionStr), &m)
	if err != nil {
		return err
	}
	// 向session中添加内容
	m[key] = value
	result, err := json.Marshal(m)
	if err != nil {
		return err
	}
	// 存入后端存储系统
	err = s.StorageServer.Put("/session/"+JSESSIONID, string(result))
	if err != nil {
		return err
	}
	return nil
}

// Remove 移除session中内容
func (s *Session) Remove(JSESSIONID string, key string) error {
	// 加分布式锁
	lock, err := s.StorageServer.NewLocker("/session/" + JSESSIONID)
	if err != nil {
		return &SessionError{ErrorCode: LOCK_ERROR, ErrorMessage: "lock error:" + err.Error()}
	}
	lock.Lock()
	defer lock.Unlock()
	// 获取session信息
	sessionStr, err := s.Get(JSESSIONID)
	if err != nil {
		return err
	}
	m := make(map[string]interface{})
	err = json.Unmarshal([]byte(sessionStr), &m)
	if err != nil {
		return err
	}
	// 删除session中内容
	delete(m, key)
	result, err := json.Marshal(m)
	if err != nil {
		return err
	}
	// 存入后端存储系统
	err = s.StorageServer.Put("/session/"+JSESSIONID, string(result))
	if err != nil {
		return err
	}
	return nil
}

// Delete 删除session
func (s *Session) Delete(JSESSIONID string) error {
	lock, err := s.StorageServer.NewLocker("/session/" + JSESSIONID)
	if err != nil {
		return &SessionError{ErrorCode: LOCK_ERROR, ErrorMessage: "lock error:" + err.Error()}
	}
	lock.Lock()
	defer lock.Unlock()
	return s.StorageServer.Delete("/session/" + JSESSIONID)
}

// Close 关闭
func (s *Session) Close() error {
	// 停止定时ticker
	s.ticker.Stop()
	// 关闭storage server
	err := s.StorageServer.Close()
	if err != nil {
		return err
	}
	return nil
}

// 定时检查
func checkSession(ticker *time.Ticker, server StorageServer) {
	for {
		<-ticker.C
		doCheckSession(server)
	}
}

// 检查session是否过期
func doCheckSession(server StorageServer) {
	keys, err := server.ListKeysByPrefix("/session/")
	if err != nil {
		return
	}
	for _, key := range keys {
		// 获取session信息
		sessionStr, err := server.Get(key)
		if err != nil {
			continue
		}
		// 判断session是否过期
		sessionInfo := &SessionInfo{}
		err = json.Unmarshal([]byte(sessionStr), sessionInfo)
		if err != nil {
			continue
		}
		expireTime, err := time.ParseInLocation("2006-01-02 15:04:05", sessionInfo.ExpireTime, time.Local)
		if err != nil {
			continue
		}
		if time.Now().Unix() >= expireTime.Unix() {
			func() {
				// session过期
				// 加分布式锁
				lock, _ := server.NewLocker(key)
				lock.Lock()
				defer lock.Unlock()
				// 删除session信息
				_ = server.Delete(key)
			}()
		}
	}
}

// 捕获异常信号，清理资源
func release(s *Session, c chan os.Signal) {
	<-c
	_ = s.Close()
	os.Exit(1)
}
