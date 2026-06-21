// Package pool 提供通用的泛型连接池实现。
//
// 核心特性：
//   - 泛型设计：支持任意类型的连接资源
//   - 动态伸缩：根据负载自动扩缩容，支持非线性扩容曲线与压力补偿
//   - 等待队列：基于无锁队列的高性能等待机制
//   - 健康检查：内置心跳 Ping 与自动重连
//   - Actor 模型：内部使用 Actor 模式管理扩缩容逻辑，保证并发安全
//
// 快速开始：
//
//	config := pool.DefaultPoolConfig()
//	p := pool.NewPool(config, myConnControl)
//	defer p.Close()
//
//	res, err := p.Get(ctx)
//	// ... 使用 res.Conn ...
//	p.Put(res)
package pool

import "time"

// Resource 资源包装器，封装连接及其元数据。
// 每个 Resource 由连接池统一管理，包含连接生命周期信息与重试计数。
type Resource[T any] struct {
	ID         string
	createTime time.Time
	updateTime time.Time
	Conn       T
	retryCount int // 重连次数
}

// 内部使用 resource 作为别名
type resource[T any] = Resource[T]

// PoolConfig 连接池配置参数。
// 所有字段在调用 NewPool 前设置，运行时不可变。
type PoolConfig struct {
	// MinSize 最小连接数，池初始化时预创建。
	MinSize int64
	// MaxSize 最大连接数上限。
	MaxSize int64
	// SurviveTime 连接最大存活时间，超过后优先被缩容驱逐。
	SurviveTime time.Duration
	// MonitorInterval 扩缩容检查间隔。
	MonitorInterval time.Duration
	// IdleBufferFactor channel 缓冲系数，控制空闲连接的 channel 缓冲区大小。
	// 例如 MaxSize=500, IdleBufferFactor=0.4 → buffer 可容纳 200 个空闲连接。
	IdleBufferFactor float64 // channel 缓冲系数

	// MaxWaitQueue 等待队列最大长度，超出后 Get 立即返回 ErrPoolBusy。
	MaxWaitQueue int64

	// MaxRetries 连接创建/重连时的最大重试次数。
	MaxRetries int
	// RetryInterval 重试间隔。
	RetryInterval time.Duration
	// ReconnectOnGet 是否在 Get 时对失效连接自动重连。
	ReconnectOnGet bool

	// PingInterval 定期 Ping 空闲连接的间隔。
	PingInterval time.Duration
	// OnUnhealthy 当 Ping 检测到不健康连接时的回调钩子。
	OnUnhealthy func(err error)
}

// DefaultPoolConfig 返回带推荐默认值的连接池配置。
// 调用方可在此基础上修改个别字段后传入 NewPool。
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MinSize:          5,
		MaxSize:          100,
		SurviveTime:      30 * time.Minute,
		MonitorInterval:  10 * time.Second,
		IdleBufferFactor: 1.0,
		MaxRetries:       3,
		RetryInterval:    1 * time.Second,
		ReconnectOnGet:   false, // Get 时 Ping 失败是否自动重连（默认关闭，避免热路径开销）
		PingInterval:     30 * time.Second,
		OnUnhealthy:      nil,
		MaxWaitQueue:     10000,
	}
}

// Conn 定义了连接的生命周期管理接口。
// 使用者需要为具体的连接类型（如 gRPC 连接、数据库连接等）实现该接口。
type Conn[T any] interface {
	Reset(T) error
	Close(T) error
	Create() (T, error)
	Ping(T) error
}
