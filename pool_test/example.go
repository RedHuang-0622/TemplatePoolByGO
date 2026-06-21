// Package pool_test 提供连接池的外部测试示例与可执行文档。
//
// 包含：
//   - ExampleConn / ExampleConnControl：Conn[T] 接口的示例实现
//   - ExampleGet / ExamplePut / ExampleStats：Go 可测试示例函数
package pool_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	pool "github.com/RedHuang-0622/TemplatePoolByGO"
)

var poolConfig = pool.PoolConfig{
	MinSize:          5,
	MaxSize:          100,
	SurviveTime:      30 * time.Minute,
	MonitorInterval:  10 * time.Second,
	IdleBufferFactor: 0.4,
	MaxRetries:       3,
	RetryInterval:    1 * time.Second,
	ReconnectOnGet:   true,
	PingInterval:     30 * time.Second,
	OnUnhealthy: func(err error) {
		fmt.Printf("[Unhealthy] %v\n", err)
	},
	MaxWaitQueue: 10000,
}

// ================= ExampleConn 定义 ================= //

// ExampleConn 示例连接类型，包含一个原子计数器用于验证连接状态。
type ExampleConn struct {
	Num atomic.Int64
}

func (e *ExampleConn) reset() error {
	e.Num.Store(0)
	return nil
}

func (e *ExampleConn) ping() error {
	e.Num.Add(1)
	return nil
}

// ================= ExampleConnControl 定义（实现 Conn[T] 接口）================= //

// ExampleConnControl 实现 pool.Conn[*ExampleConn] 接口的示例控制器。
type ExampleConnControl struct{}

// Create 创建一个新的 ExampleConn 连接。
func (ecc *ExampleConnControl) Create() (*ExampleConn, error) {
	return &ExampleConn{}, nil
}

// Close 关闭 ExampleConn 连接（空操作示例）。
func (ecc *ExampleConnControl) Close(_ *ExampleConn) error {
	return nil
}

// Reset 重置 ExampleConn 的内部计数器。
func (ecc *ExampleConnControl) Reset(c *ExampleConn) error {
	return c.reset()
}

// Ping 通过递增计数器验证连接存活。
func (ecc *ExampleConnControl) Ping(c *ExampleConn) error {
	return c.ping()
}

// ================= 使用示例 ================= //

func ExampleGet() {
	p := pool.NewPool(poolConfig, &ExampleConnControl{})
	defer p.Close()

	conn, err := p.Get(context.Background())
	if err != nil {
		fmt.Printf("获取连接失败: %v\n", err)
		return
	}
	fmt.Printf("获取连接成功, Num=%d\n", conn.Conn.Num.Load())
}

func ExamplePut() {
	p := pool.NewPool(poolConfig, &ExampleConnControl{})
	defer p.Close()

	conn, err := p.Get(context.Background())
	if err != nil {
		fmt.Printf("获取连接失败: %v\n", err)
		return
	}
	fmt.Printf("获取连接成功, Num=%d\n", conn.Conn.Num.Load())

	if err := p.Put(conn); err != nil {
		fmt.Printf("归还连接失败: %v\n", err)
		return
	}
	fmt.Println("归还连接成功")
}

func ExampleStats() {
	p := pool.NewPool(poolConfig, &ExampleConnControl{})
	defer p.Close()

	conn, err := p.Get(context.Background())
	if err != nil {
		fmt.Printf("获取连接失败: %v\n", err)
		return
	}
	defer p.Put(conn)

	stats, err := p.Stats(context.Background())
	if err != nil {
		fmt.Printf("获取统计信息失败: %v\n", err)
		return
	}
	fmt.Printf("total=%d available=%d in_use=%d waiting=%d\n",
		stats["total_size"], stats["pool_available"],
		stats["pool_in_use"], stats["waiting_count"])
}