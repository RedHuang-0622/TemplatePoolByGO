// compare_test.go — 横向对比：TemplatePoolByGO vs 市面主流 Go 连接池/协程池
//
// 对比对象（均为生产验证的真实库）：
//  1. TemplatePoolByGO (本项目)              — Actor扩缩容 + 无锁等待队列 + 泛型
//  2. silenceper/pool (github.com/silenceper/pool) — 通用连接池，~500 stars
//  3. panjf2000/ants/v2 (github.com/panjf2000/ants/v2) — Go 最主流协程池，~13.6k stars
//  4. Raw chan 基线                            — Go channel 原生原语
package bench_cmp

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pool "github.com/RedHuang-0622/TemplatePoolByGO"
	"github.com/panjf2000/ants/v2"
	sp "github.com/silenceper/pool"
)

// ============================================================================
// 共享类型
// ============================================================================

type cmpConn struct{ id int64 }

var cmpConnID atomic.Int64

// ============================================================================
// silenceper/pool 适配
// ============================================================================

func newSilenceperPool(initialCap, maxCap int, createDelay time.Duration) (sp.Pool, error) {
	return sp.NewChannelPool(&sp.Config{
		InitialCap: initialCap,
		MaxCap:     maxCap,
		MaxIdle:    maxCap,
		Factory: func() (interface{}, error) {
			if createDelay > 0 {
				time.Sleep(createDelay)
			}
			return &cmpConn{id: cmpConnID.Add(1)}, nil
		},
		Close: func(v interface{}) error { return nil },
		Ping:  func(v interface{}) error { return nil },
	})
}

// ============================================================================
// TemplatePoolByGO 适配
// ============================================================================

type cmpConnControl struct {
	createDelay time.Duration
}

func (c *cmpConnControl) Create() (*cmpConn, error) {
	if c.createDelay > 0 {
		time.Sleep(c.createDelay)
	}
	return &cmpConn{id: cmpConnID.Add(1)}, nil
}
func (c *cmpConnControl) Reset(_ *cmpConn) error { return nil }
func (c *cmpConnControl) Close(_ *cmpConn) error { return nil }
func (c *cmpConnControl) Ping(_ *cmpConn) error  { return nil }

// ============================================================================
// Raw chan 基线
// ============================================================================

type rawChanPool struct {
	ch chan *cmpConn
}

func newRawChanPool(size int) *rawChanPool {
	p := &rawChanPool{ch: make(chan *cmpConn, size)}
	for i := 0; i < size; i++ {
		p.ch <- &cmpConn{id: cmpConnID.Add(1)}
	}
	return p
}

func (p *rawChanPool) Get() *cmpConn  { return <-p.ch }
func (p *rawChanPool) Put(c *cmpConn) { p.ch <- c }

// ============================================================================
// SCENARIO 1: 纯调度 Get+Put（零业务逻辑，测池子调度开销）
// ============================================================================

func BenchmarkCompare_PureGetPut_TemplatePool(b *testing.B) {
	cfg := pool.PoolConfig{
		MinSize: 50, MaxSize: 50, IdleBufferFactor: 1.0,
		PingInterval: 0, MonitorInterval: 0,
	}
	p := pool.NewPool(cfg, &cmpConnControl{})
	defer p.Close()
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			res, _ := p.Get(ctx)
			p.Put(res)
		}
	})
}

func BenchmarkCompare_PureGetPut_SilenceperPool(b *testing.B) {
	p, _ := newSilenceperPool(50, 50, 0)
	defer p.Release()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, _ := p.Get()
			p.Put(conn)
		}
	})
}

func BenchmarkCompare_PureGetPut_RawChan(b *testing.B) {
	p := newRawChanPool(50)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn := p.Get()
			p.Put(conn)
		}
	})
}

// ============================================================================
// SCENARIO 2: 高竞争 — 连接不足，大量 goroutine 争抢
// ============================================================================

// HighContention: 50 connections, 500 goroutines (10:1 contention ratio)
// All pools use matching fixed capacity for fair comparison
// Note: silenceper/pool's Get() has no context/timeout support, so we
// use matching initial capacity to avoid permanent blocking

func BenchmarkCompare_HighContention_TemplatePool(b *testing.B) {
	cfg := pool.PoolConfig{
		MinSize: 50, MaxSize: 50, IdleBufferFactor: 1.0,
		PingInterval: 0, MonitorInterval: 0, MaxWaitQueue: 10000,
	}
	p := pool.NewPool(cfg, &cmpConnControl{})
	defer p.Close()
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	b.ResetTimer()
	b.SetParallelism(500)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			res, _ := p.Get(ctx)
			p.Put(res)
		}
	})
}

func BenchmarkCompare_HighContention_SilenceperPool(b *testing.B) {
	p, _ := newSilenceperPool(50, 50, 0)
	defer p.Release()

	b.ResetTimer()
	b.SetParallelism(500)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, _ := p.Get()
			p.Put(conn)
		}
	})
}

func BenchmarkCompare_HighContention_RawChan(b *testing.B) {
	p := newRawChanPool(50)

	b.ResetTimer()
	b.SetParallelism(500)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn := p.Get()
			p.Put(conn)
		}
	})
}

// ============================================================================
// SCENARIO 3: 带创建延迟 — 模拟真实网络建连 + 认证
// ============================================================================

func BenchmarkCompare_WithCreation_TemplatePool(b *testing.B) {
	cfg := pool.PoolConfig{
		MinSize: 50, MaxSize: 50, IdleBufferFactor: 1.0,
		PingInterval: 0, MonitorInterval: 0, MaxRetries: 2,
		RetryInterval: 10 * time.Millisecond,
	}
	p := pool.NewPool(cfg, &cmpConnControl{createDelay: 2 * time.Millisecond})
	defer p.Close()
	time.Sleep(300 * time.Millisecond)

	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			res, _ := p.Get(ctx)
			p.Put(res)
		}
	})
}

func BenchmarkCompare_WithCreation_SilenceperPool(b *testing.B) {
	p, _ := newSilenceperPool(50, 50, 2*time.Millisecond)
	defer p.Release()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, _ := p.Get()
			p.Put(conn)
		}
	})
}

// ============================================================================
// SCENARIO 4: 动态扩容 — 小池子 + 突发流量
// ============================================================================

func BenchmarkCompare_Scaling_TemplatePool(b *testing.B) {
	cfg := pool.PoolConfig{
		MinSize: 5, MaxSize: 500, IdleBufferFactor: 0.5,
		MaxRetries: 2, RetryInterval: 10 * time.Millisecond,
		PingInterval: 0, MonitorInterval: 200 * time.Millisecond,
		MaxWaitQueue: 10000,
	}
	p := pool.NewPool(cfg, &cmpConnControl{createDelay: 2 * time.Millisecond})
	defer p.Close()
	time.Sleep(300 * time.Millisecond)

	ctx := context.Background()
	b.ResetTimer()
	b.SetParallelism(300)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			res, err := p.Get(rctx)
			cancel()
			if err != nil {
				continue
			}
			p.Put(res)
		}
	})
}

func BenchmarkCompare_Scaling_SilenceperPool(b *testing.B) {
	// silenceper/pool 也支持动态扩容（InitialCap=5, MaxCap=500）
	p, _ := newSilenceperPool(5, 500, 2*time.Millisecond)
	defer p.Release()

	b.ResetTimer()
	b.SetParallelism(300)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, _ := p.Get()
			p.Put(conn)
		}
	})
}

// ============================================================================
// SCENARIO 5: 并发扫测 — 观察不同并发度下的性能曲线
// ============================================================================

var concurrencies = []int{10, 50, 100, 200, 500, 1000}

func BenchmarkCompare_ConcurrencySweep(b *testing.B) {
	for _, conc := range concurrencies {
		conc := conc

		// TemplatePoolByGO
		b.Run("TemplatePool/conc="+itoa(conc), func(b *testing.B) {
			cfg := pool.PoolConfig{
				MinSize: 50, MaxSize: 500, IdleBufferFactor: 0.5,
				MaxRetries: 2, RetryInterval: 10 * time.Millisecond,
				PingInterval: 0, MonitorInterval: 0, MaxWaitQueue: 10000,
			}
			p := pool.NewPool(cfg, &cmpConnControl{createDelay: 1 * time.Millisecond})
			defer p.Close()
			time.Sleep(200 * time.Millisecond)

			ctx := context.Background()
			b.ResetTimer()
			b.SetParallelism(conc)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
					res, err := p.Get(rctx)
					cancel()
					if err != nil {
						continue
					}
					p.Put(res)
				}
			})
		})

		// silenceper/pool
		b.Run("SilenceperPool/conc="+itoa(conc), func(b *testing.B) {
			p, _ := newSilenceperPool(50, 500, 0)
			defer p.Release()

			b.ResetTimer()
			b.SetParallelism(conc)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					conn, _ := p.Get()
					p.Put(conn)
				}
			})
		})

		// Raw chan
		b.Run("RawChan/conc="+itoa(conc), func(b *testing.B) {
			p := newRawChanPool(50)

			b.ResetTimer()
			b.SetParallelism(conc)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					conn := p.Get()
					p.Put(conn)
				}
			})
		})
	}
}

// ============================================================================
// SCENARIO 6: 协程池对比 — ants/v2 (Go 最主流协程池，13.6k stars)
// 对比 goroutine 提交开销 vs 连接池 Get/Put 开销
// ============================================================================

func BenchmarkCompare_Goroutine_Ants(b *testing.B) {
	p, _ := ants.NewPool(100, ants.WithPreAlloc(true))
	defer p.Release()

	var wg sync.WaitGroup
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wg.Add(1)
			_ = p.Submit(func() {
				wg.Done()
			})
		}
	})
	wg.Wait()
}

func BenchmarkCompare_Goroutine_GoNative(b *testing.B) {
	var wg sync.WaitGroup
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wg.Add(1)
			go func() {
				wg.Done()
			}()
		}
	})
	wg.Wait()
}

func BenchmarkCompare_Goroutine_ChannelSemaphore(b *testing.B) {
	sem := make(chan struct{}, 100)
	var wg sync.WaitGroup

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer func() { <-sem }()
				wg.Done()
			}()
		}
	})
	wg.Wait()
}

// ============================================================================
// 辅助
// ============================================================================

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}
