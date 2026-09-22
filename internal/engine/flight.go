package engine

import "sync"

// flight 是同 key 进行中的调用，后到者共享第一个调用的结果。
type flight struct {
	done chan struct{}
	res  interface{}
	err  error
}

// flightGroup 是一个极简 singleflight：同 key 并发调用在进程内串行化，
// 保证同批次并发重复执行只评估、提交、告警一次。
type flightGroup struct {
	mu      sync.Mutex
	flights map[string]*flight
}

func newFlightGroup() *flightGroup {
	return &flightGroup{flights: make(map[string]*flight)}
}

// Do 执行 fn；同 key 的并发调用等待首个调用完成并共享其结果。
// leader=true 仅对首个调用者成立。
func (g *flightGroup) Do(key string, fn func() (interface{}, error)) (interface{}, bool, error) {
	g.mu.Lock()
	if f, ok := g.flights[key]; ok {
		g.mu.Unlock()
		<-f.done
		return f.res, false, f.err
	}
	f := &flight{done: make(chan struct{})}
	g.flights[key] = f
	g.mu.Unlock()

	f.res, f.err = fn()
	close(f.done)

	g.mu.Lock()
	delete(g.flights, key)
	g.mu.Unlock()
	return f.res, true, f.err
}
