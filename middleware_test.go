package bus

import (
	"context"
	"sync"
	"testing"
)

func middleware1() Middleware {
	return func(next HandlerFunc) HandlerFunc {
		fn := func(m *Message) (interface{}, error) {
			m.Context = context.WithValue(m.Context, "middleware1", "1")
			return next(m)
		}
		return HandlerFunc(fn)
	}
}

func middleware2() Middleware {
	return func(next HandlerFunc) HandlerFunc {
		fn := func(m *Message) (interface{}, error) {
			m.Context = context.WithValue(m.Context, "middleware2", "2")
			return next(m)
		}
		return HandlerFunc(fn)
	}
}

func TestMiddleware(t *testing.T) {
	type event struct{ Name string }

	emitter, err := NewEmitter(EmitterConfig{})
	if err != nil {
		t.Errorf("Expected to initialize emitter %s", err)
	}

	e := event{"event"}
	if err := emitter.Emit("test.create", &e); err != nil {
		t.Errorf("Expected to emit message %s", err)
	}

	s := NewService()
	s.SetEnvironment("development")
	var wg sync.WaitGroup
	wg.Add(1)

	el := NewConsumer("test.create", "testChannel", func(message *Message) (reply interface{}, err error) {
		if val := message.Context.Value("middleware1"); val != "1" {
			t.Errorf("Expected middleware1 value to be set as 1 got %s", val)
		}

		if val := message.Context.Value("middleware2"); val != "2" {
			t.Errorf("Expected middleware2 value to be set as 2 got %s", val)
		}
		message.Finish()
		wg.Done()
		return
	})
	el.AddMiddleware(middleware1())
	el.AddMiddleware(middleware2())

	s.AddListener(el)
	go func() {
		if err := s.Start(); err != nil {
			t.Errorf("Expected to start server to not fail %s", err)
		}
	}()

	wg.Wait()

	s.Stop()
}
