package bus

import (
	"errors"
	"sync"

	"github.com/kr/pretty"
	nsq "github.com/nsqio/go-nsq"
)

var (
	ErrNoEventListeners      = errors.New("At least one eventListener is required")
	ErrServiceAlreadyRunning = errors.New("Service is already running")
)

type Service struct {
	eventListeners []*EventListener
	consumers      []*nsq.Consumer
	environment    string
	nsqds          []string
	nsqlookupds    []string
}

func (s *Service) AddListener(el *EventListener) {
	s.eventListeners = append(s.eventListeners, el)
}

func (s *Service) AddNSQD(nsqd string) {
	s.nsqds = append(s.nsqds, nsqd)
}

func (s *Service) AddNSQLookupd(nsqLookupd string) {
	pretty.Println(nsqLookupd)
	s.nsqlookupds = append(s.nsqlookupds, nsqLookupd)

	pretty.Println(s.nsqlookupds)

}

func (s *Service) SetEnvironment(environment string) {
	s.environment = environment
}

func NewService() *Service {
	return &Service{}
}

type nopLogger struct{}

func (*nopLogger) Output(int, string) error {
	return nil
}

func (s *Service) Start() error {
	if len(s.eventListeners) == 0 {
		return ErrNoEventListeners
	}

	for _, c := range s.eventListeners {
		c.listenerConfig.Lookup = s.nsqlookupds
		consumer, err := c.Start(s.environment)
		if err != nil {
			return err
		}

		s.consumers = append(s.consumers, consumer)

	}

	return nil
}

func (s *Service) Stop() {
	wg := sync.WaitGroup{}
	for _, c := range s.consumers {
		wg.Add(1)
		go func(c *nsq.Consumer) {
			c.Stop()
			<-c.StopChan
			wg.Done()
		}(c)
	}
	wg.Wait()
}
