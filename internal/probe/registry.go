package probe

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

type Subscription struct {
	mu     sync.RWMutex
	closed bool
	ch     chan model.Sample
}

func newSubscription(buffer int) *Subscription {
	if buffer < 1 {
		buffer = 1
	}
	return &Subscription{ch: make(chan model.Sample, buffer)}
}

func (s *Subscription) Send(sample model.Sample) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return false
	}
	select {
	case s.ch <- sample:
		return true
	default:
		return false
	}
}

func (s *Subscription) Samples() <-chan model.Sample {
	return s.ch
}

// Len reports how many samples are buffered waiting for a consumer.
func (s *Subscription) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.ch)
}

// Cap reports the maximum number of samples the buffer can hold.
func (s *Subscription) Cap() int {
	return cap(s.ch)
}

func (s *Subscription) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
}

func (s *Subscription) Closed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

type Registry struct {
	mu            sync.RWMutex
	probes        map[string]model.Probe
	subscriptions map[string]*Subscription
	buffer        int
}

func NewRegistry(buffer int) *Registry {
	return &Registry{
		probes:        make(map[string]model.Probe),
		subscriptions: make(map[string]*Subscription),
		buffer:        buffer,
	}
}

func (r *Registry) Register(probe model.Probe) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.probes[probe.ID]; exists {
		return errors.New("probe already registered")
	}
	r.probes[probe.ID] = probe
	r.subscriptions[probe.ID] = newSubscription(r.buffer)
	return nil
}

func (r *Registry) Get(id string) (model.Probe, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	probe, ok := r.probes[id]
	return probe, ok
}

func (r *Registry) List() []model.Probe {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]model.Probe, 0, len(r.probes))
	for _, probe := range r.probes {
		result = append(result, probe)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (r *Registry) Subscription(id string) (*Subscription, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	subscription, ok := r.subscriptions[id]
	return subscription, ok
}

// QueueDepth reports buffered-but-unconsumed samples for one probe and the
// buffer capacity. A detached/unknown probe reports ok == false.
func (r *Registry) QueueDepth(id string) (depth, capacity int, ok bool) {
	r.mu.RLock()
	subscription, exists := r.subscriptions[id]
	r.mu.RUnlock()
	if !exists {
		return 0, 0, false
	}
	return subscription.Len(), subscription.Cap(), true
}

// QueueDepths returns probe ID -> buffered sample count for every live
// subscription.
func (r *Registry) QueueDepths() map[string]int {
	r.mu.RLock()
	subscriptions := make(map[string]*Subscription, len(r.subscriptions))
	for id, subscription := range r.subscriptions {
		subscriptions[id] = subscription
	}
	r.mu.RUnlock()
	depths := make(map[string]int, len(subscriptions))
	for id, subscription := range subscriptions {
		depths[id] = subscription.Len()
	}
	return depths
}

func (r *Registry) Update(id string, apply func(model.Probe) (model.Probe, error)) (model.Probe, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.probes[id]
	if !ok {
		return model.Probe{}, errors.New("probe not found")
	}
	next, err := apply(current)
	if err != nil {
		return model.Probe{}, err
	}
	if next.ID != current.ID || next.BindingGeneration < current.BindingGeneration {
		return model.Probe{}, errors.New("probe identity moved backward")
	}
	r.probes[id] = next
	return next, nil
}

func (r *Registry) Detach(id string, now time.Time) error {
	r.mu.Lock()
	probe, ok := r.probes[id]
	if !ok {
		r.mu.Unlock()
		return errors.New("probe not found")
	}
	subscription := r.subscriptions[id]
	probe.Connected = false
	probe.BindingGeneration++
	probe.UpdatedAt = now.UTC()
	r.probes[id] = probe
	delete(r.subscriptions, id)
	r.mu.Unlock()
	if subscription != nil {
		subscription.Close()
	}
	return nil
}

func (r *Registry) Attach(id string, now time.Time) (*Subscription, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	probe, ok := r.probes[id]
	if !ok {
		return nil, errors.New("probe not found")
	}
	if subscription, exists := r.subscriptions[id]; exists && !subscription.Closed() {
		return subscription, nil
	}
	probe.Connected = true
	probe.BindingGeneration++
	probe.UpdatedAt = now.UTC()
	r.probes[id] = probe
	subscription := newSubscription(r.buffer)
	r.subscriptions[id] = subscription
	return subscription, nil
}
