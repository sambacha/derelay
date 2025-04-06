package actor

import (
	"context"
	"fmt"
	"sync"

	"github.com/RabbyHub/derelay/log"
	"go.uber.org/zap"
)

// ActorSystem provides a top-level container for all actors
type ActorSystem struct {
	name        string
	rootCtx     context.Context
	rootCancel  context.CancelFunc
	rootActors  sync.Map // map[string]Actor
	deadLetters *deadLetterActor
	mu          sync.Mutex
}

// NewActorSystem creates a new actor system with the given name
func NewActorSystem(name string) *ActorSystem {
	rootCtx, rootCancel := context.WithCancel(context.Background())

	system := &ActorSystem{
		name:       name,
		rootCtx:    rootCtx,
		rootCancel: rootCancel,
		rootActors: sync.Map{},
		mu:         sync.Mutex{},
	}

	// Create the dead letters actor
	deadLetters := newDeadLetterActor(system)
	system.deadLetters = deadLetters

	return system
}

// ActorOf creates a new top-level actor using the provided props
func (s *ActorSystem) ActorOf(props ActorProps, name string) (Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if an actor with this name already exists
	if _, exists := s.rootActors.Load(name); exists {
		return nil, fmt.Errorf("actor with name '%s' already exists", name)
	}

	// Create actor context
	actorCtx, actorCancel := context.WithCancel(s.rootCtx)
	ctx := &baseActorContext{
		ctx:      actorCtx,
		cancel:   actorCancel,
		system:   s,
		children: sync.Map{},
		watchers: sync.Map{},
		path:     "/" + name,
	}

	// Create the actor
	actor := props.Create(ctx)
	ctx.self = actor

	// Start the actor
	if err := actor.Start(); err != nil {
		actorCancel()
		return nil, fmt.Errorf("failed to start actor: %w", err)
	}

	// Store in root actors map
	s.rootActors.Store(name, actor)

	log.Info("Created root actor", zap.String("name", name), zap.String("id", actor.ID()))
	return actor, nil
}

// Stop gracefully shuts down the actor system and all actors
func (s *ActorSystem) Stop() {
	log.Info("Stopping actor system", zap.String("name", s.name))

	// First try to stop all root actors gracefully
	s.rootActors.Range(func(name, actorInterface interface{}) bool {
		actor := actorInterface.(Actor)
		if err := actor.Stop(); err != nil {
			log.Warn("Error stopping root actor",
				zap.String("name", name.(string)),
				zap.String("id", actor.ID()),
				zap.Error(err))
		}
		return true
	})

	// Then cancel the root context
	s.rootCancel()

	// Finally, stop the dead letter actor
	s.deadLetters.stop()

	log.Info("Actor system stopped", zap.String("name", s.name))
}

// DeadLetters returns the special actor that receives undeliverable messages
func (s *ActorSystem) DeadLetters() Actor {
	return s.deadLetters
}

// deadLetterActor is a special actor that receives undeliverable messages
type deadLetterActor struct {
	id     string
	system *ActorSystem
	msgCh  chan Message
	stopCh chan struct{}
}

// newDeadLetterActor creates a new dead letter actor
func newDeadLetterActor(system *ActorSystem) *deadLetterActor {
	a := &deadLetterActor{
		id:     "deadletters",
		system: system,
		msgCh:  make(chan Message, 100),
		stopCh: make(chan struct{}),
	}

	// Start processing messages
	go a.processMessages()

	return a
}

// processMessages handles messages sent to the dead letter actor
func (a *deadLetterActor) processMessages() {
	for {
		select {
		case msg := <-a.msgCh:
			log.Debug("Dead letter received",
				zap.String("messageType", msg.MessageType()))
		case <-a.stopCh:
			return
		}
	}
}

// ID returns the actor's identifier
func (a *deadLetterActor) ID() string {
	return a.id
}

// Start initializes the actor (no-op for deadLetterActor as it starts on creation)
func (a *deadLetterActor) Start() error {
	return nil
}

// Stop shuts down the actor
func (a *deadLetterActor) Stop() error {
	a.stop()
	return nil
}

// stop is an internal method to shut down the actor
func (a *deadLetterActor) stop() {
	select {
	case <-a.stopCh: // Already stopped
		return
	default:
		close(a.stopCh)
	}
}

// Receive handles a message
func (a *deadLetterActor) Receive(message Message) {
	select {
	case a.msgCh <- message:
		// Message queued
	default:
		// Channel full, dropping message
		log.Warn("Dead letter mailbox full, dropping message",
			zap.String("messageType", message.MessageType()))
	}
}
