package actor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/RabbyHub/derelay/log"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

var (
	// ErrActorStopped is returned when trying to send to a stopped actor
	ErrActorStopped = errors.New("actor stopped")
	// ErrActorMailboxFull is returned when the actor's mailbox is full
	ErrActorMailboxFull = errors.New("actor mailbox full")
	// ErrActorTimeout is returned when a request to an actor times out
	ErrActorTimeout = errors.New("actor request timeout")
)

// DefaultMailboxSize is the default size of an actor's mailbox
const DefaultMailboxSize = 100

// baseActorContext implements the ActorContext interface
type baseActorContext struct {
	self     Actor
	parent   Actor
	children sync.Map
	watchers sync.Map
	ctx      context.Context
	cancel   context.CancelFunc
	system   *ActorSystem
	path     string
}

// Self returns the actor this context belongs to
func (c *baseActorContext) Self() Actor {
	return c.self
}

// Parent returns the parent of this actor
func (c *baseActorContext) Parent() Actor {
	return c.parent
}

// SpawnChild creates a new child actor
func (c *baseActorContext) SpawnChild(props ActorProps) (Actor, error) {
	childCtx, childCancel := context.WithCancel(c.ctx)
	childContext := &baseActorContext{
		parent:   c.self,
		ctx:      childCtx,
		cancel:   childCancel,
		system:   c.system,
		children: sync.Map{},
		watchers: sync.Map{},
	}

	// Create the child actor
	child := props.Create(childContext)
	childContext.self = child
	childContext.path = fmt.Sprintf("%s/%s", c.path, child.ID())

	// Add the child to our list
	c.children.Store(child.ID(), child)

	// Start the child actor
	if err := child.Start(); err != nil {
		childCancel()
		c.children.Delete(child.ID())
		return nil, fmt.Errorf("failed to start child actor: %w", err)
	}

	return child, nil
}

// Children returns all child actors
func (c *baseActorContext) Children() []Actor {
	var children []Actor
	c.children.Range(func(_, value interface{}) bool {
		children = append(children, value.(Actor))
		return true
	})
	return children
}

// Watch registers this actor to be notified when the target terminates
func (c *baseActorContext) Watch(target Actor) {
	// Implementation depends on internal structure of target actor
	// For now, simply store the watcher
	if targetCtx, ok := target.(*BaseActor); ok {
		targetCtx.context.(*baseActorContext).watchers.Store(c.self.ID(), c.self)
	}
}

// Unwatch stops watching the target actor
func (c *baseActorContext) Unwatch(target Actor) {
	if targetCtx, ok := target.(*BaseActor); ok {
		targetCtx.context.(*baseActorContext).watchers.Delete(c.self.ID())
	}
}

// Send asynchronously sends a message to another actor
func (c *baseActorContext) Send(target Actor, message Message) {
	target.Receive(message)
}

// Request sends a message and awaits a response (with timeout)
func (c *baseActorContext) Request(target Actor, message Message, timeout time.Duration) (Message, error) {
	replyTo := make(chan Message, 1)
	request := RequestMessage{
		Message: message,
		ReplyTo: replyTo,
	}

	target.Receive(request)

	select {
	case reply := <-replyTo:
		return reply, nil
	case <-time.After(timeout):
		return nil, ErrActorTimeout
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	}
}

// Context returns the Go context associated with this actor
func (c *baseActorContext) Context() context.Context {
	return c.ctx
}

// BaseActor provides common actor functionality
type BaseActor struct {
	id              string
	context         ActorContext
	mailbox         chan Message
	behaviors       map[string]func(Message)
	defaultBehavior func(Message)
	supervision     SupervisionStrategy
	started         bool
	stopped         bool
	mutex           sync.RWMutex
	wg              sync.WaitGroup
}

// NewBaseActor creates a new BaseActor with the given options
func NewBaseActor(ctx ActorContext, options ...ActorOption) *BaseActor {
	actor := &BaseActor{
		id:          uuid.New().String(),
		context:     ctx,
		mailbox:     make(chan Message, DefaultMailboxSize),
		behaviors:   make(map[string]func(Message)),
		supervision: &defaultStrategy{},
		started:     false,
		stopped:     false,
		mutex:       sync.RWMutex{},
	}

	// Apply options
	for _, option := range options {
		option(actor)
	}

	return actor
}

// ActorOption is a function that configures a BaseActor
type ActorOption func(*BaseActor)

// WithID sets the actor's ID
func WithID(id string) ActorOption {
	return func(a *BaseActor) {
		a.id = id
	}
}

// WithMailboxSize sets the actor's mailbox size
func WithMailboxSize(size int) ActorOption {
	return func(a *BaseActor) {
		a.mailbox = make(chan Message, size)
	}
}

// WithSupervision sets the supervision strategy
func WithSupervision(strategy SupervisionStrategy) ActorOption {
	return func(a *BaseActor) {
		a.supervision = strategy
	}
}

// ID returns the actor's ID
func (a *BaseActor) ID() string {
	return a.id
}

// handleMessage processes a single message
func (a *BaseActor) handleMessage(msg Message) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("Actor message handler panicked", fmt.Errorf("panic: %v", r), zap.String("actorID", a.id), zap.String("messageType", msg.MessageType()))
			// TODO: implement proper supervision for panics
		}
	}()

	// Handle system messages
	switch m := msg.(type) {
	case StartMessage:
		// Already handled in Start()
		return
	case StopMessage:
		// Already handled in Stop()
		return
	case RequestMessage:
		// Extract the inner message and process it
		// The handler is responsible for sending a response on the ReplyTo channel
		if handler, ok := a.behaviors[m.Message.MessageType()]; ok {
			handler(m)
		} else if a.defaultBehavior != nil {
			a.defaultBehavior(m)
		} else {
			// No handler for this message type
			if m.ReplyTo != nil {
				select {
				case m.ReplyTo <- &ErrorMessage{
					Err: fmt.Errorf("no handler for message type %s", m.Message.MessageType()),
					Msg: "no handler for message type",
				}:
				default:
					// Can't send response, channel might be full or closed
				}
			}
		}
		return
	}

	// Handle regular messages
	if handler, ok := a.behaviors[msg.MessageType()]; ok {
		handler(msg)
	} else if a.defaultBehavior != nil {
		a.defaultBehavior(msg)
	} else {
		// Default behavior is to log unhandled messages
		log.Debug("Unhandled message", zap.String("actorID", a.id), zap.String("messageType", msg.MessageType()))
	}
}

// Start initializes the actor and begins processing messages
func (a *BaseActor) Start() error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if a.started {
		return nil // Already started
	}

	if a.stopped {
		return ErrActorStopped
	}

	a.wg.Add(1)
	a.started = true

	// Start processing messages
	go func() {
		defer a.wg.Done()
		ctx := a.context.Context()

		for {
			select {
			case msg, ok := <-a.mailbox:
				if !ok {
					// Mailbox was closed, exit
					return
				}
				a.handleMessage(msg)
			case <-ctx.Done():
				// Context was cancelled, exit
				return
			}
		}
	}()

	// Register behavior for Start message
	a.handleMessage(StartMessage{})

	return nil
}

// Stop gracefully shuts down the actor
func (a *BaseActor) Stop() error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if a.stopped {
		return nil // Already stopped
	}

	// Mark as stopped before notifying watchers
	a.stopped = true

	// Notify watchers that we're stopping
	if ctx, ok := a.context.(*baseActorContext); ok {
		ctx.watchers.Range(func(_, watcher interface{}) bool {
			watcher.(Actor).Receive(TerminatedMessage{
				Actor: a,
				Err:   nil, // Normal termination
			})
			return true
		})
	}

	// Process Stop message
	a.handleMessage(StopMessage{})

	// Stop all children
	for _, child := range a.context.Children() {
		if err := child.Stop(); err != nil {
			log.Warn("Error stopping child actor", zap.String("childID", child.ID()), zap.Error(err))
		}
	}

	// Close mailbox in a separate goroutine to avoid deadlock
	// if someone tries to send to this actor while we're stopping
	go func() {
		// Let any queued messages process first
		time.Sleep(100 * time.Millisecond)
		close(a.mailbox)

		// Cancel context if we are a root actor
		if ctx, ok := a.context.(*baseActorContext); ok && ctx.cancel != nil {
			ctx.cancel()
		}
	}()

	return nil
}

// Receive enqueues a message for processing
func (a *BaseActor) Receive(message Message) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	if a.stopped {
		log.Debug("Message sent to stopped actor", zap.String("actorID", a.id), zap.String("messageType", message.MessageType()))
		return
	}

	// Special handling for RequestMessage to return error if we can't enqueue
	if req, ok := message.(RequestMessage); ok {
		select {
		case a.mailbox <- message:
			// Message enqueued successfully
		default:
			// Mailbox is full, return error through ReplyTo
			if req.ReplyTo != nil {
				select {
				case req.ReplyTo <- &ErrorMessage{
					Err: ErrActorMailboxFull,
					Msg: "actor mailbox is full",
				}:
				default:
					// Can't send response, channel might be full or closed
				}
			}
		}
		return
	}

	// Non-request messages
	select {
	case a.mailbox <- message:
		// Message enqueued successfully
	default:
		log.Warn("Actor mailbox full, dropping message", zap.String("actorID", a.id), zap.String("messageType", message.MessageType()))
	}
}

// Become registers a behavior for a specific message type
func (a *BaseActor) Become(messageType string, handler func(Message)) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.behaviors[messageType] = handler
}

// Unbecome removes a behavior for a specific message type
func (a *BaseActor) Unbecome(messageType string) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	delete(a.behaviors, messageType)
}

// SetDefaultBehavior sets the default behavior for unhandled messages
func (a *BaseActor) SetDefaultBehavior(handler func(Message)) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.defaultBehavior = handler
}

// SuperviseChild applies the supervision strategy to a child actor that failed
func (a *BaseActor) SuperviseChild(child Actor, err error) {
	directive := a.supervision.HandleFailure(child, err)

	switch directive {
	case Resume:
		// Do nothing, let the actor continue
	case Restart:
		// Stop and recreate the actor
		child.Stop()
		// Recreating would require keeping track of props used to create the child
		// This is left as an implementation detail for specific actors
	case Stop:
		child.Stop()
		// Remove from children list
		if ctx, ok := a.context.(*baseActorContext); ok {
			ctx.children.Delete(child.ID())
		}
	case Escalate:
		// Pass the failure to our parent
		if a.context.Parent() != nil {
			if parent, ok := a.context.Parent().(*BaseActor); ok {
				parent.SuperviseChild(a.context.Self(), err)
			}
		}
	}
}

// defaultStrategy is a simple supervision strategy that always stops failing actors
type defaultStrategy struct{}

// HandleFailure implements SupervisionStrategy
func (s *defaultStrategy) HandleFailure(actor Actor, err error) SupervisionDirective {
	log.Warn("Actor failed", zap.String("actorID", actor.ID()), zap.Error(err))
	return Stop
}

// OneForOneStrategy restarts only the failed actor
type OneForOneStrategy struct {
	maxRetries int
	withinTime time.Duration
	decider    func(err error) SupervisionDirective
	retryMap   map[string][]time.Time
	retryMutex sync.Mutex
}

// NewOneForOneStrategy creates a new OneForOneStrategy
func NewOneForOneStrategy(maxRetries int, withinTime time.Duration, decider func(err error) SupervisionDirective) *OneForOneStrategy {
	return &OneForOneStrategy{
		maxRetries: maxRetries,
		withinTime: withinTime,
		decider:    decider,
		retryMap:   make(map[string][]time.Time),
	}
}

// HandleFailure implements SupervisionStrategy
func (s *OneForOneStrategy) HandleFailure(actor Actor, err error) SupervisionDirective {
	directive := s.decider(err)

	if directive == Restart {
		s.retryMutex.Lock()
		defer s.retryMutex.Unlock()

		now := time.Now()
		actorID := actor.ID()

		// Add the current failure time
		s.retryMap[actorID] = append(s.retryMap[actorID], now)

		// Remove failures outside the time window
		var recentFailures []time.Time
		for _, t := range s.retryMap[actorID] {
			if now.Sub(t) <= s.withinTime {
				recentFailures = append(recentFailures, t)
			}
		}
		s.retryMap[actorID] = recentFailures

		// Check if we've exceeded max retries
		if len(recentFailures) > s.maxRetries {
			log.Warn("Actor exceeded restart retry limit", zap.String("actorID", actorID), zap.Error(err))
			return Stop
		}
	}

	return directive
}

// AllForOneStrategy restarts all children when one fails
type AllForOneStrategy struct {
	maxRetries   int
	withinTime   time.Duration
	decider      func(err error) SupervisionDirective
	retryCount   int
	firstFailure time.Time
	retryMutex   sync.Mutex
}

// NewAllForOneStrategy creates a new AllForOneStrategy
func NewAllForOneStrategy(maxRetries int, withinTime time.Duration, decider func(err error) SupervisionDirective) *AllForOneStrategy {
	return &AllForOneStrategy{
		maxRetries: maxRetries,
		withinTime: withinTime,
		decider:    decider,
		retryCount: 0,
	}
}

// HandleFailure implements SupervisionStrategy
func (s *AllForOneStrategy) HandleFailure(actor Actor, err error) SupervisionDirective {
	directive := s.decider(err)

	if directive == Restart {
		s.retryMutex.Lock()
		defer s.retryMutex.Unlock()

		now := time.Now()

		// Reset counter if we're outside the time window
		if s.retryCount > 0 && now.Sub(s.firstFailure) > s.withinTime {
			s.retryCount = 0
		}

		// Initialize time of first failure if this is the first retry
		if s.retryCount == 0 {
			s.firstFailure = now
		}

		s.retryCount++

		// Check if we've exceeded max retries
		if s.retryCount > s.maxRetries {
			log.Warn("Actor supervisor exceeded restart retry limit", zap.Error(err))
			return Stop
		}
	}

	return directive
}
