package actor

import (
	"context"
	"time"
)

// Message is the base interface for all actor messages
type Message interface {
	// MessageType returns the type identifier for this message
	MessageType() string
}

// Actor represents the basic actor interface that all actors must implement
type Actor interface {
	// Receive processes a message
	Receive(message Message)
	// Start initializes the actor and begins message processing
	Start() error
	// Stop gracefully shuts down the actor
	Stop() error
	// ID returns a unique identifier for this actor
	ID() string
}

// ActorContext provides actor system capabilities to actors
type ActorContext interface {
	// Self returns a reference to the current actor
	Self() Actor
	// Parent returns the parent actor
	Parent() Actor
	// SpawnChild creates a new child actor
	SpawnChild(props ActorProps) (Actor, error)
	// Children returns all child actors
	Children() []Actor
	// Watch allows an actor to be notified when another actor terminates
	Watch(target Actor)
	// Unwatch stops watching an actor
	Unwatch(target Actor)
	// Send asynchronously sends a message to another actor
	Send(target Actor, message Message)
	// Request sends a message and awaits a response (with timeout)
	Request(target Actor, message Message, timeout time.Duration) (Message, error)
	// Context returns the Go context associated with this actor
	Context() context.Context
}

// ActorProps defines how to create a new actor
type ActorProps interface {
	// Create instantiates a new actor
	Create(ctx ActorContext) Actor
}

// ActorPropsFunc is a function that implements ActorProps
type ActorPropsFunc func(ctx ActorContext) Actor

// Create implements the ActorProps interface
func (f ActorPropsFunc) Create(ctx ActorContext) Actor {
	return f(ctx)
}

// SupervisionStrategy defines how to handle failures in child actors
type SupervisionStrategy interface {
	// HandleFailure decides what to do when a child actor fails
	HandleFailure(actor Actor, err error) SupervisionDirective
}

// SupervisionDirective indicates what action to take for a failed actor
type SupervisionDirective int

const (
	// Resume allows the actor to continue with its current state
	Resume SupervisionDirective = iota
	// Restart recreates the actor while losing its internal state
	Restart
	// Stop permanently stops the actor
	Stop
	// Escalate passes the failure up to the parent's supervisor
	Escalate
)

// TerminatedMessage is sent to watchers when an actor terminates
type TerminatedMessage struct {
	Actor Actor
	Err   error
}

// MessageType implements the Message interface
func (t TerminatedMessage) MessageType() string {
	return "actor.terminated"
}

// StartMessage is sent to actors to trigger initialization
type StartMessage struct{}

// MessageType implements the Message interface
func (s StartMessage) MessageType() string {
	return "actor.start"
}

// StopMessage is sent to actors to trigger graceful shutdown
type StopMessage struct{}

// MessageType implements the Message interface
func (s StopMessage) MessageType() string {
	return "actor.stop"
}

// RequestMessage wraps a message with reply channel for request-response pattern
type RequestMessage struct {
	Message Message
	ReplyTo chan Message
}

// MessageType implements the Message interface
func (r RequestMessage) MessageType() string {
	return "actor.request"
}

// ErrorMessage represents an error that occurred during message processing
type ErrorMessage struct {
	Err error
	Msg string
}

// MessageType implements the Message interface
func (e ErrorMessage) MessageType() string {
	return "actor.error"
}
