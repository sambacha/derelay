package fakeredis

import (
	"context"
	"errors"  // Add errors import
	"fmt"     // Add fmt import
	"strconv" // Add strconv import
	"strings" // Add strings import
	"sync"
	"time"

	"github.com/RabbyHub/derelay/relay" // Import the package where interfaces are defined
	"github.com/redis/go-redis/v9"
)

// FakeRedisClient implements the relay.RedisClient interface for testing.
type FakeRedisClient struct {
	mu            sync.RWMutex
	hashes        map[string]map[string]string                       // key -> field -> value
	sets          map[string]map[string]struct{}                     // key -> member -> exists
	expirations   map[string]time.Time                               // key -> expiration time
	pubsub        map[string][]*fakeRedisPubSub                      // channel -> list of active pubsub instances
	streams       map[string][]fakeStreamEntry                       // stream key -> list of entries
	streamGroups  map[string]map[string]string                       // stream key -> group name -> last delivered ID
	streamPending map[string]map[string]map[string][]fakeStreamEntry // stream key -> group name -> consumer name -> pending entries
}

// Represents a single entry in a fake stream
type fakeStreamEntry struct {
	ID     string
	Values map[string]interface{}
}

// New creates a new FakeRedisClient.
func New() *FakeRedisClient {
	return &FakeRedisClient{
		hashes:        make(map[string]map[string]string),
		sets:          make(map[string]map[string]struct{}),
		expirations:   make(map[string]time.Time),
		pubsub:        make(map[string][]*fakeRedisPubSub),
		streams:       make(map[string][]fakeStreamEntry),
		streamGroups:  make(map[string]map[string]string),
		streamPending: make(map[string]map[string]map[string][]fakeStreamEntry),
	}
}

// --- Implement relay.RedisClient interface ---

func (f *FakeRedisClient) Subscribe(ctx context.Context, channels ...string) relay.RedisPubSub {
	// Locking is handled within fakeRedisPubSub methods where necessary
	fakePS := newFakeRedisPubSub(f, channels...) // Pass the client for publish interaction
	return fakePS
}

func (f *FakeRedisClient) Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd {
	f.mu.RLock() // Read lock to access pubsub map
	defer f.mu.RUnlock()

	subscribers := 0
	if subs, ok := f.pubsub[channel]; ok {
		msgStr, _ := message.(string) // Assuming string messages based on usage
		redisMsg := &redis.Message{
			Channel: channel,
			Payload: msgStr,
		}
		// Send to all active subscribers for this channel
		activeSubs := []*fakeRedisPubSub{}
		for _, sub := range subs {
			if !sub.isClosed() {
				sub.deliver(redisMsg) // Let fakeRedisPubSub handle delivery
				activeSubs = append(activeSubs, sub)
				subscribers++
			}
		}
		// Update the list in the main client map if subscribers might close themselves
		// This requires write lock on the main client, which complicates things.
		// Simpler approach: Let Unsubscribe/Close handle removal from the map.
	}

	// Publish returns the number of clients that received the message.
	cmd := &redis.IntCmd{}
	cmd.SetVal(int64(subscribers))
	return cmd
}

func (f *FakeRedisClient) XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.checkExpiry(a.Stream) // Streams can expire too

	streamKey := a.Stream
	if _, ok := f.streams[streamKey]; !ok {
		f.streams[streamKey] = []fakeStreamEntry{}
	}

	// Generate a simple ID (timestamp-sequence)
	entryID := fmt.Sprintf("%d-0", time.Now().UnixNano()/int64(time.Millisecond))
	if len(f.streams[streamKey]) > 0 {
		lastID := f.streams[streamKey][len(f.streams[streamKey])-1].ID
		parts := strings.Split(lastID, "-")
		lastTs, _ := strconv.ParseInt(parts[0], 10, 64)
		lastSeq, _ := strconv.ParseInt(parts[1], 10, 64)
		currentTs := time.Now().UnixNano() / int64(time.Millisecond)
		if currentTs == lastTs {
			entryID = fmt.Sprintf("%d-%d", currentTs, lastSeq+1)
		} else {
			entryID = fmt.Sprintf("%d-0", currentTs)
		}
	}
	if a.ID != "" && a.ID != "*" {
		entryID = a.ID // Allow specific ID setting if provided
	}

	valuesMap, ok := a.Values.(map[string]interface{})
	if !ok {
		valuesMap = make(map[string]interface{}) // Handle potential non-map values gracefully
	}

	entry := fakeStreamEntry{
		ID:     entryID,
		Values: valuesMap,
	}
	f.streams[streamKey] = append(f.streams[streamKey], entry)

	// Handle MaxLen/Approx trimming
	if a.MaxLen > 0 && int64(len(f.streams[streamKey])) > a.MaxLen {
		f.streams[streamKey] = f.streams[streamKey][int64(len(f.streams[streamKey]))-a.MaxLen:]
	}

	cmd := &redis.StringCmd{}
	cmd.SetVal(entry.ID)
	return cmd
}

func (f *FakeRedisClient) XGroupCreateMkStream(ctx context.Context, stream string, group string, start string) *redis.StatusCmd {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.checkExpiry(stream)

	if _, ok := f.streams[stream]; !ok {
		f.streams[stream] = []fakeStreamEntry{}
	}
	if _, ok := f.streamGroups[stream]; !ok {
		f.streamGroups[stream] = make(map[string]string)
	}

	if _, ok := f.streamGroups[stream][group]; !ok {
		startID := start
		if start == "$" {
			if len(f.streams[stream]) > 0 {
				startID = f.streams[stream][len(f.streams[stream])-1].ID
			} else {
				startID = "0-0"
			}
		} else if start == "" {
			startID = "0"
		}
		f.streamGroups[stream][group] = startID
		if _, ok := f.streamPending[stream]; !ok {
			f.streamPending[stream] = make(map[string]map[string][]fakeStreamEntry)
		}
		f.streamPending[stream][group] = make(map[string][]fakeStreamEntry)
	}

	cmd := &redis.StatusCmd{}
	cmd.SetVal("OK")
	return cmd
}

func (f *FakeRedisClient) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	f.mu.Lock() // Use Write lock as we modify pending list
	defer f.mu.Unlock()

	results := []redis.XStream{}
	readCount := int(a.Count)
	if readCount == 0 {
		readCount = 10
	}

	for _, streamArg := range a.Streams {
		streamKey := streamArg
		requestedID := ">"
		parts := strings.Fields(streamArg)
		if len(parts) == 2 {
			streamKey = parts[0]
			requestedID = parts[1]
		} else if len(parts) != 1 {
			continue
		}

		f.checkExpiry(streamKey)

		if _, ok := f.streams[streamKey]; !ok {
			continue
		}
		if _, ok := f.streamGroups[streamKey]; !ok {
			continue
		}
		groupInfo, ok := f.streamGroups[streamKey][a.Group]
		if !ok {
			continue
		}
		_ = groupInfo

		if _, ok := f.streamPending[streamKey]; !ok {
			f.streamPending[streamKey] = make(map[string]map[string][]fakeStreamEntry)
		}
		if _, ok := f.streamPending[streamKey][a.Group]; !ok {
			f.streamPending[streamKey][a.Group] = make(map[string][]fakeStreamEntry)
		}
		if _, ok := f.streamPending[streamKey][a.Group][a.Consumer]; !ok {
			f.streamPending[streamKey][a.Group][a.Consumer] = []fakeStreamEntry{}
		}

		streamMessages := []redis.XMessage{}
		foundMessages := 0

		if requestedID == "0" {
			pending := f.streamPending[streamKey][a.Group][a.Consumer]
			for _, entry := range pending {
				if foundMessages >= readCount {
					break
				}
				streamMessages = append(streamMessages, redis.XMessage{ID: entry.ID, Values: entry.Values})
				foundMessages++
			}
		}

		if requestedID == ">" && foundMessages < readCount {
			streamData := f.streams[streamKey]
			lastDeliveredID := f.streamGroups[streamKey][a.Group]
			startIndex := -1
			for i, entry := range streamData {
				if compareStreamIDs(entry.ID, lastDeliveredID) > 0 {
					startIndex = i
					break
				}
			}

			if startIndex != -1 {
				newlyPending := []fakeStreamEntry{}
				for i := startIndex; i < len(streamData); i++ {
					if foundMessages >= readCount {
						break
					}
					entry := streamData[i]
					streamMessages = append(streamMessages, redis.XMessage{ID: entry.ID, Values: entry.Values})
					newlyPending = append(newlyPending, entry)
					f.streamGroups[streamKey][a.Group] = entry.ID // Update last delivered ID for the group
					foundMessages++
				}
				f.streamPending[streamKey][a.Group][a.Consumer] = append(f.streamPending[streamKey][a.Group][a.Consumer], newlyPending...)
			}
		}

		if len(streamMessages) > 0 {
			results = append(results, redis.XStream{Stream: streamKey, Messages: streamMessages})
		}
	}

	cmd := &redis.XStreamSliceCmd{}
	// Only set redis.Nil for specific cases like reading pending '0' when none exist,
	// not for reading new '>' when none exist.
	// For simplicity here, we'll just return empty slice if no results,
	// which matches the behavior needed for the current contract test.
	// A more accurate fake might need finer-grained error handling.
	cmd.SetVal(results) // Always set value, even if empty
	return cmd
}

func (f *FakeRedisClient) XAck(ctx context.Context, stream string, group string, ids ...string) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()

	removedIDs := make(map[string]bool)

	if _, ok := f.streamPending[stream]; !ok {
		cmd := &redis.IntCmd{}
		cmd.SetVal(0)
		return cmd
	}
	groupPending, ok := f.streamPending[stream][group]
	if !ok {
		cmd := &redis.IntCmd{}
		cmd.SetVal(0)
		return cmd
	}

	for consumer, pendingList := range groupPending {
		newList := []fakeStreamEntry{}
		consumerListChanged := false
		for _, pendingEntry := range pendingList {
			shouldRemove := false
			for _, ackID := range ids {
				if pendingEntry.ID == ackID {
					shouldRemove = true
					removedIDs[ackID] = true
					break
				}
			}
			if !shouldRemove {
				newList = append(newList, pendingEntry)
			} else {
				consumerListChanged = true
			}
		}
		if consumerListChanged {
			f.streamPending[stream][group][consumer] = newList
		}
	}

	cmd := &redis.IntCmd{}
	cmd.SetVal(int64(len(removedIDs)))
	return cmd
}

func (f *FakeRedisClient) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.checkExpiry(key)

	if _, ok := f.hashes[key]; !ok {
		f.hashes[key] = make(map[string]string)
	}

	count := 0
	for i := 0; i < len(values); i += 2 {
		field, okField := values[i].(string)
		value, okValue := values[i+1].(string)
		if !okField || !okValue {
			continue
		}
		if _, exists := f.hashes[key][field]; !exists {
			count++
		}
		f.hashes[key][field] = value
	}

	cmd := &redis.IntCmd{}
	cmd.SetVal(int64(count))
	return cmd
}

func (f *FakeRedisClient) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.checkExpiry(key)

	if _, ok := f.sets[key]; !ok {
		f.sets[key] = make(map[string]struct{})
	}

	addedCount := 0
	for _, member := range members {
		memberStr, ok := member.(string)
		if !ok {
			continue
		}
		if _, exists := f.sets[key][memberStr]; !exists {
			f.sets[key][memberStr] = struct{}{}
			addedCount++
		}
	}

	cmd := &redis.IntCmd{}
	cmd.SetVal(int64(addedCount))
	return cmd
}

func (f *FakeRedisClient) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	f.mu.Lock()
	defer f.mu.Unlock()

	exists := f.keyExists(key) // Assumes keyExists handles expiry check internally
	if !exists {
		cmd := &redis.BoolCmd{}
		cmd.SetVal(false)
		return cmd
	}

	if expiration <= 0 {
		delete(f.expirations, key)
	} else {
		f.expirations[key] = time.Now().Add(expiration)
	}

	cmd := &redis.BoolCmd{}
	cmd.SetVal(true)
	return cmd
}

func (f *FakeRedisClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()

	deletedCount := 0
	for _, key := range keys {
		f.checkExpiry(key) // Check expiry first

		deleted := false
		if _, ok := f.hashes[key]; ok {
			delete(f.hashes, key)
			deleted = true
		}
		if _, ok := f.sets[key]; ok {
			delete(f.sets, key)
			deleted = true
		}
		if _, ok := f.streams[key]; ok {
			delete(f.streams, key)
			deleted = true
		} // Also delete streams
		if _, ok := f.streamGroups[key]; ok {
			delete(f.streamGroups, key)
			deleted = true
		}
		if _, ok := f.streamPending[key]; ok {
			delete(f.streamPending, key)
			deleted = true
		}

		if deleted {
			delete(f.expirations, key)
			deletedCount++
		}
	}

	cmd := &redis.IntCmd{}
	cmd.SetVal(int64(deletedCount))
	return cmd
}

func (f *FakeRedisClient) Pipeline() relay.RedisPipeliner {
	return newFakeRedisPipeliner(f)
}

func (f *FakeRedisClient) Close() error {
	return nil // No-op for fake
}

// --- Helper methods ---

func (f *FakeRedisClient) keyExists(key string) bool {
	// Assumes caller holds lock
	f.checkExpiry(key)
	if _, ok := f.hashes[key]; ok {
		return true
	}
	if _, ok := f.sets[key]; ok {
		return true
	}
	if _, ok := f.streams[key]; ok {
		return true
	}
	return false
}

func (f *FakeRedisClient) checkExpiry(key string) {
	// Assumes caller holds lock
	expiry, ok := f.expirations[key]
	if ok && time.Now().After(expiry) {
		delete(f.hashes, key)
		delete(f.sets, key)
		delete(f.expirations, key)
		delete(f.streams, key)
		delete(f.streamGroups, key)
		delete(f.streamPending, key)
	}
}

// compareStreamIDs compares two Redis stream IDs (ts-seq). Returns -1, 0, or 1.
func compareStreamIDs(id1, id2 string) int {
	p1 := strings.Split(id1, "-")
	p2 := strings.Split(id2, "-")
	ts1, _ := strconv.ParseInt(p1[0], 10, 64)
	// Handle cases like "0" where there's no sequence number
	var seq1 int64
	if len(p1) > 1 {
		seq1, _ = strconv.ParseInt(p1[1], 10, 64)
	}
	ts2, _ := strconv.ParseInt(p2[0], 10, 64)
	var seq2 int64
	if len(p2) > 1 {
		seq2, _ = strconv.ParseInt(p2[1], 10, 64)
	}

	if ts1 < ts2 {
		return -1
	}
	if ts1 > ts2 {
		return 1
	}
	if seq1 < seq2 {
		return -1
	}
	if seq1 > seq2 {
		return 1
	}
	return 0
}

// Ensure FakeRedisClient satisfies the interface (compile-time check)
var _ relay.RedisClient = (*FakeRedisClient)(nil)

// --- Fake Redis PubSub ---

type fakeRedisPubSub struct {
	client         *FakeRedisClient
	mu             sync.RWMutex
	channels       map[string]struct{}
	messageCh      chan *redis.Message
	closed         bool
	receiveConfirm chan struct{}
}

func newFakeRedisPubSub(client *FakeRedisClient, initialChannels ...string) *fakeRedisPubSub {
	ps := &fakeRedisPubSub{
		client:         client,
		channels:       make(map[string]struct{}),
		messageCh:      make(chan *redis.Message, 128),
		receiveConfirm: make(chan struct{}, 1),
	}
	for _, ch := range initialChannels {
		ps.channels[ch] = struct{}{}
	}
	ps.registerWithClient()
	ps.receiveConfirm <- struct{}{}
	return ps
}

func (ps *fakeRedisPubSub) registerWithClient() {
	ps.client.mu.Lock()
	defer ps.client.mu.Unlock()
	for ch := range ps.channels {
		ps.client.pubsub[ch] = append(ps.client.pubsub[ch], ps)
	}
}

func (ps *fakeRedisPubSub) Channel(opts ...redis.ChannelOption) <-chan *redis.Message {
	return ps.messageCh
}

func (ps *fakeRedisPubSub) Receive(ctx context.Context) (interface{}, error) {
	select {
	case <-ps.receiveConfirm:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(1 * time.Second):
		return nil, errors.New("fakeredis: timeout waiting for subscription confirmation")
	}
}

func (ps *fakeRedisPubSub) Subscribe(ctx context.Context, channels ...string) error {
	ps.mu.Lock()
	if ps.closed {
		ps.mu.Unlock()
		return errors.New("fakeredis: pubsub connection closed")
	}
	newlySubscribed := []string{}
	for _, ch := range channels {
		if _, exists := ps.channels[ch]; !exists {
			ps.channels[ch] = struct{}{}
			newlySubscribed = append(newlySubscribed, ch)
		}
	}
	ps.mu.Unlock()

	if len(newlySubscribed) > 0 {
		ps.client.mu.Lock()
		for _, ch := range newlySubscribed {
			ps.client.pubsub[ch] = append(ps.client.pubsub[ch], ps)
		}
		ps.client.mu.Unlock()
	}
	return nil
}

func (ps *fakeRedisPubSub) Unsubscribe(ctx context.Context, channels ...string) error {
	ps.mu.Lock()
	if ps.closed {
		ps.mu.Unlock()
		return nil
	}
	channelsToRemove := []string{}
	if len(channels) == 0 {
		for ch := range ps.channels {
			channelsToRemove = append(channelsToRemove, ch)
		}
		ps.channels = make(map[string]struct{})
	} else {
		for _, ch := range channels {
			if _, exists := ps.channels[ch]; exists {
				delete(ps.channels, ch)
				channelsToRemove = append(channelsToRemove, ch)
			}
		}
	}
	ps.mu.Unlock()

	if len(channelsToRemove) > 0 {
		ps.client.mu.Lock()
		for _, ch := range channelsToRemove {
			if subs, ok := ps.client.pubsub[ch]; ok {
				newSubs := []*fakeRedisPubSub{}
				for _, sub := range subs {
					if sub != ps {
						newSubs = append(newSubs, sub)
					}
				}
				if len(newSubs) == 0 {
					delete(ps.client.pubsub, ch)
				} else {
					ps.client.pubsub[ch] = newSubs
				}
			}
		}
		ps.client.mu.Unlock()
	}
	return nil
}

func (ps *fakeRedisPubSub) Close() error {
	ps.mu.Lock()
	if ps.closed {
		ps.mu.Unlock()
		return nil
	}
	ps.closed = true
	close(ps.messageCh)
	channelsToUnsub := []string{}
	for ch := range ps.channels {
		channelsToUnsub = append(channelsToUnsub, ch)
	}
	ps.channels = make(map[string]struct{})
	ps.mu.Unlock()

	if len(channelsToUnsub) > 0 {
		ps.Unsubscribe(context.Background(), channelsToUnsub...)
	}
	return nil
}

func (ps *fakeRedisPubSub) deliver(msg *redis.Message) {
	ps.mu.RLock()
	isClosed := ps.closed
	ps.mu.RUnlock()
	if isClosed {
		return
	}
	select {
	case ps.messageCh <- msg:
	default: // Non-blocking send
	}
}

func (ps *fakeRedisPubSub) isClosed() bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.closed
}

var _ relay.RedisPubSub = (*fakeRedisPubSub)(nil)

// --- Fake Redis Pipeliner ---

type fakeRedisPipeliner struct {
	client *FakeRedisClient
	mu     sync.Mutex
	cmds   []func() (redis.Cmder, error)
}

func newFakeRedisPipeliner(client *FakeRedisClient) *fakeRedisPipeliner {
	return &fakeRedisPipeliner{
		client: client,
		cmds:   []func() (redis.Cmder, error){},
	}
}

func (p *fakeRedisPipeliner) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	cmd := &redis.IntCmd{}
	p.mu.Lock()
	p.cmds = append(p.cmds, func() (redis.Cmder, error) {
		resCmd := p.client.HSet(ctx, key, values...)
		val, err := resCmd.Result()
		cmd.SetVal(val)
		cmd.SetErr(err)
		return cmd, err
	})
	p.mu.Unlock()
	return cmd
}

func (p *fakeRedisPipeliner) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	cmd := &redis.BoolCmd{}
	p.mu.Lock()
	p.cmds = append(p.cmds, func() (redis.Cmder, error) {
		resCmd := p.client.Expire(ctx, key, expiration)
		val, err := resCmd.Result()
		cmd.SetVal(val)
		cmd.SetErr(err)
		return cmd, err
	})
	p.mu.Unlock()
	return cmd
}

func (p *fakeRedisPipeliner) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	cmd := &redis.IntCmd{}
	p.mu.Lock()
	p.cmds = append(p.cmds, func() (redis.Cmder, error) {
		resCmd := p.client.Del(ctx, keys...)
		val, err := resCmd.Result()
		cmd.SetVal(val)
		cmd.SetErr(err)
		return cmd, err
	})
	p.mu.Unlock()
	return cmd
}

func (p *fakeRedisPipeliner) Exec(ctx context.Context) ([]redis.Cmder, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	results := make([]redis.Cmder, 0, len(p.cmds))
	var firstErr error

	for _, cmdFunc := range p.cmds {
		resCmd, err := cmdFunc()
		results = append(results, resCmd)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.cmds = []func() (redis.Cmder, error){} // Clear commands
	return results, firstErr
}

var _ relay.RedisPipeliner = (*fakeRedisPipeliner)(nil)
