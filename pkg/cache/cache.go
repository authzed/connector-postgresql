package cache

import (
	"context"
	"sync"

	"github.com/authzed/authzed-go/proto/authzed/api/v1"
)

// TODO: metrics
// TODO: store xlogpos
// TODO: fix deletion

// Cache stores operations fetched from the WAL that need to be transformed
// and synced to SpiceDB. It is safe to use from multiple threads.
type Cache struct {
	sync.Mutex
	sync.Cond
	ctx context.Context

	queue []string
	rels  map[string]*Operation
}

// OperationType stores what needs to happen to the relationships in the cache
type OperationType int

func (t OperationType) String() string {
	switch t {
	case OperationTypeTouch:
		return "touch"
	case OperationTypeDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// RelationshipUpdateOpType transforms the cache's OpType to a v1 relationship
// operation.
func (t OperationType) RelationshipUpdateOpType() v1.RelationshipUpdate_Operation {
	switch t {
	case OperationTypeTouch:
		return v1.RelationshipUpdate_OPERATION_TOUCH
	case OperationTypeDelete:
		return v1.RelationshipUpdate_OPERATION_DELETE
	default:
		return v1.RelationshipUpdate_OPERATION_UNSPECIFIED
	}
}

// Currently supported types are Touch and Delete
const (
	OperationTypeTouch OperationType = iota
	OperationTypeDelete
)

// Operation is an OpType + the Relationship that was generated
type Operation struct {
	OpType OperationType
	Rel    *v1.Relationship
}

// NewCache returns a new cache tied to the lifetime of the context.
// The cache can be closed by cancelling the context.
// Calling NewCache spawns a goroutine to handle cancellation.
func NewCache(ctx context.Context) *Cache {
	c := Cache{
		rels:  make(map[string]*Operation, 0),
		queue: make([]string, 0),
		ctx:   ctx,
	}
	// the cache's mutex also serves as the sync.Cond locker
	c.L = &c

	// listen for context cancellation and broadcast when context is closed
	// this will unblock anything waiting on the sync.Cond and let them clean up
	go func() {
		<-ctx.Done()
		c.Broadcast()
	}()
	return &c
}

// Touch puts a new touch event in the cache for the relationship
func (c *Cache) Touch(rel *v1.Relationship) {
	key := rel.String()
	c.Lock()
	defer c.Unlock()
	defer c.Broadcast()

	op, ok := c.rels[key]
	if ok {
		op.OpType = OperationTypeTouch
		return
	}
	c.rels[key] = &Operation{OpType: OperationTypeTouch, Rel: rel}
	c.queue = append(c.queue, key)
}

// TODO: WARNING: DELETEs need to be handled differently

// Delete puts a new delete event in the cache for the relationship
func (c *Cache) Delete(rel *v1.Relationship) {
	key := rel.String()
	c.Lock()
	defer c.Unlock()

	// if the tuple is deleted before it's popped, we can just remove it
	// from the queue
	if _, ok := c.rels[key]; ok {
		delete(c.rels, key)
		return
	}
	c.rels[key] = &Operation{OpType: OperationTypeDelete, Rel: rel}
	c.queue = append(c.queue, key)
}

// Requeue re-adds a key that failed to apply
// It is handled differently from normal additions to the queue. A delete added
// to a cache with a touch would normally cancel both out, but a requeued delete
// may still need to delete the relationship in the backing spicedb.
func (c *Cache) Requeue(op OperationType, rel *v1.Relationship) {
	key := rel.String()
	c.Lock()
	defer c.Unlock()

	// if it exists, overwrite the operation
	if _, ok := c.rels[key]; ok {
		c.rels[key].OpType = op
	}
	c.rels[key] = &Operation{OpType: op, Rel: rel}
	c.queue = append(c.queue, key)
}

// Next returns the next relationship in the queue
// it blocks until an item is added if the queue is empty and returns nil only
// when stopped via the context
func (c *Cache) Next() *Operation {
	c.Lock()
	defer c.Unlock()
	for {
		if len(c.queue) == 0 {
			// wait until there are more items in the queue
			c.Wait()
		}
		// exit if the context has been cancelled
		if c.ctx.Err() != nil {
			return nil
		}
		key := c.queue[0]
		c.queue = c.queue[1:]
		rel, ok := c.rels[key]
		if !ok {
			// rel was deleted, check next in queue
			continue
		}
		delete(c.rels, key)
		return rel
	}
}
