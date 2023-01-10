/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"errors"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"
)

// PopProcessFunc is passed to Pop() method of Queue interface.
// It is supposed to process the accumulator popped from the queue.
type PopProcessFunc func(interface{}) error

// ErrRequeue may be returned by a PopProcessFunc to safely requeue
// the current item. The value of Err will be returned from Pop.
type ErrRequeue struct {
	// Err is returned by the Pop function
	Err error
}

// ErrFIFOClosed used when FIFO is closed
var ErrFIFOClosed = errors.New("DeltaFIFO: manipulating with closed queue")

func (e ErrRequeue) Error() string {
	if e.Err == nil {
		return "the popped item should be requeued without returning an error"
	}
	return e.Err.Error()
}

// Queue extends Store with a collection of Store keys to "process".
// Every Add, Update, or Delete may put the object's key in that collection.
// A Queue has a way to derive the corresponding key given an accumulator.
// A Queue can be accessed concurrently from multiple goroutines.
// A Queue can be "closed", after which Pop operations return an error.
type Queue interface {
	Store

	// Pop blocks until there is at least one key to process or the
	// Queue is closed.  In the latter case Pop returns with an error.
	// In the former case Pop atomically picks one key to process,
	// removes that (key, accumulator) association from the Store, and
	// processes the accumulator.  Pop returns the accumulator that
	// was processed and the result of processing.  The PopProcessFunc
	// may return an ErrRequeue{inner} and in this case Pop will (a)
	// return that (key, accumulator) association to the Queue as part
	// of the atomic processing and (b) return the inner error from
	// Pop.
	Pop(PopProcessFunc) (interface{}, error)

	// AddIfNotPresent puts the given accumulator into the Queue (in
	// association with the accumulator's key) if and only if that key
	// is not already associated with a non-empty accumulator.
	AddIfNotPresent(interface{}) error

	// HasSynced returns true if the first batch of keys have all been
	// popped.  The first batch of keys are those of the first Replace
	// operation if that happened before any Add, AddIfNotPresent,
	// Update, or Delete; otherwise the first batch is empty.
	HasSynced() bool

	// Close the queue
	Close()
}

// Pop is helper function for popping from Queue.
// WARNING: Do NOT use this function in non-test code to avoid races
// unless you really really really really know what you are doing.
func Pop(queue Queue) interface{} {
	var result interface{}
	queue.Pop(func(obj interface{}) error {
		result = obj
		return nil
	})
	return result
}

// FIFO is a Queue in which (a) each accumulator is simply the most
// recently provided object and (b) the collection of keys to process
// is a FIFO.  The accumulators all start out empty, and deleting an
// object from its accumulator empties the accumulator.  The Resync
// operation is a no-op.
//
// Thus: if multiple adds/updates of a single object happen while that
// object's key is in the queue before it has been processed then it
// will only be processed once, and when it is processed the most
// recent version will be processed. This can't be done with a channel
//
// FIFO solves this use case:
//  * You want to process every object (exactly) once.
//  * You want to process the most recent version of the object when you process it.
//  * You do not want to process deleted objects, they should be removed from the queue.
//  * You do not want to periodically reprocess objects.
// Compare with DeltaFIFO for other use cases.
type FIFO struct {
	lock sync.RWMutex
	cond sync.Cond
	// We depend on the property that every key in `items` is also in `queue`
	items map[string]interface{}
	queue []string

	// populated is true if the first batch of items inserted by Replace() has been populated
	// or Delete/Add/Update was called first.
	populated bool
	// initialPopulationCount is the number of items inserted by the first call of Replace()
	initialPopulationCount int

	// keyFunc is used to make the key used for queued item insertion and retrieval, and
	// should be deterministic.
	keyFunc KeyFunc

	// Indication the queue is closed.
	// Used to indicate a queue is closed so a control loop can exit when a queue is empty.
	// Currently, not used to gate any of CRED operations.
	closed bool
}

var (
	_ = Queue(&FIFO{}) // FIFO is a Queue
)

// Close the queue.
func (f *FIFO) Close() {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.closed = true
	f.cond.Broadcast()
}

// HasSynced returns true if an Add/Update/Delete/AddIfNotPresent are called first,
// or the first batch of items inserted by Replace() has been popped.

// HasSynced 如果先调用了 Add/Update/Delete/AddIfNotPresent，或者先调用了Update，但被 Replace() 插入的第一批元素已经被弹出，则 HasSynced 返回true。
func (f *FIFO) HasSynced() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.populated && f.initialPopulationCount == 0
}

// Add inserts an item, and puts it in the queue. The item is only enqueued
// if it doesn't already exist in the set.
func (f *FIFO) Add(obj interface{}) error {
	// Add 插入一个对象，将其放入队列中，只有当元素不在集合中时才会插入队列。
	id, err := f.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.populated = true
	// 元素不在队列中的时候才插入队列
	if _, exists := f.items[id]; !exists {
		f.queue = append(f.queue, id)
	}
	// items 是一个 map，所以直接赋值给这个 key，这样对更新元素也同样适用
	f.items[id] = obj
	f.cond.Broadcast()
	return nil
}

// AddIfNotPresent inserts an item, and puts it in the queue. If the item is already
// present in the set, it is neither enqueued nor added to the set.
//
// This is useful in a single producer/consumer scenario so that the consumer can
// safely retry items without contending with the producer and potentially enqueueing
// stale items.

// AddIfNotPresent 插入一个元素，将其放入队列中。
// 如果元素已经在集合中了，则会被忽略。
//
// 这在单个的 生产者/消费者 的场景下非常有用，这样消费者可以安全地重试
// 而不需要与生产者争夺，也不需要排队等待过时的元素。

func (f *FIFO) AddIfNotPresent(obj interface{}) error {
	id, err := f.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.addIfNotPresent(id, obj)
	return nil
}

// addIfNotPresent assumes the fifo lock is already held and adds the provided
// item to the queue under id if it does not already exist.

// addIfNotPresent 会假设已经持有 fifo 锁了，如果不存在，则将其添加到队列中去。
func (f *FIFO) addIfNotPresent(id string, obj interface{}) {
	f.populated = true
	if _, exists := f.items[id]; exists {
		return
	}

	f.queue = append(f.queue, id)
	f.items[id] = obj
	f.cond.Broadcast()
}

// Update is the same as Add in this implementation.
// 更新对象，实现非常简单，因为上面的 Add 方法就包含了 Update 的实现，因为 items 属性是一个 Map，对象有更新直接将对应 key 的 value 值替换成新的对象即可：
func (f *FIFO) Update(obj interface{}) error {
	return f.Add(obj)
}

// Delete removes an item. It doesn't add it to the queue, because
// this implementation assumes the consumer only cares about the objects,
// not the order in which they were created/added.
//接着就是删除 Delete 方法的实现，这里可能大家会有一个疑问，下面的删除实现只删除了 items 中的元素，
//那这样岂不是 queue 和 items 中的 key 会不一致吗？
//的确会这样，但是这是一个队列，下面的 Pop() 函数会根据 queue 里面的元素一个一个的弹出 key，没有对象就不处理了，相当于下面的 Pop() 函数中实现了 queue 的 key 的删除：
func (f *FIFO) Delete(obj interface{}) error {
	id, err := f.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.populated = true
	delete(f.items, id)
	return err
}

// List returns a list of all the items.
func (f *FIFO) List() []interface{} {
	f.lock.RLock()
	defer f.lock.RUnlock()
	list := make([]interface{}, 0, len(f.items))
	for _, item := range f.items {
		list = append(list, item)
	}
	return list
}

// ListKeys returns a list of all the keys of the objects currently
// in the FIFO.
func (f *FIFO) ListKeys() []string {
	f.lock.RLock()
	defer f.lock.RUnlock()
	list := make([]string, 0, len(f.items))
	for key := range f.items {
		list = append(list, key)
	}
	return list
}

// Get returns the requested item, or sets exists=false.
func (f *FIFO) Get(obj interface{}) (item interface{}, exists bool, err error) {
	key, err := f.keyFunc(obj)
	if err != nil {
		return nil, false, KeyError{obj, err}
	}
	return f.GetByKey(key)
}

// GetByKey returns the requested item, or sets exists=false.
func (f *FIFO) GetByKey(key string) (item interface{}, exists bool, err error) {
	f.lock.RLock()
	defer f.lock.RUnlock()
	item, exists = f.items[key]
	return item, exists, nil
}

// IsClosed checks if the queue is closed
func (f *FIFO) IsClosed() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.closed {
		return true
	}
	return false
}

// Pop waits until an item is ready and processes it. If multiple items are
// ready, they are returned in the order in which they were added/updated.
// The item is removed from the queue (and the store) before it is processed,
// so if you don't successfully process it, it should be added back with
// AddIfNotPresent(). process function is called under lock, so it is safe
// update data structures in it that need to be in sync with the queue.

// Pop 会等到一个元素准备好后再进行处理，如果有多个元素准备好了，则按照它们被添加或更新的顺序返回。
//
// 在处理之前，元素会从队列（和存储）中移除，所以如果没有成功处理，应该用 AddIfNotPresent() 函数把它添加回来。
// 处理函数是在有锁的情况下调用的，所以更新其中需要和队列同步的数据结构是安全的。

func (f *FIFO) Pop(process PopProcessFunc) (interface{}, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	for {
		for len(f.queue) == 0 {
			// When the queue is empty, invocation of Pop() is blocked until new item is enqueued.
			// When Close() is called, the f.closed is set and the condition is broadcasted.
			// Which causes this loop to continue and return from the Pop().
			if f.closed {
				return nil, ErrFIFOClosed
			}
			// 当队列为空时，Pop() 的调用会被阻塞住，直到新的元素插入队列后
			f.cond.Wait()
		}
		// 取出 queue 队列中的第一个元素（key）
		id := f.queue[0]
		// 删除第一个元素
		f.queue = f.queue[1:]
		if f.initialPopulationCount > 0 {
			f.initialPopulationCount--
		}
		// 获取被弹出的元素
		item, ok := f.items[id]
		if !ok {
			// Item may have been deleted subsequently.
			continue
		}
		// 删除弹出的元素
		delete(f.items, id)
		// 处理弹出的元素
		err := process(item)
		if e, ok := err.(ErrRequeue); ok {
			// 如果处理没成功，需要调用 addIfNotPresent 加回队列
			f.addIfNotPresent(id, item)
			err = e.Err
		}
		return item, err
	}
}

// Replace will delete the contents of 'f', using instead the given map.
// 'f' takes ownership of the map, you should not reference the map again
// after calling this function. f's queue is reset, too; upon return, it
// will contain the items in the map, in no particular order.

// Replace 将删除队列中的内容，'f' 拥有 map 的所有权，调用该函数过后，不应该再引用 map。
// 'f' 的队列也会被重置，返回时，队列将包含 map 中的元素，没有特定的顺序。
func (f *FIFO) Replace(list []interface{}, resourceVersion string) error {
	items := make(map[string]interface{}, len(list))
	for _, item := range list {
		key, err := f.keyFunc(item)
		if err != nil {
			return KeyError{item, err}
		}
		items[key] = item
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	if !f.populated {
		f.populated = true
		f.initialPopulationCount = len(items)
	}

	f.items = items
	f.queue = f.queue[:0]
	for id := range items {
		f.queue = append(f.queue, id)
	}
	if len(f.queue) > 0 {
		f.cond.Broadcast()
	}
	return nil
}

// Resync will ensure that every object in the Store has its key in the queue.
// This should be a no-op, because that property is maintained by all operations.

// Resync 会保证 Store 中的每个对象在 queue 中都有它的 key。
// 这应该是禁止操作的，因为该属性由所有操作维护

func (f *FIFO) Resync() error {
	f.lock.Lock()
	defer f.lock.Unlock()

	inQueue := sets.NewString()
	for _, id := range f.queue {
		inQueue.Insert(id)
	}
	for id := range f.items {
		if !inQueue.Has(id) {
			f.queue = append(f.queue, id)
		}
	}
	if len(f.queue) > 0 {
		f.cond.Broadcast()
	}
	return nil
}

// NewFIFO returns a Store which can be used to queue up items to
// process.
func NewFIFO(keyFunc KeyFunc) *FIFO {
	f := &FIFO{
		items:   map[string]interface{}{},
		queue:   []string{},
		keyFunc: keyFunc,
	}
	f.cond.L = &f.lock
	return f
}
