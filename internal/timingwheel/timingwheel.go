package timingwheel

const (
	bitWidth  = 4
	slotCount = 1 << bitWidth
	baseMask  = slotCount - 1
)

// TimingWheel is a hierarchical timing wheel, where abstract timer durations
// are stored in a hierarchy of wheels. For a given resolution, the top timer
// wheel holds 16 slots each of the resolution (e.g. 16 seconds for a 1 second
// resolution) and then subsequent wheels hold successively lower resolutions
// (e.g. 16 slots of 16 seconds, then 16 slots of 256 seconds, etc). When the
// first wheel completes a full revolution, it advances the next wheel, which
// may in turn advance the next wheel, and so on. Higher level wheels reinsert
// their items with their remaining un-counted duration.
//
// TimingWheel makes no assumptions about how the time will be managed. Items
// are added via Add, and the wheel is advanced by calling Advance. The handler
// is responsible for converting user time to an appropriate multiple of the
// desired resolution, and then for calling Advance appropriately to poll the
// wheel.
type TimingWheel[T any] struct {
	slots   [][]T
	current int
	next    subTimingWheel[T]
}

type subTimingWheel[T any] struct {
	slots   [][]item[T]
	current int

	max int

	// The mask is used to calculate the slot for a given delay,
	// which can be converted to a slot by right shifting.
	mask  int
	shift int

	// The submask is used to retain only the lower-bucket bits
	// of the delay.
	submask int

	next *subTimingWheel[T]
}

type item[T any] struct {
	value T
	delay int
}

func New[T any]() *TimingWheel[T] {
	return &TimingWheel[T]{
		slots:   make([][]T, slotCount),
		current: 0,
		next:    *newSubWheel[T](baseMask, baseMask, 0),
	}
}

func newSubWheel[T any](oldMax, oldMask, oldShift int) *subTimingWheel[T] {
	return &subTimingWheel[T]{
		slots:   make([][]item[T], slotCount),
		current: 0,
		max:     (oldMax << bitWidth) | baseMask,
		mask:    oldMask << bitWidth,
		shift:   oldShift + bitWidth,
		submask: oldMax,
		next:    nil,
	}
}

// Add an item to the timing wheel with a given delay. The delay is given as a
// multiple of the resolution of the wheel.
func (tw *TimingWheel[T]) Add(value T, delay int) {
	if delay > baseMask {
		tw.next.addItem(item[T]{value, delay})
	} else {
		slot := (tw.current + delay) % slotCount
		tw.slots[slot] = append(tw.slots[slot], value)
	}
}

func (tw *subTimingWheel[T]) addItem(item item[T]) {
	if item.delay > tw.max {
		if tw.next == nil {
			tw.next = newSubWheel[T](tw.max, tw.mask, tw.shift)
		}
		tw.next.addItem(item)
	} else {
		slot := ((item.delay & tw.mask) >> tw.shift) - 1
		slot = (tw.current + slot) % slotCount
		item.delay &= tw.submask
		tw.slots[slot] = append(tw.slots[slot], item)
	}
}

// Advance the timing wheel by one slot. The handler is called for each item
// in the current slot.
func (tw *TimingWheel[T]) Advance(handler func(T)) {
	items := tw.slots[tw.current]
	tw.slots[tw.current] = nil
	for i := 0; i < len(items); i++ {
		handler(items[i])
	}

	tw.current++
	if tw.current >= slotCount {
		tw.current = 0
		tw.next.advanceItem(tw)
	}
}

// Handler for subwheels. This is largely the same as the top-level handler,
// except it passes through the root Wheel so that items can be re-inserted
func (tw *subTimingWheel[T]) advanceItem(root *TimingWheel[T]) {
	items := tw.slots[tw.current]
	tw.slots[tw.current] = nil

	// Re-insert items back into the root wheel
	for i := 0; i < len(items); i++ {
		root.Add(items[i].value, items[i].delay)
	}

	// Advance the current slot. If we wrap around, advance the next wheel as well.
	tw.current++
	if tw.current >= slotCount {
		tw.current = 0
		if tw.next != nil {
			tw.next.advanceItem(root)
		}
	}
}
