package timingwheel

import (
	"testing"
)

func TestNew(t *testing.T) {
	tw := New[int]()
	if tw.current != 0 {
		t.Errorf("current = %d, want 0", tw.current)
	}
	if len(tw.slots) != 16 {
		t.Errorf("len(slots) = %d, want 16", len(tw.slots))
	}
}

func TestAdd(t *testing.T) {
	tw := New[int]()

	tw.Add(1, 1)
	if len(tw.slots[0]) != 0 {
		t.Errorf("len(slots[0]) = %d, want 0", len(tw.slots[0]))
	}
	if len(tw.slots[1]) != 1 {
		t.Errorf("len(slots[1]) = %d, want 1", len(tw.slots[1]))
	}

	tw.Add(2, 16)
	if len(tw.next.slots[0]) != 1 {
		t.Errorf("len(next.slots[0]) = %d, want 1 %v", len(tw.next.slots[0]), tw.next)
	}
	if tw.next.slots[0][0].delay != 0 {
		t.Errorf("next.slots[0][0].delay = %d, want 0", tw.next.slots[0][0].delay)
	}
	if tw.next.slots[0][0].value != 2 {
		t.Errorf("next.slots[0][0].Value = %d, want 2", tw.next.slots[0][0].value)
	}
	tw.Add(2, 17)
	if len(tw.next.slots[0]) != 2 {
		t.Errorf("len(next.slots[0]) = %d, want 2", len(tw.next.slots[0]))
	}
	if tw.next.slots[0][1].delay != 1 {
		t.Errorf("next.slots[0][1].delay = %d, want 1", tw.next.slots[0][1].delay)
	}
}

func TestAdvance(t *testing.T) {
	tw := New[int]()

	total := 0
	add := func(value int) {
		total += value
	}

	tw.Add(2, 0)
	tw.Add(3, 1)
	tw.Add(5, 1)
	tw.Add(7, 2)
	tw.Add(11, 3)
	tw.Add(13, 4)

	tw.Advance(add)
	if total != 2 { // sum(2)
		t.Errorf("total = %d, want 2", total)
	}

	tw.Advance(add)
	if total != 10 { // sum(2, 3, 5)
		t.Errorf("total = %d, want 10", total)
	}

	tw.Add(1, 0) // Add an extra 1 to next slot
	tw.Add(2, 2) // Add an extra 2 to 2 slots away
	tw.Advance(add)
	if total != 18 { // sum(2, 3, 5, 7, 1)
		t.Errorf("total = %d, want 18", total)
	}

	tw.Advance(add)
	if total != 29 { // sum(2, 3, 5, 7, 1, 11)
		t.Errorf("total = %d, want 29", total)
	}

	tw.Advance(add)
	if total != 44 { // sum(2, 3, 5, 7, 1, 11, 13, 2)
		t.Errorf("total = %d, want 44", total)
	}
}

func TestLoop(t *testing.T) {
	tw := New[int]()

	total := 0
	add := func(value int) {
		total += value
	}

	for i := 0; i < 12345; i++ {
		tw.Add(1, i)
	}

	for i := 0; i < 12345; i++ {
		tw.Advance(add)
		if total != i+1 {
			t.Errorf("total = %d, want %d", total, i)
			break
		}
	}
}

func TestMultilevelAdvance(t *testing.T) {
	tw := New[int]()

	total := 0
	add := func(value int) {
		total += value
	}

	tw.Add(1, 256)

	if tw.next.next == nil {
		t.Errorf("next.next = nil, want non-nil")
	}
	nested := tw.next.next
	if len(nested.slots[0]) != 1 {
		t.Errorf("len(nested.slots[0]) = %d, want 1", len(nested.slots[0]))
	}
	t.Logf("%#v", nested)

	for i := 0; i < 241; i++ {
		tw.Advance(add)
	}

	if total != 0 {
		t.Errorf("total = %d, want 0", total)
	}
}

func TestMasks(t *testing.T) {
	tw := New[int]()
	tw.Add(1, 1234567890)
	if tw.next.mask != 0b11110000 {
		t.Errorf("mask = %b, want 0b11110000", tw.next.mask)
	}
	if tw.next.submask != 0b1111 {
		t.Errorf("submask = %b, want 0b1111", tw.next.submask)
	}
	if tw.next.max != 255 {
		t.Errorf("max = %d, want 255", tw.next.max)
	}
	if tw.next.next.mask != 0b111100000000 {
		t.Errorf("mask = %b, want 0b111100000000", tw.next.next.mask)
	}
	if tw.next.next.submask != 0b11111111 {
		t.Errorf("submask = %b, want 0b11111111", tw.next.next.submask)
	}
	if tw.next.next.max != 4095 {
		t.Errorf("max = %d, want 4095", tw.next.next.max)
	}
}

var result int

// Basic benchmark of repeated add+advance, where N is the delay for each added item.
// N effectively sets the 'working set' size of the timing wheel.
func benchmarkAddAdvance(b *testing.B, n int) {
	tw := New[int]()
	record := func(r int) { result = r }

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tw.Add(1, n)
		tw.Advance(record)
	}
	b.StopTimer()
	tw.Advance(func(i int) { result = i })
}

func BenchmarkAddAdvance1(b *testing.B) {
	benchmarkAddAdvance(b, 1)
}
func BenchmarkAddAdvance10K(b *testing.B) {
	benchmarkAddAdvance(b, 10000)
}
func BenchmarkAddAdvance1M(b *testing.B) {
	benchmarkAddAdvance(b, 1000000)
}
func BenchmarkAddAdvance10M(b *testing.B) {
	benchmarkAddAdvance(b, 10000000)
}
func BenchmarkAddAdvance100M(b *testing.B) {
	benchmarkAddAdvance(b, 100000000)
}
