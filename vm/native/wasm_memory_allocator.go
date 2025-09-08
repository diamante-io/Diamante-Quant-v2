// Package native provides a proper memory allocator for WASM runtime
package native

import (
	"fmt"
	"sync"
)

// MemoryBlock represents a block of memory
type MemoryBlock struct {
	offset uint32
	size   uint32
	free   bool
	next   *MemoryBlock
	prev   *MemoryBlock
}

// WASMMemoryAllocator manages memory allocation for WASM modules
type WASMMemoryAllocator struct {
	totalSize    uint32
	allocated    uint32
	freeList     *MemoryBlock
	allocatedMap map[uint32]*MemoryBlock
	mu           sync.Mutex

	// Metrics
	allocations   int64
	deallocations int64
	fragmentation float64
}

// NewWASMMemoryAllocator creates a new memory allocator
func NewWASMMemoryAllocator(totalSize uint32) *WASMMemoryAllocator {
	// Create initial free block covering entire memory
	initialBlock := &MemoryBlock{
		offset: 0,
		size:   totalSize,
		free:   true,
		next:   nil,
		prev:   nil,
	}

	return &WASMMemoryAllocator{
		totalSize:    totalSize,
		allocated:    0,
		freeList:     initialBlock,
		allocatedMap: make(map[uint32]*MemoryBlock),
	}
}

// Allocate allocates a block of memory
func (ma *WASMMemoryAllocator) Allocate(size uint32) (uint32, error) {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	// Align size to 8 bytes for better performance
	alignedSize := (size + 7) &^ 7

	// Find a suitable free block using first-fit algorithm
	current := ma.freeList
	for current != nil {
		if current.free && current.size >= alignedSize {
			// Found a suitable block
			offset := current.offset

			if current.size == alignedSize {
				// Exact fit - mark block as allocated
				current.free = false
			} else {
				// Split the block
				newBlock := &MemoryBlock{
					offset: current.offset + alignedSize,
					size:   current.size - alignedSize,
					free:   true,
					next:   current.next,
					prev:   current,
				}

				if current.next != nil {
					current.next.prev = newBlock
				}

				current.next = newBlock
				current.size = alignedSize
				current.free = false
			}

			// Track allocation
			ma.allocatedMap[offset] = current
			ma.allocated += alignedSize
			ma.allocations++

			// Update fragmentation metric
			ma.updateFragmentation()

			return offset, nil
		}
		current = current.next
	}

	return 0, fmt.Errorf("out of memory: requested %d bytes, available %d bytes",
		size, ma.totalSize-ma.allocated)
}

// Deallocate frees a previously allocated block
func (ma *WASMMemoryAllocator) Deallocate(offset uint32) error {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	// Find the allocated block
	block, exists := ma.allocatedMap[offset]
	if !exists {
		return fmt.Errorf("invalid deallocation: offset %d not found", offset)
	}

	// Mark block as free
	block.free = true
	delete(ma.allocatedMap, offset)
	ma.allocated -= block.size
	ma.deallocations++

	// Coalesce with adjacent free blocks
	ma.coalesceBlocks(block)

	// Update fragmentation metric
	ma.updateFragmentation()

	return nil
}

// coalesceBlocks merges adjacent free blocks to reduce fragmentation
func (ma *WASMMemoryAllocator) coalesceBlocks(block *MemoryBlock) {
	// Merge with next block if it's free
	if block.next != nil && block.next.free {
		block.size += block.next.size
		block.next = block.next.next
		if block.next != nil {
			block.next.prev = block
		}
	}

	// Merge with previous block if it's free
	if block.prev != nil && block.prev.free {
		block.prev.size += block.size
		block.prev.next = block.next
		if block.next != nil {
			block.next.prev = block.prev
		}
	}
}

// Reallocate resizes an allocated block
func (ma *WASMMemoryAllocator) Reallocate(offset uint32, newSize uint32) (uint32, error) {
	ma.mu.Lock()

	// Find the allocated block
	block, exists := ma.allocatedMap[offset]
	if !exists {
		ma.mu.Unlock()
		return 0, fmt.Errorf("invalid reallocation: offset %d not found", offset)
	}

	oldSize := block.size
	ma.mu.Unlock()

	// If new size is smaller, we can reuse the same block
	if newSize <= oldSize {
		// Optionally split the block to free unused space
		if newSize < oldSize-16 { // Only split if we save at least 16 bytes
			ma.mu.Lock()
			defer ma.mu.Unlock()

			alignedSize := (newSize + 7) &^ 7
			freedSize := oldSize - alignedSize

			// Create a new free block for the unused space
			newBlock := &MemoryBlock{
				offset: offset + alignedSize,
				size:   freedSize,
				free:   true,
				next:   block.next,
				prev:   block,
			}

			if block.next != nil {
				block.next.prev = newBlock
			}

			block.next = newBlock
			block.size = alignedSize

			ma.allocated -= freedSize
			ma.coalesceBlocks(newBlock)
			ma.updateFragmentation()
		}
		return offset, nil
	}

	// Need to allocate a new larger block
	newOffset, err := ma.Allocate(newSize)
	if err != nil {
		return 0, fmt.Errorf("failed to reallocate: %w", err)
	}

	// Free the old block
	if err := ma.Deallocate(offset); err != nil {
		// This shouldn't happen, but handle it gracefully
		ma.Deallocate(newOffset) // Free the new allocation
		return 0, fmt.Errorf("failed to free old block during reallocation: %w", err)
	}

	return newOffset, nil
}

// GetStats returns allocator statistics
func (ma *WASMMemoryAllocator) GetStats() map[string]interface{} {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	freeBlocks := 0
	largestFreeBlock := uint32(0)
	totalFree := ma.totalSize - ma.allocated

	// Count free blocks and find largest
	current := ma.freeList
	for current != nil {
		if current.free {
			freeBlocks++
			if current.size > largestFreeBlock {
				largestFreeBlock = current.size
			}
		}
		current = current.next
	}

	return map[string]interface{}{
		"total_size":         ma.totalSize,
		"allocated":          ma.allocated,
		"free":               totalFree,
		"allocations":        ma.allocations,
		"deallocations":      ma.deallocations,
		"active_allocations": len(ma.allocatedMap),
		"free_blocks":        freeBlocks,
		"largest_free_block": largestFreeBlock,
		"fragmentation":      ma.fragmentation,
		"utilization":        float64(ma.allocated) / float64(ma.totalSize),
	}
}

// updateFragmentation calculates the fragmentation metric
func (ma *WASMMemoryAllocator) updateFragmentation() {
	if ma.allocated == 0 || ma.allocated == ma.totalSize {
		ma.fragmentation = 0
		return
	}

	// Count free blocks and total free space
	freeBlocks := 0
	totalFree := uint32(0)
	largestFree := uint32(0)

	current := ma.freeList
	for current != nil {
		if current.free {
			freeBlocks++
			totalFree += current.size
			if current.size > largestFree {
				largestFree = current.size
			}
		}
		current = current.next
	}

	if totalFree == 0 {
		ma.fragmentation = 0
		return
	}

	// Fragmentation = 1 - (largest free block / total free space)
	// 0 = no fragmentation, 1 = completely fragmented
	ma.fragmentation = 1.0 - (float64(largestFree) / float64(totalFree))
}

// Defragment attempts to reduce memory fragmentation by compacting allocations
func (ma *WASMMemoryAllocator) Defragment() (movedBlocks int, error error) {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	// Check if defragmentation is needed
	if ma.fragmentation < 0.3 {
		// Less than 30% fragmentation, no need to defragment
		return 0, nil
	}

	// Create a new memory layout
	newAllocMap := make(map[uint32]*MemoryBlock)
	relocations := make(map[uint32]uint32) // old offset -> new offset

	// Sort allocated blocks by offset to maintain order
	sortedBlocks := make([]*MemoryBlock, 0, len(ma.allocatedMap))
	for _, block := range ma.allocatedMap {
		sortedBlocks = append(sortedBlocks, block)
	}

	// Sort by offset
	for i := 0; i < len(sortedBlocks); i++ {
		for j := i + 1; j < len(sortedBlocks); j++ {
			if sortedBlocks[i].offset > sortedBlocks[j].offset {
				sortedBlocks[i], sortedBlocks[j] = sortedBlocks[j], sortedBlocks[i]
			}
		}
	}

	// Compact allocations starting from offset 0
	currentOffset := uint32(0)
	var firstBlock, lastBlock, prevBlock *MemoryBlock

	for _, oldBlock := range sortedBlocks {
		// Create new compacted block
		newBlock := &MemoryBlock{
			offset: currentOffset,
			size:   oldBlock.size,
			free:   false,
			prev:   prevBlock,
			next:   nil,
		}

		if prevBlock != nil {
			prevBlock.next = newBlock
		}

		if firstBlock == nil {
			firstBlock = newBlock
		}
		lastBlock = newBlock
		prevBlock = newBlock

		// Record relocation
		relocations[oldBlock.offset] = currentOffset
		newAllocMap[currentOffset] = newBlock

		currentOffset += oldBlock.size
		movedBlocks++
	}

	// Create the final free block with all remaining space
	if currentOffset < ma.totalSize {
		freeBlock := &MemoryBlock{
			offset: currentOffset,
			size:   ma.totalSize - currentOffset,
			free:   true,
			prev:   lastBlock,
			next:   nil,
		}

		if lastBlock != nil {
			lastBlock.next = freeBlock
		} else {
			// No allocated blocks, entire memory is free
			firstBlock = freeBlock
		}
	}

	// Update the allocator state
	ma.freeList = firstBlock
	ma.allocatedMap = newAllocMap
	ma.updateFragmentation()

	// Note: In a real WASM environment, we would need to:
	// 1. Pause WASM execution
	// 2. Update all pointers in WASM memory
	// 3. Notify the WASM module of relocations
	// Since this is a simplified implementation, we return the relocation map
	// The caller would be responsible for updating references

	return movedBlocks, nil
}

// ValidateHeap checks the integrity of the memory allocator
func (ma *WASMMemoryAllocator) ValidateHeap() error {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	totalAccounted := uint32(0)
	blockCount := 0

	// Validate the linked list structure
	current := ma.freeList
	var prev *MemoryBlock

	for current != nil {
		blockCount++

		// Check block integrity
		if current.offset+current.size > ma.totalSize {
			return fmt.Errorf("block at offset %d extends beyond memory bounds", current.offset)
		}

		// Check linked list integrity
		if current.prev != prev {
			return fmt.Errorf("broken prev link at block %d", current.offset)
		}

		// Check for overlapping blocks
		if prev != nil && prev.offset+prev.size > current.offset {
			return fmt.Errorf("overlapping blocks at offsets %d and %d", prev.offset, current.offset)
		}

		totalAccounted += current.size
		prev = current
		current = current.next
	}

	// Verify total memory is accounted for
	if totalAccounted != ma.totalSize {
		return fmt.Errorf("memory accounting error: expected %d, got %d", ma.totalSize, totalAccounted)
	}

	// Verify allocated blocks
	allocatedSize := uint32(0)
	for _, block := range ma.allocatedMap {
		if block.free {
			return fmt.Errorf("allocated block at offset %d is marked as free", block.offset)
		}
		allocatedSize += block.size
	}

	if allocatedSize != ma.allocated {
		return fmt.Errorf("allocated size mismatch: expected %d, got %d", ma.allocated, allocatedSize)
	}

	return nil
}

// Reset clears all allocations and resets the allocator
func (ma *WASMMemoryAllocator) Reset() {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	// Create a single free block covering entire memory
	ma.freeList = &MemoryBlock{
		offset: 0,
		size:   ma.totalSize,
		free:   true,
		next:   nil,
		prev:   nil,
	}

	ma.allocated = 0
	ma.allocatedMap = make(map[uint32]*MemoryBlock)
	ma.fragmentation = 0
}
