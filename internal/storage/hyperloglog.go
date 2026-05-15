// Package storage implements HyperLogLog probabilistic data structure.
// This implementation uses 14 bits of precision (16384 registers) matching Redis.
package storage

import (
	"hash/fnv"
	"math"
	"math/bits"
)

const (
	// hllPrecision is the number of bits used for register indexing (Redis uses 14)
	hllPrecision = 14
	// hllRegisters is 2^14 = 16384 registers
	hllRegisters = 1 << hllPrecision
	// hllAlpha is the bias correction factor for 16384 registers
	hllAlpha = 0.7213 / (1 + 1.079/float64(hllRegisters))
)

// HyperLogLog represents a HyperLogLog data structure
type HyperLogLog struct {
	registers []uint8
}

// NewHyperLogLog creates a new HyperLogLog with 16384 registers
func NewHyperLogLog() *HyperLogLog {
	return &HyperLogLog{
		registers: make([]uint8, hllRegisters),
	}
}

// HyperLogLogFromBytes deserializes a HyperLogLog from bytes
func HyperLogLogFromBytes(data []byte) *HyperLogLog {
	if len(data) != hllRegisters {
		// Invalid data, return empty HLL
		return NewHyperLogLog()
	}
	hll := &HyperLogLog{
		registers: make([]uint8, hllRegisters),
	}
	copy(hll.registers, data)
	return hll
}

// ToBytes serializes the HyperLogLog to bytes
func (hll *HyperLogLog) ToBytes() []byte {
	result := make([]byte, len(hll.registers))
	copy(result, hll.registers)
	return result
}

// murmur64 is a simple 64-bit hash based on MurmurHash3 finalizer
func murmur64(data []byte) uint64 {
	// Use FNV to get initial hash, then apply MurmurHash3 finalizer for better distribution
	h := fnv.New64a()
	h.Write(data)
	k := h.Sum64()

	// MurmurHash3 64-bit finalizer - excellent bit distribution
	k ^= k >> 33
	k *= 0xff51afd7ed558ccd
	k ^= k >> 33
	k *= 0xc4ceb9fe1a85ec53
	k ^= k >> 33

	return k
}

// Add adds an element to the HyperLogLog
// Returns true if the internal state changed (approximation)
func (hll *HyperLogLog) Add(element string) bool {
	hash := murmur64([]byte(element))

	// Use LOWER 14 bits as register index (better distribution after mixing)
	index := hash & (hllRegisters - 1)

	// Use UPPER bits for rank calculation
	// Count leading zeros in the upper 50 bits + 1
	upperBits := hash >> hllPrecision
	var rank uint8
	if upperBits == 0 {
		rank = uint8(64 - hllPrecision + 1) // Max rank
	} else {
		rank = uint8(bits.LeadingZeros64(upperBits) - hllPrecision + 1)
	}

	// Ensure rank is at least 1
	if rank < 1 {
		rank = 1
	}

	// Update register if new rank is higher
	if rank > hll.registers[index] {
		hll.registers[index] = rank
		return true
	}
	return false
}

// getRegisterFromHash extracts register index and rank from a hash
// Used internally for consistent hashing
func getRegisterFromHash(hash uint64) (index uint64, rank uint8) {
	index = hash & (hllRegisters - 1)
	upperBits := hash >> hllPrecision
	if upperBits == 0 {
		rank = uint8(64 - hllPrecision + 1)
	} else {
		// Count leading zeros in upperBits
		// Since upperBits is at most 50 bits, we count zeros after those leading 14 bits
		lz := bits.LeadingZeros64(upperBits)
		if lz > hllPrecision {
			rank = uint8(lz - hllPrecision + 1)
		} else {
			rank = 1
		}
	}
	if rank < 1 {
		rank = 1
	}
	return
}

// AddHash adds a pre-computed hash to the HyperLogLog
func (hll *HyperLogLog) AddHash(hash uint64) bool {
	index, rank := getRegisterFromHash(hash)
	if rank > hll.registers[index] {
		hll.registers[index] = rank
		return true
	}
	return false
}

// Count returns the estimated cardinality
func (hll *HyperLogLog) Count() int64 {
	// Calculate harmonic mean of 2^(-register[i])
	sum := 0.0
	zeros := 0

	for _, val := range hll.registers {
		sum += math.Pow(2, -float64(val))
		if val == 0 {
			zeros++
		}
	}

	// Raw estimate
	estimate := hllAlpha * float64(hllRegisters) * float64(hllRegisters) / sum

	// Apply corrections for small and large cardinalities
	if estimate <= 2.5*float64(hllRegisters) {
		// Small range correction
		if zeros > 0 {
			// Linear counting for small cardinalities
			estimate = float64(hllRegisters) * math.Log(float64(hllRegisters)/float64(zeros))
		}
	} else if estimate > (1.0/30.0)*math.Pow(2, 32) {
		// Large range correction (for 32-bit hash)
		// Not needed with 64-bit hash, but kept for completeness
		estimate = -math.Pow(2, 32) * math.Log(1-estimate/math.Pow(2, 32))
	}

	return int64(math.Round(estimate))
}

// Merge merges another HyperLogLog into this one
func (hll *HyperLogLog) Merge(other *HyperLogLog) {
	for i := range hll.registers {
		if other.registers[i] > hll.registers[i] {
			hll.registers[i] = other.registers[i]
		}
	}
}

// IsEmpty returns true if the HyperLogLog has no data
func (hll *HyperLogLog) IsEmpty() bool {
	for _, v := range hll.registers {
		if v > 0 {
			return false
		}
	}
	return true
}
