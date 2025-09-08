// Package diamantepos provides fixed-point arithmetic for deterministic calculations
package diamantepos

import (
	"math/big"
)

// FixedPoint represents a fixed-point decimal number with specified precision
// All consensus calculations MUST use this instead of float64 to ensure determinism
type FixedPoint struct {
	value     *big.Int // Actual value multiplied by scale
	precision uint     // Number of decimal places
	scale     *big.Int // 10^precision
}

const (
	// DefaultPrecision is the default number of decimal places (6 = parts per million)
	DefaultPrecision = 6
	// MaxPrecision is the maximum supported precision
	MaxPrecision = 18
)

// NewFixedPoint creates a new fixed-point number from an integer
func NewFixedPoint(value int64, precision uint) *FixedPoint {
	if precision > MaxPrecision {
		precision = MaxPrecision
	}

	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(precision)), nil)
	scaledValue := new(big.Int).Mul(big.NewInt(value), scale)

	return &FixedPoint{
		value:     scaledValue,
		precision: precision,
		scale:     scale,
	}
}

// NewFixedPointFromUint64 creates a new fixed-point number from uint64
func NewFixedPointFromUint64(value uint64, precision uint) *FixedPoint {
	if precision > MaxPrecision {
		precision = MaxPrecision
	}

	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(precision)), nil)
	scaledValue := new(big.Int).Mul(new(big.Int).SetUint64(value), scale)

	return &FixedPoint{
		value:     scaledValue,
		precision: precision,
		scale:     scale,
	}
}

// NewFixedPointFromRatio creates a fixed-point number from a ratio (numerator/denominator)
func NewFixedPointFromRatio(numerator, denominator uint64, precision uint) *FixedPoint {
	if precision > MaxPrecision {
		precision = MaxPrecision
	}
	if denominator == 0 {
		return NewFixedPoint(0, precision)
	}

	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(precision)), nil)

	// Calculate (numerator * scale) / denominator
	num := new(big.Int).SetUint64(numerator)
	den := new(big.Int).SetUint64(denominator)
	scaledNum := new(big.Int).Mul(num, scale)
	result := new(big.Int).Div(scaledNum, den)

	return &FixedPoint{
		value:     result,
		precision: precision,
		scale:     scale,
	}
}

// NewFixedPointFromFloat64 creates from float64 with specified precision
func NewFixedPointFromFloat64(value float64, precision uint) *FixedPoint {
	if precision > MaxPrecision {
		precision = MaxPrecision
	}

	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(precision)), nil)

	// Convert float to scaled integer
	scaledFloat := value * float64(scale.Int64())
	scaledValue := new(big.Int).SetInt64(int64(scaledFloat))

	return &FixedPoint{
		value:     scaledValue,
		precision: precision,
		scale:     scale,
	}
}

// Mul multiplies two fixed-point numbers
func (fp *FixedPoint) Mul(other *FixedPoint) *FixedPoint {
	// Ensure same precision
	if fp.precision != other.precision {
		other = other.ToPrecision(fp.precision)
	}

	// Multiply values and divide by scale to maintain precision
	result := new(big.Int).Mul(fp.value, other.value)
	result.Div(result, fp.scale)

	return &FixedPoint{
		value:     result,
		precision: fp.precision,
		scale:     new(big.Int).Set(fp.scale),
	}
}

// Compare returns -1 if fp < other, 0 if equal, 1 if fp > other
func (fp *FixedPoint) Compare(other *FixedPoint) int {
	// Ensure same precision
	if fp.precision != other.precision {
		other = other.ToPrecision(fp.precision)
	}

	return fp.value.Cmp(other.value)
}

// ToPrecision converts a fixed-point number to a different precision
func (fp *FixedPoint) ToPrecision(newPrecision uint) *FixedPoint {
	if newPrecision > MaxPrecision {
		newPrecision = MaxPrecision
	}

	if newPrecision == fp.precision {
		return &FixedPoint{
			value:     new(big.Int).Set(fp.value),
			precision: fp.precision,
			scale:     new(big.Int).Set(fp.scale),
		}
	}

	newScale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(newPrecision)), nil)

	if newPrecision > fp.precision {
		// Increasing precision - multiply by difference
		diff := newPrecision - fp.precision
		multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(diff)), nil)
		newValue := new(big.Int).Mul(fp.value, multiplier)
		return &FixedPoint{
			value:     newValue,
			precision: newPrecision,
			scale:     newScale,
		}
	} else {
		// Decreasing precision - divide by difference
		diff := fp.precision - newPrecision
		divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(diff)), nil)
		newValue := new(big.Int).Div(fp.value, divisor)
		return &FixedPoint{
			value:     newValue,
			precision: newPrecision,
			scale:     newScale,
		}
	}
}
