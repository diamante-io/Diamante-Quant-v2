// Package consensus provides fixed-point arithmetic for deterministic calculations
package consensus

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

// Div divides one fixed-point number by another
func (fp *FixedPoint) Div(other *FixedPoint) *FixedPoint {
	if other.value.Sign() == 0 {
		// Division by zero returns zero
		return NewFixedPoint(0, fp.precision)
	}

	// Ensure same precision
	if fp.precision != other.precision {
		other = other.ToPrecision(fp.precision)
	}

	// Multiply by scale before division to maintain precision
	result := new(big.Int).Mul(fp.value, fp.scale)
	result.Div(result, other.value)

	return &FixedPoint{
		value:     result,
		precision: fp.precision,
		scale:     new(big.Int).Set(fp.scale),
	}
}

// Add adds two fixed-point numbers
func (fp *FixedPoint) Add(other *FixedPoint) *FixedPoint {
	// Ensure same precision
	if fp.precision != other.precision {
		other = other.ToPrecision(fp.precision)
	}

	result := new(big.Int).Add(fp.value, other.value)

	return &FixedPoint{
		value:     result,
		precision: fp.precision,
		scale:     new(big.Int).Set(fp.scale),
	}
}

// Sub subtracts one fixed-point number from another
func (fp *FixedPoint) Sub(other *FixedPoint) *FixedPoint {
	// Ensure same precision
	if fp.precision != other.precision {
		other = other.ToPrecision(fp.precision)
	}

	result := new(big.Int).Sub(fp.value, other.value)

	return &FixedPoint{
		value:     result,
		precision: fp.precision,
		scale:     new(big.Int).Set(fp.scale),
	}
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

// ToUint64 converts fixed-point to uint64 (truncates decimal places)
func (fp *FixedPoint) ToUint64() uint64 {
	result := new(big.Int).Div(fp.value, fp.scale)
	return result.Uint64()
}

// ToInt64 converts fixed-point to int64 (truncates decimal places)
func (fp *FixedPoint) ToInt64() int64 {
	result := new(big.Int).Div(fp.value, fp.scale)
	return result.Int64()
}

// MulUint64 multiplies a fixed-point number by a uint64
func (fp *FixedPoint) MulUint64(val uint64) *FixedPoint {
	result := new(big.Int).Mul(fp.value, new(big.Int).SetUint64(val))

	return &FixedPoint{
		value:     result,
		precision: fp.precision,
		scale:     new(big.Int).Set(fp.scale),
	}
}

// DivUint64 divides a fixed-point number by a uint64
func (fp *FixedPoint) DivUint64(val uint64) *FixedPoint {
	if val == 0 {
		return NewFixedPoint(0, fp.precision)
	}

	result := new(big.Int).Div(fp.value, new(big.Int).SetUint64(val))

	return &FixedPoint{
		value:     result,
		precision: fp.precision,
		scale:     new(big.Int).Set(fp.scale),
	}
}

// IsZero returns true if the value is zero
func (fp *FixedPoint) IsZero() bool {
	return fp.value.Sign() == 0
}

// IsNegative returns true if the value is negative
func (fp *FixedPoint) IsNegative() bool {
	return fp.value.Sign() < 0
}

// IsPositive returns true if the value is positive
func (fp *FixedPoint) IsPositive() bool {
	return fp.value.Sign() > 0
}

// Compare returns -1 if fp < other, 0 if equal, 1 if fp > other
func (fp *FixedPoint) Compare(other *FixedPoint) int {
	// Ensure same precision
	if fp.precision != other.precision {
		other = other.ToPrecision(fp.precision)
	}

	return fp.value.Cmp(other.value)
}

// String returns a string representation of the fixed-point number
func (fp *FixedPoint) String() string {
	// Get the string representation of the absolute value
	absValue := new(big.Int).Abs(fp.value)
	str := absValue.String()

	// Pad with zeros if necessary
	for len(str) <= int(fp.precision) {
		str = "0" + str
	}

	// Insert decimal point
	intPart := str[:len(str)-int(fp.precision)]
	fracPart := str[len(str)-int(fp.precision):]

	// Remove trailing zeros from fractional part
	fracPart = trimTrailingZeros(fracPart)

	result := intPart
	if fracPart != "" {
		result = intPart + "." + fracPart
	}

	// Add negative sign if needed
	if fp.value.Sign() < 0 {
		result = "-" + result
	}

	return result
}

func trimTrailingZeros(s string) string {
	for len(s) > 0 && s[len(s)-1] == '0' {
		s = s[:len(s)-1]
	}
	return s
}

// Percentage creates a fixed-point percentage (e.g., 5 = 5%)
func Percentage(percent int64) *FixedPoint {
	// Convert percentage to decimal (5% = 0.05)
	return NewFixedPointFromRatio(uint64(percent), 100, DefaultPrecision)
}

// PerMillage creates a fixed-point per-millage (e.g., 5 = 0.5%)
func PerMillage(perMille int64) *FixedPoint {
	// Convert per-millage to decimal (5‰ = 0.005)
	return NewFixedPointFromRatio(uint64(perMille), 1000, DefaultPrecision)
}
