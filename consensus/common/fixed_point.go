// consensus/common/fixed_point.go

package common

import (
	"fmt"
	"math"
	"math/big"
)

// FixedPoint represents a fixed-point decimal number with 6 decimal places precision
// This provides deterministic arithmetic for consensus operations
type FixedPoint struct {
	// Value stored as integer with 6 decimal places
	// e.g., 1.5 is stored as 1500000
	value int64
}

const (
	// Precision defines the number of decimal places
	Precision = 6
	// ScaleFactor is 10^Precision
	ScaleFactor = 1000000
	// MaxValue is the maximum value that can be represented
	MaxValue = math.MaxInt64 / ScaleFactor
	// MinValue is the minimum value that can be represented
	MinValue = math.MinInt64 / ScaleFactor
)

// NewFixedPoint creates a new FixedPoint from an integer value
func NewFixedPoint(value int64) FixedPoint {
	return FixedPoint{value: value * ScaleFactor}
}

// NewFixedPointFromRaw creates a FixedPoint from a raw scaled value
func NewFixedPointFromRaw(rawValue int64) FixedPoint {
	return FixedPoint{value: rawValue}
}

// NewFixedPointFromFraction creates a FixedPoint from a fraction
func NewFixedPointFromFraction(numerator, denominator int64) (FixedPoint, error) {
	if denominator == 0 {
		return FixedPoint{}, fmt.Errorf("division by zero")
	}

	// Use big.Int to avoid overflow during multiplication
	num := big.NewInt(numerator)
	num.Mul(num, big.NewInt(ScaleFactor))
	num.Div(num, big.NewInt(denominator))

	if !num.IsInt64() {
		return FixedPoint{}, fmt.Errorf("result overflow")
	}

	return FixedPoint{value: num.Int64()}, nil
}

// NewFixedPointFromPercentage creates a FixedPoint from a percentage (0-100)
func NewFixedPointFromPercentage(percentage int64) (FixedPoint, error) {
	if percentage < 0 || percentage > 100 {
		return FixedPoint{}, fmt.Errorf("percentage must be between 0 and 100")
	}
	return NewFixedPointFromFraction(percentage, 100)
}

// NewFixedPointFromBasisPoints creates a FixedPoint from basis points (0-10000)
func NewFixedPointFromBasisPoints(basisPoints int64) (FixedPoint, error) {
	if basisPoints < 0 || basisPoints > 10000 {
		return FixedPoint{}, fmt.Errorf("basis points must be between 0 and 10000")
	}
	return NewFixedPointFromFraction(basisPoints, 10000)
}

// Int64 returns the integer part of the fixed-point number
func (fp FixedPoint) Int64() int64 {
	return fp.value / ScaleFactor
}

// Raw returns the raw scaled value
func (fp FixedPoint) Raw() int64 {
	return fp.value
}

// String returns a string representation of the fixed-point number
func (fp FixedPoint) String() string {
	integerPart := fp.value / ScaleFactor
	fractionalPart := fp.value % ScaleFactor
	if fractionalPart < 0 {
		fractionalPart = -fractionalPart
	}
	return fmt.Sprintf("%d.%06d", integerPart, fractionalPart)
}

// Add adds two fixed-point numbers with overflow checking
func (fp FixedPoint) Add(other FixedPoint) (FixedPoint, error) {
	// Check for overflow
	if (fp.value > 0 && other.value > math.MaxInt64-fp.value) ||
		(fp.value < 0 && other.value < math.MinInt64-fp.value) {
		return FixedPoint{}, fmt.Errorf("addition overflow")
	}
	return FixedPoint{value: fp.value + other.value}, nil
}

// Sub subtracts two fixed-point numbers with overflow checking
func (fp FixedPoint) Sub(other FixedPoint) (FixedPoint, error) {
	// Check for overflow
	if (other.value < 0 && fp.value > math.MaxInt64+other.value) ||
		(other.value > 0 && fp.value < math.MinInt64+other.value) {
		return FixedPoint{}, fmt.Errorf("subtraction overflow")
	}
	return FixedPoint{value: fp.value - other.value}, nil
}

// Mul multiplies two fixed-point numbers with overflow checking
func (fp FixedPoint) Mul(other FixedPoint) (FixedPoint, error) {
	// Use big.Int to avoid overflow during multiplication
	a := big.NewInt(fp.value)
	b := big.NewInt(other.value)
	result := new(big.Int).Mul(a, b)
	result.Div(result, big.NewInt(ScaleFactor))

	if !result.IsInt64() {
		return FixedPoint{}, fmt.Errorf("multiplication overflow")
	}

	return FixedPoint{value: result.Int64()}, nil
}

// Div divides two fixed-point numbers with overflow checking
func (fp FixedPoint) Div(other FixedPoint) (FixedPoint, error) {
	if other.value == 0 {
		return FixedPoint{}, fmt.Errorf("division by zero")
	}

	// Use big.Int to avoid overflow during multiplication
	a := big.NewInt(fp.value)
	a.Mul(a, big.NewInt(ScaleFactor))
	a.Div(a, big.NewInt(other.value))

	if !a.IsInt64() {
		return FixedPoint{}, fmt.Errorf("division overflow")
	}

	return FixedPoint{value: a.Int64()}, nil
}

// Mod returns the remainder of dividing two fixed-point numbers
func (fp FixedPoint) Mod(other FixedPoint) (FixedPoint, error) {
	if other.value == 0 {
		return FixedPoint{}, fmt.Errorf("modulo by zero")
	}
	return FixedPoint{value: fp.value % other.value}, nil
}

// Abs returns the absolute value of the fixed-point number
func (fp FixedPoint) Abs() FixedPoint {
	if fp.value < 0 {
		return FixedPoint{value: -fp.value}
	}
	return fp
}

// Neg returns the negation of the fixed-point number
func (fp FixedPoint) Neg() (FixedPoint, error) {
	if fp.value == math.MinInt64 {
		return FixedPoint{}, fmt.Errorf("negation overflow")
	}
	return FixedPoint{value: -fp.value}, nil
}

// Equal returns true if two fixed-point numbers are equal
func (fp FixedPoint) Equal(other FixedPoint) bool {
	return fp.value == other.value
}

// Less returns true if fp < other
func (fp FixedPoint) Less(other FixedPoint) bool {
	return fp.value < other.value
}

// LessOrEqual returns true if fp <= other
func (fp FixedPoint) LessOrEqual(other FixedPoint) bool {
	return fp.value <= other.value
}

// Greater returns true if fp > other
func (fp FixedPoint) Greater(other FixedPoint) bool {
	return fp.value > other.value
}

// GreaterOrEqual returns true if fp >= other
func (fp FixedPoint) GreaterOrEqual(other FixedPoint) bool {
	return fp.value >= other.value
}

// Min returns the minimum of two fixed-point numbers
func (fp FixedPoint) Min(other FixedPoint) FixedPoint {
	if fp.value < other.value {
		return fp
	}
	return other
}

// Max returns the maximum of two fixed-point numbers
func (fp FixedPoint) Max(other FixedPoint) FixedPoint {
	if fp.value > other.value {
		return fp
	}
	return other
}

// IsZero returns true if the fixed-point number is zero
func (fp FixedPoint) IsZero() bool {
	return fp.value == 0
}

// IsPositive returns true if the fixed-point number is positive
func (fp FixedPoint) IsPositive() bool {
	return fp.value > 0
}

// IsNegative returns true if the fixed-point number is negative
func (fp FixedPoint) IsNegative() bool {
	return fp.value < 0
}

// ToBasisPoints converts the fixed-point number to basis points (0-10000)
func (fp FixedPoint) ToBasisPoints() (int64, error) {
	// Multiply by 10000 and divide by ScaleFactor
	result := big.NewInt(fp.value)
	result.Mul(result, big.NewInt(10000))
	result.Div(result, big.NewInt(ScaleFactor))

	if !result.IsInt64() {
		return 0, fmt.Errorf("basis points conversion overflow")
	}

	basisPoints := result.Int64()
	if basisPoints < 0 || basisPoints > 10000 {
		return 0, fmt.Errorf("value out of basis points range")
	}

	return basisPoints, nil
}

// ToPercentage converts the fixed-point number to percentage (0-100)
func (fp FixedPoint) ToPercentage() (int64, error) {
	// Multiply by 100 and divide by ScaleFactor
	result := big.NewInt(fp.value)
	result.Mul(result, big.NewInt(100))
	result.Div(result, big.NewInt(ScaleFactor))

	if !result.IsInt64() {
		return 0, fmt.Errorf("percentage conversion overflow")
	}

	percentage := result.Int64()
	if percentage < 0 || percentage > 100 {
		return 0, fmt.Errorf("value out of percentage range")
	}

	return percentage, nil
}

// MulInt64 multiplies a fixed-point number by an integer
func (fp FixedPoint) MulInt64(n int64) (FixedPoint, error) {
	result := big.NewInt(fp.value)
	result.Mul(result, big.NewInt(n))

	if !result.IsInt64() {
		return FixedPoint{}, fmt.Errorf("multiplication overflow")
	}

	return FixedPoint{value: result.Int64()}, nil
}

// DivInt64 divides a fixed-point number by an integer
func (fp FixedPoint) DivInt64(n int64) (FixedPoint, error) {
	if n == 0 {
		return FixedPoint{}, fmt.Errorf("division by zero")
	}

	result := big.NewInt(fp.value)
	result.Div(result, big.NewInt(n))

	return FixedPoint{value: result.Int64()}, nil
}

// Pow raises the fixed-point number to an integer power
func (fp FixedPoint) Pow(n int64) (FixedPoint, error) {
	if n < 0 {
		return FixedPoint{}, fmt.Errorf("negative exponent not supported")
	}

	if n == 0 {
		return NewFixedPoint(1), nil
	}

	result := fp
	for i := int64(1); i < n; i++ {
		var err error
		result, err = result.Mul(fp)
		if err != nil {
			return FixedPoint{}, fmt.Errorf("power overflow: %w", err)
		}
	}

	return result, nil
}

// Sqrt returns the square root of the fixed-point number
func (fp FixedPoint) Sqrt() (FixedPoint, error) {
	if fp.value < 0 {
		return FixedPoint{}, fmt.Errorf("square root of negative number")
	}

	if fp.value == 0 {
		return FixedPoint{value: 0}, nil
	}

	// Use Newton's method for square root
	// x_{n+1} = (x_n + a/x_n) / 2
	x := big.NewInt(fp.value)
	x.Mul(x, big.NewInt(ScaleFactor)) // Scale up for precision

	// Initial guess
	guess := new(big.Int).Sqrt(x)
	if guess.Sign() == 0 {
		guess.SetInt64(1)
	}

	// Newton iterations
	for i := 0; i < 10; i++ {
		newGuess := new(big.Int).Div(x, guess)
		newGuess.Add(newGuess, guess)
		newGuess.Div(newGuess, big.NewInt(2))

		if newGuess.Cmp(guess) == 0 {
			break
		}
		guess = newGuess
	}

	return FixedPoint{value: guess.Int64()}, nil
}

// Zero returns a zero fixed-point number
func Zero() FixedPoint {
	return FixedPoint{value: 0}
}

// One returns a fixed-point number representing 1
func One() FixedPoint {
	return FixedPoint{value: ScaleFactor}
}

// Half returns a fixed-point number representing 0.5
func Half() FixedPoint {
	return FixedPoint{value: ScaleFactor / 2}
}
