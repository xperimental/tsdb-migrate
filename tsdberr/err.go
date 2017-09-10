package tsdberr

import "errors"

var (
	// ErrNotFound is used when a fast-add reference is not found.
	ErrNotFound = errors.New("not found")

	// ErrOutOfOrderSample is used when a samples arrives out of order.
	ErrOutOfOrderSample = errors.New("out of order sample")

	// ErrDuplicateSampleForTimestamp is used when a duplicate sample arrives for a timestamp.
	ErrDuplicateSampleForTimestamp = errors.New("duplicate sample for timestamp")

	// ErrOutOfBounds is used when a sample value is out of bounds.
	ErrOutOfBounds = errors.New("out of bounds")
)
