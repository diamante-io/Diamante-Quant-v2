package api

import "errors"

// ErrBodyTooLarge is returned when the request body exceeds allowed limit.
var ErrBodyTooLarge = errors.New("http: request body too large")
