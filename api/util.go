package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// decodeJSONBody decodes JSON from the request with a size limit.
func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst interface{}, limit int64) error {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var syntaxError *json.SyntaxError
		var unmarshalTypeError *json.UnmarshalTypeError

		switch {
		case errors.Is(err, io.EOF):
			return fmt.Errorf("empty request body")
		case errors.As(err, &syntaxError):
			return fmt.Errorf("malformed JSON at position %d", syntaxError.Offset)
		case errors.As(err, &unmarshalTypeError):
			return fmt.Errorf("invalid value for field %s", unmarshalTypeError.Field)
		case errors.As(err, new(*http.MaxBytesError)):
			return ErrBodyTooLarge
		default:
			return fmt.Errorf("invalid request body")
		}
	}
	if dec.More() {
		return fmt.Errorf("multiple JSON objects")
	}
	return nil
}
