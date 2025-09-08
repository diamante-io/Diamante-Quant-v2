package p2p

import (
	"bytes"
	"compress/gzip"
	"io"

	"diamante/apperrors"
)

// compressPayload compresses data using gzip
func compressPayload(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)

	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to write compressed data")
	}

	if err := writer.Close(); err != nil {
		return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to close gzip writer")
	}

	return buf.Bytes(), nil
}

// decompressPayload decompresses gzip-compressed data
func decompressPayload(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to create gzip reader")
	}
	defer reader.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, reader); err != nil {
		return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to decompress data")
	}

	return buf.Bytes(), nil
}
