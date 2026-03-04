package service

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
)

// timeoutError implements net.Error with Timeout() returning true.
type timeoutError struct{}

func (e *timeoutError) Error() string   { return "i/o timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return false }

var _ net.Error = (*timeoutError)(nil)

func TestIsRetriableConnectionError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"io.EOF", io.EOF, true},
		{"io.ErrUnexpectedEOF", io.ErrUnexpectedEOF, true},
		{"context.DeadlineExceeded", context.DeadlineExceeded, true},
		{"net timeout", &timeoutError{}, true},
		{"wrapped EOF", fmt.Errorf("read body: %w", io.EOF), true},
		{"wrapped unexpected EOF", fmt.Errorf("http: %w", io.ErrUnexpectedEOF), true},
		{"connection reset string", fmt.Errorf("read tcp: connection reset by peer"), true},
		{"broken pipe string", fmt.Errorf("write: broken pipe"), true},
		{"unexpected EOF string", fmt.Errorf("http2: unexpected EOF"), true},
		{"non-retriable error", fmt.Errorf("parse error: invalid JSON"), false},
		{"non-retriable 403", fmt.Errorf("403 Forbidden"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isRetriableConnectionError(tt.err)
			if got != tt.want {
				t.Errorf("isRetriableConnectionError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
