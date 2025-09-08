package tracing

import (
	"io"

	opentracing "github.com/opentracing/opentracing-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

// InitTracer configures and returns a Jaeger tracer.
func InitTracer(service string) (opentracing.Tracer, io.Closer, error) {
	cfg, err := jaegercfg.FromEnv()
	if err != nil {
		return nil, nil, err
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = service
	}
	tracer, closer, err := cfg.NewTracer()
	if err == nil {
		opentracing.SetGlobalTracer(tracer)
	}
	return tracer, closer, err
}
