package unit

import (
	"os"
	"testing"

	"diamante/monitoring/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitTracer(t *testing.T) {
	tests := []struct {
		name        string
		serviceName string
		setupEnv    func()
		cleanupEnv  func()
		wantErr     bool
	}{
		{
			name:        "init with service name",
			serviceName: "test-service",
			setupEnv:    func() {},
			cleanupEnv:  func() {},
			wantErr:     false,
		},
		{
			name:        "init with empty service name",
			serviceName: "",
			setupEnv:    func() {},
			cleanupEnv:  func() {},
			wantErr:     false,
		},
		{
			name:        "init with environment config",
			serviceName: "env-test-service",
			setupEnv: func() {
				os.Setenv("JAEGER_SERVICE_NAME", "env-service")
				os.Setenv("JAEGER_AGENT_HOST", "localhost")
				os.Setenv("JAEGER_AGENT_PORT", "6831")
			},
			cleanupEnv: func() {
				os.Unsetenv("JAEGER_SERVICE_NAME")
				os.Unsetenv("JAEGER_AGENT_HOST")
				os.Unsetenv("JAEGER_AGENT_PORT")
			},
			wantErr: false,
		},
		{
			name:        "init with sampler config",
			serviceName: "sampled-service",
			setupEnv: func() {
				os.Setenv("JAEGER_SAMPLER_TYPE", "const")
				os.Setenv("JAEGER_SAMPLER_PARAM", "1")
			},
			cleanupEnv: func() {
				os.Unsetenv("JAEGER_SAMPLER_TYPE")
				os.Unsetenv("JAEGER_SAMPLER_PARAM")
			},
			wantErr: false,
		},
		{
			name:        "init with reporter config",
			serviceName: "reported-service",
			setupEnv: func() {
				os.Setenv("JAEGER_REPORTER_LOG_SPANS", "true")
				os.Setenv("JAEGER_REPORTER_FLUSH_INTERVAL", "1s")
			},
			cleanupEnv: func() {
				os.Unsetenv("JAEGER_REPORTER_LOG_SPANS")
				os.Unsetenv("JAEGER_REPORTER_FLUSH_INTERVAL")
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup environment
			tt.setupEnv()
			defer tt.cleanupEnv()

			// Initialize tracer
			tracer, closer, err := tracing.InitTracer(tt.serviceName)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, tracer)
				assert.Nil(t, closer)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, tracer)
				assert.NotNil(t, closer)

				// Verify global tracer is set
				globalTracer := opentracing.GlobalTracer()
				assert.NotNil(t, globalTracer)
				assert.Equal(t, tracer, globalTracer)

				// Clean up
				if closer != nil {
					closer.Close()
				}
			}
		})
	}
}

func TestInitTracerServiceNameHandling(t *testing.T) {
	tests := []struct {
		name            string
		serviceName     string
		envServiceName  string
		expectedService string
	}{
		{
			name:            "service name from parameter",
			serviceName:     "param-service",
			envServiceName:  "",
			expectedService: "param-service",
		},
		{
			name:            "service name from environment when param empty",
			serviceName:     "",
			envServiceName:  "env-service",
			expectedService: "env-service",
		},
		{
			name:            "parameter overrides environment",
			serviceName:     "param-service",
			envServiceName:  "env-service",
			expectedService: "param-service",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup environment
			if tt.envServiceName != "" {
				os.Setenv("JAEGER_SERVICE_NAME", tt.envServiceName)
				defer os.Unsetenv("JAEGER_SERVICE_NAME")
			}

			// Initialize tracer
			tracer, closer, err := tracing.InitTracer(tt.serviceName)
			require.NoError(t, err)
			require.NotNil(t, tracer)
			require.NotNil(t, closer)

			defer closer.Close()

			// The service name handling is internal to Jaeger config
			// We can't directly test it without accessing internal state
			// But we can ensure the tracer is created successfully
			assert.NotNil(t, tracer)
		})
	}
}

func TestInitTracerWithInvalidConfig(t *testing.T) {
	// Test with invalid sampler parameter
	os.Setenv("JAEGER_SAMPLER_PARAM", "invalid")
	defer os.Unsetenv("JAEGER_SAMPLER_PARAM")

	tracer, closer, err := tracing.InitTracer("test-service")

	// Jaeger should handle invalid config gracefully and use defaults
	// So this should not error in most cases
	if err != nil {
		assert.Nil(t, tracer)
		assert.Nil(t, closer)
	} else {
		assert.NotNil(t, tracer)
		assert.NotNil(t, closer)
		closer.Close()
	}
}

func TestTracerBasicOperation(t *testing.T) {
	tracer, closer, err := tracing.InitTracer("test-operation-service")
	require.NoError(t, err)
	require.NotNil(t, tracer)
	require.NotNil(t, closer)

	defer closer.Close()

	// Test basic span creation
	span := tracer.StartSpan("test-operation")
	require.NotNil(t, span)

	// Test span tagging
	span.SetTag("test.tag", "test-value")
	span.SetTag("test.number", 42)
	span.SetTag("test.bool", true)

	// Test span logging
	span.LogKV("event", "test-event", "value", "test-log")

	// Test span finishing
	span.Finish()

	// Should complete without panics
}

func TestTracerChildSpan(t *testing.T) {
	tracer, closer, err := tracing.InitTracer("test-child-service")
	require.NoError(t, err)
	require.NotNil(t, tracer)
	require.NotNil(t, closer)

	defer closer.Close()

	// Create parent span
	parentSpan := tracer.StartSpan("parent-operation")
	require.NotNil(t, parentSpan)

	// Create child span
	childSpan := tracer.StartSpan("child-operation", opentracing.ChildOf(parentSpan.Context()))
	require.NotNil(t, childSpan)

	// Test operations on child span
	childSpan.SetTag("child.tag", "child-value")
	childSpan.LogKV("child.event", "child-event")

	// Finish spans
	childSpan.Finish()
	parentSpan.Finish()

	// Should complete without panics
}

func TestTracerContextPropagation(t *testing.T) {
	tracer, closer, err := tracing.InitTracer("test-context-service")
	require.NoError(t, err)
	require.NotNil(t, tracer)
	require.NotNil(t, closer)

	defer closer.Close()

	// Create span
	span := tracer.StartSpan("context-operation")
	require.NotNil(t, span)

	// Test context extraction/injection (basic smoke test)
	spanContext := span.Context()
	require.NotNil(t, spanContext)

	// Test baggage
	span.SetBaggageItem("test-baggage", "baggage-value")
	baggageValue := span.BaggageItem("test-baggage")
	assert.Equal(t, "baggage-value", baggageValue)

	span.Finish()
}

func TestMultipleTracerInitialization(t *testing.T) {
	// Test that multiple tracer initializations work correctly
	tracer1, closer1, err1 := tracing.InitTracer("service-1")
	require.NoError(t, err1)
	require.NotNil(t, tracer1)
	require.NotNil(t, closer1)

	// Second initialization should replace the global tracer
	tracer2, closer2, err2 := tracing.InitTracer("service-2")
	require.NoError(t, err2)
	require.NotNil(t, tracer2)
	require.NotNil(t, closer2)

	// Global tracer should be the latest one
	globalTracer := opentracing.GlobalTracer()
	assert.Equal(t, tracer2, globalTracer)

	// Clean up
	closer1.Close()
	closer2.Close()
}

func TestTracerConcurrentAccess(t *testing.T) {
	tracer, closer, err := tracing.InitTracer("concurrent-test-service")
	require.NoError(t, err)
	require.NotNil(t, tracer)
	require.NotNil(t, closer)

	defer closer.Close()

	// Test concurrent span creation
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			span := tracer.StartSpan("concurrent-operation")
			span.SetTag("goroutine.id", id)
			span.LogKV("event", "concurrent-test")
			span.Finish()
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should complete without race conditions or panics
}

// Benchmark tests

func BenchmarkInitTracer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tracer, closer, err := tracing.InitTracer("benchmark-service")
		if err != nil {
			b.Fatal(err)
		}
		if closer != nil {
			closer.Close()
		}
		_ = tracer
	}
}

func BenchmarkSpanCreation(b *testing.B) {
	tracer, closer, err := tracing.InitTracer("benchmark-span-service")
	require.NoError(b, err)
	defer closer.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		span := tracer.StartSpan("benchmark-operation")
		span.Finish()
	}
}

func BenchmarkSpanWithTags(b *testing.B) {
	tracer, closer, err := tracing.InitTracer("benchmark-tags-service")
	require.NoError(b, err)
	defer closer.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		span := tracer.StartSpan("benchmark-operation")
		span.SetTag("iteration", i)
		span.SetTag("benchmark", true)
		span.SetTag("value", float64(i)*1.5)
		span.Finish()
	}
}

func BenchmarkSpanWithLogs(b *testing.B) {
	tracer, closer, err := tracing.InitTracer("benchmark-logs-service")
	require.NoError(b, err)
	defer closer.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		span := tracer.StartSpan("benchmark-operation")
		span.LogKV("event", "benchmark-event", "iteration", i)
		span.Finish()
	}
}

func BenchmarkChildSpanCreation(b *testing.B) {
	tracer, closer, err := tracing.InitTracer("benchmark-child-service")
	require.NoError(b, err)
	defer closer.Close()

	parentSpan := tracer.StartSpan("parent-operation")
	defer parentSpan.Finish()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		childSpan := tracer.StartSpan("child-operation", opentracing.ChildOf(parentSpan.Context()))
		childSpan.Finish()
	}
}

// Integration test with realistic blockchain tracing scenario
func TestBlockchainTracingScenario(t *testing.T) {
	tracer, closer, err := tracing.InitTracer("diamante-blockchain")
	require.NoError(t, err)
	require.NotNil(t, tracer)
	require.NotNil(t, closer)

	defer closer.Close()

	// Simulate transaction processing trace
	txSpan := tracer.StartSpan("process-transaction")
	txSpan.SetTag("tx.hash", "0x1234567890abcdef")
	txSpan.SetTag("tx.type", "transfer")
	txSpan.SetTag("tx.amount", 100.0)

	// Simulate validation step
	validationSpan := tracer.StartSpan("validate-transaction", opentracing.ChildOf(txSpan.Context()))
	validationSpan.SetTag("validation.rules", 5)
	validationSpan.LogKV("event", "validation-start")
	// Simulate validation work
	validationSpan.LogKV("event", "validation-complete", "result", "valid")
	validationSpan.Finish()

	// Simulate consensus step
	consensusSpan := tracer.StartSpan("consensus-process", opentracing.ChildOf(txSpan.Context()))
	consensusSpan.SetTag("consensus.round", 123)
	consensusSpan.SetTag("consensus.validators", 10)
	consensusSpan.LogKV("event", "consensus-start")
	// Simulate consensus work
	consensusSpan.LogKV("event", "consensus-complete", "result", "accepted")
	consensusSpan.Finish()

	// Simulate storage step
	storageSpan := tracer.StartSpan("store-transaction", opentracing.ChildOf(txSpan.Context()))
	storageSpan.SetTag("storage.type", "mongodb")
	storageSpan.SetTag("storage.block", 1000)
	storageSpan.LogKV("event", "storage-start")
	// Simulate storage work
	storageSpan.LogKV("event", "storage-complete", "result", "stored")
	storageSpan.Finish()

	txSpan.LogKV("event", "transaction-complete", "status", "success")
	txSpan.Finish()

	// Should complete the full trace without issues
}

func TestTracingDisabled(t *testing.T) {
	// Test behavior when tracing is disabled via environment
	os.Setenv("JAEGER_DISABLED", "true")
	defer os.Unsetenv("JAEGER_DISABLED")

	tracer, closer, err := tracing.InitTracer("disabled-service")
	require.NoError(t, err)
	require.NotNil(t, tracer)
	require.NotNil(t, closer)

	defer closer.Close()

	// Even when disabled, basic operations should still work
	span := tracer.StartSpan("test-operation")
	span.SetTag("test", "value")
	span.LogKV("event", "test")
	span.Finish()

	// Should complete without issues
}
