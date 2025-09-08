// Package evm provides initialization for the EVM runtime
package evm

import (
	"diamante/vm/runtime"
)

// InitializeEVMRuntime explicitly registers the EVM runtime with the global runtime registry
// This should be called during application startup instead of using init()
func InitializeEVMRuntime() error {
	return runtime.AutoRegisterRuntime(runtime.RuntimeTypeEVM, func() runtime.Runtime {
		return NewEVMRuntime()
	})
}
