package cvm

import (
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// MessageRouter routes messages between VMs
type MessageRouter struct {
	routes       map[string]Route // sourceVM:targetVM -> route
	routeMetrics map[string]*RouteMetrics
	mu           sync.RWMutex

	// Metrics
	routedMessages prometheus.Counter
	routingErrors  prometheus.Counter
}

// Route defines a routing path between VMs
type Route struct {
	SourceVM       VMType
	TargetVM       VMType
	Direct         bool    // Direct route or requires intermediary
	Intermediary   *VMType // Intermediary VM if not direct
	CostMultiplier float64 // Gas cost multiplier for this route
	Enabled        bool
}

// RouteMetrics tracks metrics for a specific route
type RouteMetrics struct {
	MessageCount   int64
	TotalGasUsed   uint64
	AverageLatency time.Duration
	LastUsed       time.Time
	Errors         int64
}

// NewMessageRouter creates a new message router
func NewMessageRouter() *MessageRouter {
	r := &MessageRouter{
		routes:       make(map[string]Route),
		routeMetrics: make(map[string]*RouteMetrics),

		routedMessages: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cvm_routed_messages_total",
			Help: "Total number of routed messages",
		}),
		routingErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cvm_routing_errors_total",
			Help: "Total number of routing errors",
		}),
	}

	// Initialize default routes
	r.initializeDefaultRoutes()

	// Register metrics (ignore errors in case of duplicate registration)
	prometheus.Register(r.routedMessages)
	prometheus.Register(r.routingErrors)

	return r
}

// initializeDefaultRoutes sets up default routing paths
func (r *MessageRouter) initializeDefaultRoutes() {
	// Direct routes between all VM pairs
	vmTypes := []VMType{VMTypeZKEVM, VMTypeChaincode, VMTypeNative}

	for _, source := range vmTypes {
		for _, target := range vmTypes {
			if source != target {
				routeKey := r.createRouteKey(source, target)
				r.routes[routeKey] = Route{
					SourceVM:       source,
					TargetVM:       target,
					Direct:         true,
					CostMultiplier: 1.0,
					Enabled:        true,
				}
				r.routeMetrics[routeKey] = &RouteMetrics{}
			}
		}
	}

	// Special optimized routes with lower cost
	// zkEVM <-> Native (optimized for DeFi)
	r.routes[r.createRouteKey(VMTypeZKEVM, VMTypeNative)] = Route{
		SourceVM:       VMTypeZKEVM,
		TargetVM:       VMTypeNative,
		Direct:         true,
		CostMultiplier: 0.8, // 20% discount for optimized route
		Enabled:        true,
	}

	// Chaincode <-> Native (optimized for enterprise)
	r.routes[r.createRouteKey(VMTypeChaincode, VMTypeNative)] = Route{
		SourceVM:       VMTypeChaincode,
		TargetVM:       VMTypeNative,
		Direct:         true,
		CostMultiplier: 0.9, // 10% discount
		Enabled:        true,
	}
}

// GetRoute returns the best route between two VMs
func (r *MessageRouter) GetRoute(source, target VMType) (*Route, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	routeKey := r.createRouteKey(source, target)
	route, exists := r.routes[routeKey]
	if !exists {
		return nil, fmt.Errorf("no route found from %s to %s", source, target)
	}

	if !route.Enabled {
		return nil, fmt.Errorf("route from %s to %s is disabled", source, target)
	}

	return &route, nil
}

// RouteMessage routes a message and tracks metrics
func (r *MessageRouter) RouteMessage(msg CVMMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	route, err := r.GetRoute(msg.SourceVM, msg.TargetVM)
	if err != nil {
		r.routingErrors.Inc()
		return err
	}

	// Update metrics
	routeKey := r.createRouteKey(msg.SourceVM, msg.TargetVM)
	metrics := r.routeMetrics[routeKey]
	if metrics == nil {
		metrics = &RouteMetrics{}
		r.routeMetrics[routeKey] = metrics
	}

	metrics.MessageCount++
	metrics.LastUsed = time.Now()
	r.routedMessages.Inc()

	// If route requires intermediary, update the message path
	if !route.Direct && route.Intermediary != nil {
		// This would be handled by the protocol layer
		// For now, just log it
	}

	return nil
}

// UpdateRouteMetrics updates metrics after message execution
func (r *MessageRouter) UpdateRouteMetrics(source, target VMType, gasUsed uint64, latency time.Duration, success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	routeKey := r.createRouteKey(source, target)
	metrics := r.routeMetrics[routeKey]
	if metrics == nil {
		return
	}

	metrics.TotalGasUsed += gasUsed

	// Update average latency
	if metrics.MessageCount > 0 {
		totalLatency := metrics.AverageLatency * time.Duration(metrics.MessageCount-1)
		metrics.AverageLatency = (totalLatency + latency) / time.Duration(metrics.MessageCount)
	} else {
		metrics.AverageLatency = latency
	}

	if !success {
		metrics.Errors++
	}
}

// EnableRoute enables a route between VMs
func (r *MessageRouter) EnableRoute(source, target VMType) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	routeKey := r.createRouteKey(source, target)
	route, exists := r.routes[routeKey]
	if !exists {
		return fmt.Errorf("route from %s to %s does not exist", source, target)
	}

	route.Enabled = true
	r.routes[routeKey] = route
	return nil
}

// DisableRoute disables a route between VMs
func (r *MessageRouter) DisableRoute(source, target VMType) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	routeKey := r.createRouteKey(source, target)
	route, exists := r.routes[routeKey]
	if !exists {
		return fmt.Errorf("route from %s to %s does not exist", source, target)
	}

	route.Enabled = false
	r.routes[routeKey] = route
	return nil
}

// SetRouteCostMultiplier updates the cost multiplier for a route
func (r *MessageRouter) SetRouteCostMultiplier(source, target VMType, multiplier float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if multiplier <= 0 {
		return fmt.Errorf("cost multiplier must be positive")
	}

	routeKey := r.createRouteKey(source, target)
	route, exists := r.routes[routeKey]
	if !exists {
		return fmt.Errorf("route from %s to %s does not exist", source, target)
	}

	route.CostMultiplier = multiplier
	r.routes[routeKey] = route
	return nil
}

// GetRouteMetrics returns metrics for a specific route
func (r *MessageRouter) GetRouteMetrics(source, target VMType) (*RouteMetrics, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	routeKey := r.createRouteKey(source, target)
	metrics, exists := r.routeMetrics[routeKey]
	if !exists {
		return nil, fmt.Errorf("no metrics found for route from %s to %s", source, target)
	}

	// Return a copy
	copy := *metrics
	return &copy, nil
}

// GetAllRoutes returns all configured routes
func (r *MessageRouter) GetAllRoutes() []Route {
	r.mu.RLock()
	defer r.mu.RUnlock()

	routes := make([]Route, 0, len(r.routes))
	for _, route := range r.routes {
		routes = append(routes, route)
	}

	return routes
}

// createRouteKey creates a unique key for a route
func (r *MessageRouter) createRouteKey(source, target VMType) string {
	return fmt.Sprintf("%s:%s", source, target)
}

// RoutingTable provides a summary of all routes and their status
type RoutingTable struct {
	Routes []RouteInfo `json:"routes"`
}

// RouteInfo contains route information and metrics
type RouteInfo struct {
	Source         string        `json:"source"`
	Target         string        `json:"target"`
	Direct         bool          `json:"direct"`
	Enabled        bool          `json:"enabled"`
	CostMultiplier float64       `json:"cost_multiplier"`
	MessageCount   int64         `json:"message_count"`
	ErrorRate      float64       `json:"error_rate"`
	AverageLatency time.Duration `json:"average_latency"`
}

// GetRoutingTable returns the complete routing table with metrics
func (r *MessageRouter) GetRoutingTable() RoutingTable {
	r.mu.RLock()
	defer r.mu.RUnlock()

	table := RoutingTable{
		Routes: make([]RouteInfo, 0, len(r.routes)),
	}

	for routeKey, route := range r.routes {
		metrics := r.routeMetrics[routeKey]

		errorRate := float64(0)
		if metrics != nil && metrics.MessageCount > 0 {
			errorRate = float64(metrics.Errors) / float64(metrics.MessageCount)
		}

		info := RouteInfo{
			Source:         route.SourceVM.String(),
			Target:         route.TargetVM.String(),
			Direct:         route.Direct,
			Enabled:        route.Enabled,
			CostMultiplier: route.CostMultiplier,
		}

		if metrics != nil {
			info.MessageCount = metrics.MessageCount
			info.ErrorRate = errorRate
			info.AverageLatency = metrics.AverageLatency
		}

		table.Routes = append(table.Routes, info)
	}

	return table
}
