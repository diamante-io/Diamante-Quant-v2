// Package monitoring provides Grafana dashboard generation
package monitoring

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// DashboardGenerator generates Grafana dashboards
type DashboardGenerator struct {
	config *DashboardConfig
}

// DashboardConfig contains dashboard configuration
type DashboardConfig struct {
	OutputDir   string `json:"output_dir"`
	DataSource  string `json:"data_source"`
	RefreshRate string `json:"refresh_rate"`
	TimeRange   string `json:"time_range"`
}

// GrafanaDashboard represents a Grafana dashboard
type GrafanaDashboard struct {
	ID              *int           `json:"id"`
	Title           string         `json:"title"`
	Tags            []string       `json:"tags"`
	Style           string         `json:"style"`
	Timezone        string         `json:"timezone"`
	Editable        bool           `json:"editable"`
	HideControls    bool           `json:"hideControls"`
	SharedCrosshair bool           `json:"sharedCrosshair"`
	Panels          []GrafanaPanel `json:"panels"`
	Time            GrafanaTime    `json:"time"`
	Refresh         string         `json:"refresh"`
	SchemaVersion   int            `json:"schemaVersion"`
	Version         int            `json:"version"`
}

// GrafanaPanel represents a dashboard panel
type GrafanaPanel struct {
	ID          int                    `json:"id"`
	Title       string                 `json:"title"`
	Type        string                 `json:"type"`
	DataSource  string                 `json:"datasource"`
	Targets     []GrafanaTarget        `json:"targets"`
	GridPos     GrafanaGridPos         `json:"gridPos"`
	XAxis       *GrafanaAxis           `json:"xAxes,omitempty"`
	YAxes       []GrafanaAxis          `json:"yAxes,omitempty"`
	Legend      *GrafanaLegend         `json:"legend,omitempty"`
	Tooltip     *GrafanaTooltip        `json:"tooltip,omitempty"`
	Options     map[string]interface{} `json:"options,omitempty"`
	FieldConfig *GrafanaFieldConfig    `json:"fieldConfig,omitempty"`
}

// GrafanaTarget represents a query target
type GrafanaTarget struct {
	Expr         string `json:"expr"`
	Format       string `json:"format"`
	IntervalMs   int    `json:"intervalMs"`
	LegendFormat string `json:"legendFormat"`
	RefID        string `json:"refId"`
}

// GrafanaGridPos represents panel position
type GrafanaGridPos struct {
	H int `json:"h"`
	W int `json:"w"`
	X int `json:"x"`
	Y int `json:"y"`
}

// GrafanaAxis represents chart axis
type GrafanaAxis struct {
	Label string  `json:"label,omitempty"`
	Min   *string `json:"min,omitempty"`
	Max   *string `json:"max,omitempty"`
	Unit  string  `json:"unit,omitempty"`
}

// GrafanaLegend represents chart legend
type GrafanaLegend struct {
	Show   bool `json:"show"`
	Values bool `json:"values"`
	Min    bool `json:"min"`
	Max    bool `json:"max"`
	Avg    bool `json:"avg"`
	Total  bool `json:"total"`
}

// GrafanaTooltip represents chart tooltip
type GrafanaTooltip struct {
	Shared    bool   `json:"shared"`
	Sort      int    `json:"sort"`
	ValueType string `json:"value_type"`
}

// GrafanaTime represents time range
type GrafanaTime struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// GrafanaFieldConfig represents field configuration
type GrafanaFieldConfig struct {
	Defaults  GrafanaFieldDefaults `json:"defaults"`
	Overrides []interface{}        `json:"overrides"`
}

// GrafanaFieldDefaults represents field defaults
type GrafanaFieldDefaults struct {
	Color GrafanaColorConfig `json:"color"`
	Unit  string             `json:"unit,omitempty"`
	Min   *float64           `json:"min,omitempty"`
	Max   *float64           `json:"max,omitempty"`
}

// GrafanaColorConfig represents color configuration
type GrafanaColorConfig struct {
	Mode string `json:"mode"`
}

// NewDashboardGenerator creates a new dashboard generator
func NewDashboardGenerator(config *DashboardConfig) *DashboardGenerator {
	if config == nil {
		config = &DashboardConfig{
			OutputDir:   "./dashboards",
			DataSource:  "Prometheus",
			RefreshRate: "5s",
			TimeRange:   "1h",
		}
	}

	return &DashboardGenerator{
		config: config,
	}
}

// GenerateAllDashboards generates all Diamante dashboards
func (dg *DashboardGenerator) GenerateAllDashboards() error {
	// Create output directory
	if err := os.MkdirAll(dg.config.OutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	dashboards := []struct {
		name      string
		generator func() (*GrafanaDashboard, error)
	}{
		{"diamante-overview", dg.generateOverviewDashboard},
		{"diamante-transactions", dg.generateTransactionDashboard},
		{"diamante-consensus", dg.generateConsensusDashboard},
		{"diamante-network", dg.generateNetworkDashboard},
		{"diamante-storage", dg.generateStorageDashboard},
		{"diamante-security", dg.generateSecurityDashboard},
		{"diamante-system", dg.generateSystemDashboard},
	}

	for _, dash := range dashboards {
		dashboard, err := dash.generator()
		if err != nil {
			return fmt.Errorf("failed to generate %s dashboard: %w", dash.name, err)
		}

		if err := dg.saveDashboard(dash.name, dashboard); err != nil {
			return fmt.Errorf("failed to save %s dashboard: %w", dash.name, err)
		}
	}

	return nil
}

// generateOverviewDashboard generates the main overview dashboard
func (dg *DashboardGenerator) generateOverviewDashboard() (*GrafanaDashboard, error) {
	return &GrafanaDashboard{
		Title:           "Diamante Blockchain Overview",
		Tags:            []string{"diamante", "blockchain", "overview"},
		Style:           "dark",
		Timezone:        "browser",
		Editable:        true,
		HideControls:    false,
		SharedCrosshair: true,
		SchemaVersion:   27,
		Version:         1,
		Time: GrafanaTime{
			From: "now-1h",
			To:   "now",
		},
		Refresh: dg.config.RefreshRate,
		Panels: []GrafanaPanel{
			// TPS Panel
			{
				ID:         1,
				Title:      "Transactions Per Second",
				Type:       "stat",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 6, X: 0, Y: 0},
				Targets: []GrafanaTarget{
					{
						Expr:         "rate(diamante_transactions_total[5m])",
						Format:       "time_series",
						LegendFormat: "TPS",
						RefID:        "A",
					},
				},
				FieldConfig: &GrafanaFieldConfig{
					Defaults: GrafanaFieldDefaults{
						Unit:  "reqps",
						Color: GrafanaColorConfig{Mode: "palette-classic"},
					},
				},
			},
			// Block Height Panel
			{
				ID:         2,
				Title:      "Block Height",
				Type:       "stat",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 6, X: 6, Y: 0},
				Targets: []GrafanaTarget{
					{
						Expr:         "diamante_block_height",
						Format:       "time_series",
						LegendFormat: "Height",
						RefID:        "A",
					},
				},
			},
			// Peer Count Panel
			{
				ID:         3,
				Title:      "Connected Peers",
				Type:       "stat",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 6, X: 12, Y: 0},
				Targets: []GrafanaTarget{
					{
						Expr:         "diamante_peer_count",
						Format:       "time_series",
						LegendFormat: "Peers",
						RefID:        "A",
					},
				},
			},
			// System Status Panel
			{
				ID:         4,
				Title:      "System Health",
				Type:       "stat",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 6, X: 18, Y: 0},
				Targets: []GrafanaTarget{
					{
						Expr:         "up",
						Format:       "time_series",
						LegendFormat: "Status",
						RefID:        "A",
					},
				},
			},
			// Transaction Rate Chart
			{
				ID:         5,
				Title:      "Transaction Rate Over Time",
				Type:       "graph",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 9, W: 12, X: 0, Y: 8},
				Targets: []GrafanaTarget{
					{
						Expr:         "rate(diamante_transactions_total[1m])",
						Format:       "time_series",
						LegendFormat: "{{type}}",
						RefID:        "A",
					},
				},
				YAxes: []GrafanaAxis{
					{Label: "TPS", Unit: "reqps"},
					{Label: "", Unit: "short"},
				},
				Legend: &GrafanaLegend{Show: true, Values: true, Avg: true, Max: true},
			},
			// Block Production Chart
			{
				ID:         6,
				Title:      "Block Production",
				Type:       "graph",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 9, W: 12, X: 12, Y: 8},
				Targets: []GrafanaTarget{
					{
						Expr:         "rate(diamante_blocks_total[1m])",
						Format:       "time_series",
						LegendFormat: "Blocks/sec",
						RefID:        "A",
					},
				},
				YAxes: []GrafanaAxis{
					{Label: "Blocks/sec", Unit: "reqps"},
					{Label: "", Unit: "short"},
				},
			},
		},
	}, nil
}

// generateTransactionDashboard generates the transaction dashboard
func (dg *DashboardGenerator) generateTransactionDashboard() (*GrafanaDashboard, error) {
	return &GrafanaDashboard{
		Title:         "Diamante Transaction Metrics",
		Tags:          []string{"diamante", "transactions"},
		Style:         "dark",
		Timezone:      "browser",
		Editable:      true,
		SchemaVersion: 27,
		Version:       1,
		Time: GrafanaTime{
			From: "now-1h",
			To:   "now",
		},
		Refresh: dg.config.RefreshRate,
		Panels: []GrafanaPanel{
			// Transaction Pool Size
			{
				ID:         1,
				Title:      "Transaction Pool Size",
				Type:       "graph",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 12, X: 0, Y: 0},
				Targets: []GrafanaTarget{
					{
						Expr:         "diamante_transaction_pool_size",
						Format:       "time_series",
						LegendFormat: "Pool Size",
						RefID:        "A",
					},
				},
			},
			// Transaction Processing Duration
			{
				ID:         2,
				Title:      "Transaction Processing Duration",
				Type:       "graph",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 12, X: 12, Y: 0},
				Targets: []GrafanaTarget{
					{
						Expr:         "histogram_quantile(0.95, diamante_transaction_processing_duration_bucket)",
						Format:       "time_series",
						LegendFormat: "95th percentile",
						RefID:        "A",
					},
					{
						Expr:         "histogram_quantile(0.50, diamante_transaction_processing_duration_bucket)",
						Format:       "time_series",
						LegendFormat: "50th percentile",
						RefID:        "B",
					},
				},
				YAxes: []GrafanaAxis{
					{Label: "Duration", Unit: "s"},
					{Label: "", Unit: "short"},
				},
			},
			// Transaction Types
			{
				ID:         3,
				Title:      "Transaction Types",
				Type:       "piechart",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 12, X: 0, Y: 8},
				Targets: []GrafanaTarget{
					{
						Expr:         "sum by (type) (rate(diamante_transactions_total[5m]))",
						Format:       "time_series",
						LegendFormat: "{{type}}",
						RefID:        "A",
					},
				},
			},
			// Transaction Status
			{
				ID:         4,
				Title:      "Transaction Status",
				Type:       "stat",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 12, X: 12, Y: 8},
				Targets: []GrafanaTarget{
					{
						Expr:         "sum by (status) (rate(diamante_transactions_total[5m]))",
						Format:       "time_series",
						LegendFormat: "{{status}}",
						RefID:        "A",
					},
				},
			},
		},
	}, nil
}

// generateConsensusDashboard generates the consensus dashboard
func (dg *DashboardGenerator) generateConsensusDashboard() (*GrafanaDashboard, error) {
	return &GrafanaDashboard{
		Title:         "Diamante Consensus Metrics",
		Tags:          []string{"diamante", "consensus"},
		Style:         "dark",
		Timezone:      "browser",
		Editable:      true,
		SchemaVersion: 27,
		Version:       1,
		Time: GrafanaTime{
			From: "now-1h",
			To:   "now",
		},
		Refresh: dg.config.RefreshRate,
		Panels: []GrafanaPanel{
			// Consensus Rounds
			{
				ID:         1,
				Title:      "Consensus Rounds",
				Type:       "graph",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 12, X: 0, Y: 0},
				Targets: []GrafanaTarget{
					{
						Expr:         "rate(diamante_consensus_rounds_total[1m])",
						Format:       "time_series",
						LegendFormat: "{{result}}",
						RefID:        "A",
					},
				},
			},
			// Consensus Duration
			{
				ID:         2,
				Title:      "Consensus Duration",
				Type:       "graph",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 12, X: 12, Y: 0},
				Targets: []GrafanaTarget{
					{
						Expr:         "histogram_quantile(0.95, diamante_consensus_duration_bucket)",
						Format:       "time_series",
						LegendFormat: "95th percentile",
						RefID:        "A",
					},
				},
				YAxes: []GrafanaAxis{
					{Label: "Duration", Unit: "s"},
					{Label: "", Unit: "short"},
				},
			},
			// Validator Count
			{
				ID:         3,
				Title:      "Active Validators",
				Type:       "stat",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 6, X: 0, Y: 8},
				Targets: []GrafanaTarget{
					{
						Expr:         "diamante_validator_count",
						Format:       "time_series",
						LegendFormat: "Validators",
						RefID:        "A",
					},
				},
			},
			// Vote Rate
			{
				ID:         4,
				Title:      "Vote Rate",
				Type:       "graph",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 18, X: 6, Y: 8},
				Targets: []GrafanaTarget{
					{
						Expr:         "rate(diamante_votes_total[1m])",
						Format:       "time_series",
						LegendFormat: "{{type}}",
						RefID:        "A",
					},
				},
			},
		},
	}, nil
}

// generateNetworkDashboard generates the network dashboard
func (dg *DashboardGenerator) generateNetworkDashboard() (*GrafanaDashboard, error) {
	return &GrafanaDashboard{
		Title:         "Diamante Network Metrics",
		Tags:          []string{"diamante", "network"},
		Style:         "dark",
		Timezone:      "browser",
		Editable:      true,
		SchemaVersion: 27,
		Version:       1,
		Time: GrafanaTime{
			From: "now-1h",
			To:   "now",
		},
		Refresh: dg.config.RefreshRate,
		Panels: []GrafanaPanel{
			// Network Messages
			{
				ID:         1,
				Title:      "Network Messages",
				Type:       "graph",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 12, X: 0, Y: 0},
				Targets: []GrafanaTarget{
					{
						Expr:         "rate(diamante_network_messages_total[1m])",
						Format:       "time_series",
						LegendFormat: "{{type}} {{direction}}",
						RefID:        "A",
					},
				},
			},
			// Network Latency
			{
				ID:         2,
				Title:      "Network Latency",
				Type:       "graph",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 12, X: 12, Y: 0},
				Targets: []GrafanaTarget{
					{
						Expr:         "histogram_quantile(0.95, diamante_network_latency_bucket)",
						Format:       "time_series",
						LegendFormat: "95th percentile",
						RefID:        "A",
					},
				},
				YAxes: []GrafanaAxis{
					{Label: "Latency", Unit: "s"},
					{Label: "", Unit: "short"},
				},
			},
		},
	}, nil
}

// generateStorageDashboard generates the storage dashboard
func (dg *DashboardGenerator) generateStorageDashboard() (*GrafanaDashboard, error) {
	return &GrafanaDashboard{
		Title:         "Diamante Storage Metrics",
		Tags:          []string{"diamante", "storage"},
		Style:         "dark",
		Timezone:      "browser",
		Editable:      true,
		SchemaVersion: 27,
		Version:       1,
		Time: GrafanaTime{
			From: "now-1h",
			To:   "now",
		},
		Refresh: dg.config.RefreshRate,
		Panels: []GrafanaPanel{
			// Storage Operations
			{
				ID:         1,
				Title:      "Storage Operations",
				Type:       "graph",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 12, X: 0, Y: 0},
				Targets: []GrafanaTarget{
					{
						Expr:         "rate(diamante_storage_operations_total[1m])",
						Format:       "time_series",
						LegendFormat: "{{operation}} {{status}}",
						RefID:        "A",
					},
				},
			},
			// Storage Size
			{
				ID:         2,
				Title:      "Storage Size",
				Type:       "graph",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 12, X: 12, Y: 0},
				Targets: []GrafanaTarget{
					{
						Expr:         "diamante_storage_size_bytes",
						Format:       "time_series",
						LegendFormat: "{{type}}",
						RefID:        "A",
					},
				},
				YAxes: []GrafanaAxis{
					{Label: "Size", Unit: "bytes"},
					{Label: "", Unit: "short"},
				},
			},
		},
	}, nil
}

// generateSecurityDashboard generates the security dashboard
func (dg *DashboardGenerator) generateSecurityDashboard() (*GrafanaDashboard, error) {
	return &GrafanaDashboard{
		Title:         "Diamante Security Metrics",
		Tags:          []string{"diamante", "security"},
		Style:         "dark",
		Timezone:      "browser",
		Editable:      true,
		SchemaVersion: 27,
		Version:       1,
		Time: GrafanaTime{
			From: "now-1h",
			To:   "now",
		},
		Refresh: dg.config.RefreshRate,
		Panels: []GrafanaPanel{
			// Security Events
			{
				ID:         1,
				Title:      "Security Events",
				Type:       "graph",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 12, X: 0, Y: 0},
				Targets: []GrafanaTarget{
					{
						Expr:         "rate(diamante_security_events_total[1m])",
						Format:       "time_series",
						LegendFormat: "{{type}} {{severity}}",
						RefID:        "A",
					},
				},
			},
			// Threat Level
			{
				ID:         2,
				Title:      "Threat Level",
				Type:       "stat",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 12, X: 12, Y: 0},
				Targets: []GrafanaTarget{
					{
						Expr:         "diamante_threat_level",
						Format:       "time_series",
						LegendFormat: "Level",
						RefID:        "A",
					},
				},
			},
		},
	}, nil
}

// generateSystemDashboard generates the system dashboard
func (dg *DashboardGenerator) generateSystemDashboard() (*GrafanaDashboard, error) {
	return &GrafanaDashboard{
		Title:         "Diamante System Metrics",
		Tags:          []string{"diamante", "system"},
		Style:         "dark",
		Timezone:      "browser",
		Editable:      true,
		SchemaVersion: 27,
		Version:       1,
		Time: GrafanaTime{
			From: "now-1h",
			To:   "now",
		},
		Refresh: dg.config.RefreshRate,
		Panels: []GrafanaPanel{
			// CPU Usage
			{
				ID:         1,
				Title:      "CPU Usage",
				Type:       "graph",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 12, X: 0, Y: 0},
				Targets: []GrafanaTarget{
					{
						Expr:         "diamante_system_cpu_usage",
						Format:       "time_series",
						LegendFormat: "CPU %",
						RefID:        "A",
					},
				},
				YAxes: []GrafanaAxis{
					{Label: "Percentage", Unit: "percent", Min: stringPtr("0"), Max: stringPtr("100")},
					{Label: "", Unit: "short"},
				},
			},
			// Memory Usage
			{
				ID:         2,
				Title:      "Memory Usage",
				Type:       "graph",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 12, X: 12, Y: 0},
				Targets: []GrafanaTarget{
					{
						Expr:         "diamante_system_memory_usage",
						Format:       "time_series",
						LegendFormat: "{{type}}",
						RefID:        "A",
					},
				},
				YAxes: []GrafanaAxis{
					{Label: "Bytes", Unit: "bytes"},
					{Label: "", Unit: "short"},
				},
			},
			// Goroutines
			{
				ID:         3,
				Title:      "Goroutines",
				Type:       "graph",
				DataSource: dg.config.DataSource,
				GridPos:    GrafanaGridPos{H: 8, W: 24, X: 0, Y: 8},
				Targets: []GrafanaTarget{
					{
						Expr:         "diamante_system_goroutines",
						Format:       "time_series",
						LegendFormat: "Goroutines",
						RefID:        "A",
					},
				},
			},
		},
	}, nil
}

// saveDashboard saves a dashboard to file
func (dg *DashboardGenerator) saveDashboard(name string, dashboard *GrafanaDashboard) error {
	data, err := json.MarshalIndent(dashboard, "", "  ")
	if err != nil {
		return err
	}

	filename := filepath.Join(dg.config.OutputDir, name+".json")
	return os.WriteFile(filename, data, 0644)
}

// Helper function
func stringPtr(s string) *string {
	return &s
}
