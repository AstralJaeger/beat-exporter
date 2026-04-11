// Package collector provides OpenTelemetry-based metric collection from Elastic Beat HTTP endpoints.
package collector

// BeatInfo holds the identity information returned by the beat's root HTTP endpoint.
type BeatInfo struct {
	Beat     string `json:"beat"`
	Hostname string `json:"hostname"`
	Name     string `json:"name"`
	UUID     string `json:"uuid"`
	Version  string `json:"version"`
}

// Stats is the top-level structure of the /stats endpoint response.
type Stats struct {
	System     System      `json:"system"`
	Beat       BeatStats   `json:"beat"`
	LibBeat    LibBeat     `json:"libbeat"`
	Registrar  Registrar   `json:"registrar"`
	Filebeat   Filebeat    `json:"filebeat"`
	Metricbeat Metricbeat  `json:"metricbeat"`
	Auditd     AuditdStats `json:"auditd"`
}

// ---- beat stats ----

// CPUTimings holds CPU usage timing data.
type CPUTimings struct {
	Ticks float64 `json:"ticks"`
	Time  struct {
		MS float64 `json:"ms"`
	} `json:"time"`
	Value float64 `json:"value"`
}

// BeatStats holds per-beat resource usage statistics.
type BeatStats struct {
	CPU struct {
		System CPUTimings `json:"system"`
		Total  CPUTimings `json:"total"`
		User   CPUTimings `json:"user"`
	} `json:"cpu"`
	Info struct {
		Uptime struct {
			MS float64 `json:"ms"`
		} `json:"uptime"`
		EphemeralID string `json:"ephemeral_id"`
	} `json:"info"`
	Memstats struct {
		GCNext      float64 `json:"gc_next"`
		MemoryAlloc float64 `json:"memory_alloc"`
		MemoryTotal float64 `json:"memory_total"`
		RSS         float64 `json:"rss"`
	} `json:"memstats"`
	Runtime struct {
		Goroutines uint64 `json:"goroutines"`
	} `json:"runtime"`
}

// ---- libbeat stats ----

// LibBeat holds libbeat pipeline and output statistics.
type LibBeat struct {
	Config struct {
		Module struct {
			Running float64 `json:"running"`
			Starts  float64 `json:"starts"`
			Stops   float64 `json:"stops"`
		} `json:"module"`
		Reloads float64 `json:"reloads"`
	} `json:"config"`
	Output   LibBeatOutput   `json:"output"`
	Pipeline LibBeatPipeline `json:"pipeline"`
}

// LibBeatEvents holds event counters.
type LibBeatEvents struct {
	Acked      float64 `json:"acked"`
	Active     float64 `json:"active"`
	Batches    float64 `json:"batches"`
	Dropped    float64 `json:"dropped"`
	Duplicates float64 `json:"duplicates"`
	Failed     float64 `json:"failed"`
	Filtered   float64 `json:"filtered"`
	Published  float64 `json:"published"`
	Retry      float64 `json:"retry"`
}

// LibBeatOutputBytesErrors holds byte and error counts for output I/O.
type LibBeatOutputBytesErrors struct {
	Bytes  float64 `json:"bytes"`
	Errors float64 `json:"errors"`
}

// LibBeatOutput holds libbeat output statistics.
type LibBeatOutput struct {
	Events LibBeatEvents            `json:"events"`
	Read   LibBeatOutputBytesErrors `json:"read"`
	Write  LibBeatOutputBytesErrors `json:"write"`
	Type   string                   `json:"type"`
}

// LibBeatPipeline holds libbeat pipeline statistics.
type LibBeatPipeline struct {
	Clients float64       `json:"clients"`
	Events  LibBeatEvents `json:"events"`
	Queue   struct {
		Acked float64 `json:"acked"`
	} `json:"queue"`
}

// ---- system stats ----

// CPUStats holds CPU load averages.
type CPUStats struct {
	M1  float64 `json:"1"`
	M5  float64 `json:"5"`
	M15 float64 `json:"15"`
}

// System holds system-level statistics (CPU cores, load averages).
type System struct {
	CPU struct {
		Cores int64 `json:"cores"`
	} `json:"cpu"`
	Load struct {
		CPUStats
		Norm CPUStats `json:"norm"`
	} `json:"load"`
}

// ---- registrar stats (filebeat) ----

// Registrar holds filebeat registrar statistics.
type Registrar struct {
	Writes struct {
		Fail    float64 `json:"fail"`
		Success float64 `json:"success"`
		Total   float64 `json:"total"`
	} `json:"writes"`
	States struct {
		Cleanup float64 `json:"cleanup"`
		Current float64 `json:"current"`
		Update  float64 `json:"update"`
	} `json:"states"`
}

// ---- filebeat stats ----

// Filebeat holds filebeat-specific statistics.
type Filebeat struct {
	Events struct {
		Active float64 `json:"active"`
		Added  float64 `json:"added"`
		Done   float64 `json:"done"`
	} `json:"events"`
	Harvester struct {
		Closed    float64 `json:"closed"`
		OpenFiles float64 `json:"open_files"`
		Running   float64 `json:"running"`
		Skipped   float64 `json:"skipped"`
		Started   float64 `json:"started"`
	} `json:"harvester"`
	Input struct {
		Log struct {
			Files struct {
				Renamed   float64 `json:"renamed"`
				Truncated float64 `json:"truncated"`
			} `json:"files"`
		} `json:"log"`
	} `json:"input"`
}

// ---- metricbeat stats ----

// MetricbeatEvent holds success/failure counts for a metricbeat module event.
type MetricbeatEvent struct {
	Failures float64 `json:"failures"`
	Success  float64 `json:"success"`
}

// Metricbeat holds metricbeat-specific statistics.
type Metricbeat struct {
	System struct {
		CPU            MetricbeatEvent `json:"cpu"`
		Filesystem     MetricbeatEvent `json:"filesystem"`
		Fsstat         MetricbeatEvent `json:"fsstat"`
		Load           MetricbeatEvent `json:"load"`
		Memory         MetricbeatEvent `json:"memory"`
		Network        MetricbeatEvent `json:"network"`
		Process        MetricbeatEvent `json:"process"`
		ProcessSummary MetricbeatEvent `json:"process_summary"`
		Uptime         MetricbeatEvent `json:"uptime"`
	} `json:"system"`
}

// ---- auditbeat stats ----

// AuditdStats holds auditd-specific statistics.
type AuditdStats struct {
	KernelLost         float64 `json:"kernel_lost"`
	ReassemblerSeqGaps float64 `json:"reassembler_seq_gaps"`
	ReceivedMsgs       float64 `json:"received_msgs"`
	UserspaceLost      float64 `json:"userspace_lost"`
}
