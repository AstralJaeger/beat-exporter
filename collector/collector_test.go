package collector_test

import (
	"encoding/json"
	"testing"

	"github.com/AstralJaeger/beat-exporter/collector"
)

func TestBeatInfoUnmarshal(t *testing.T) {
	raw := `{"beat":"filebeat","hostname":"host1","name":"fb1","uuid":"abc-123","version":"8.0.0"}`
	var info collector.BeatInfo
	if err := json.Unmarshal([]byte(raw), &info); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Beat != "filebeat" {
		t.Errorf("got Beat=%q, want filebeat", info.Beat)
	}
	if info.Version != "8.0.0" {
		t.Errorf("got Version=%q, want 8.0.0", info.Version)
	}
}

func TestStatsUnmarshal(t *testing.T) {
	raw := `{
		"beat":{"cpu":{"system":{"ticks":100,"time":{"ms":500}},"user":{"ticks":200,"time":{"ms":1000}}},
		        "info":{"uptime":{"ms":60000}},
		        "memstats":{"gc_next":1024,"memory_alloc":2048,"memory_total":4096,"rss":8192},
		        "runtime":{"goroutines":42}},
		"libbeat":{"output":{"type":"elasticsearch"}},
		"system":{"cpu":{"cores":4},"load":{"1":0.5,"5":0.3,"15":0.2,"norm":{"1":0.1,"5":0.07,"15":0.05}}}
	}`
	var s collector.Stats
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Beat.CPU.System.Ticks != 100 {
		t.Errorf("got System.Ticks=%v, want 100", s.Beat.CPU.System.Ticks)
	}
	if s.Beat.Runtime.Goroutines != 42 {
		t.Errorf("got Goroutines=%v, want 42", s.Beat.Runtime.Goroutines)
	}
	if s.LibBeat.Output.Type != "elasticsearch" {
		t.Errorf("got Output.Type=%q, want elasticsearch", s.LibBeat.Output.Type)
	}
	if s.System.CPU.Cores != 4 {
		t.Errorf("got CPU.Cores=%v, want 4", s.System.CPU.Cores)
	}
}

func TestFilebeatStatsUnmarshal(t *testing.T) {
	raw := `{
		"filebeat":{"events":{"active":5,"added":10,"done":3},
		            "harvester":{"closed":1,"open_files":2,"running":3,"skipped":0,"started":5}},
		"registrar":{"writes":{"fail":0,"success":10,"total":10},
		             "states":{"cleanup":0,"current":100,"update":5}}
	}`
	var s collector.Stats
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Filebeat.Events.Active != 5 {
		t.Errorf("got Events.Active=%v, want 5", s.Filebeat.Events.Active)
	}
	if s.Registrar.Writes.Total != 10 {
		t.Errorf("got Writes.Total=%v, want 10", s.Registrar.Writes.Total)
	}
}
