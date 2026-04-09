package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// hackfixRegex normalises the filebeat /stats JSON where `"time": 123` must
// become `"time": {"ms": 123}` to match all other beats.
var hackfixRegex = regexp.MustCompile(`"time":(\d+)`)

// BeatCollector fetches stats from a beat's HTTP endpoint and exposes them as
// OpenTelemetry observable instruments.
type BeatCollector struct {
	mu         sync.RWMutex
	stats      Stats
	beatInfo   BeatInfo
	client     *http.Client
	beatURL    *url.URL
	systemBeat bool
	up         bool
}

// NewBeatCollector creates a BeatCollector, registers all OTel instruments and
// their observation callbacks against meter, and returns the collector.
//
// The caller is responsible for shutting down the MeterProvider when done.
func NewBeatCollector(
	meter metric.Meter,
	client *http.Client,
	beatURL *url.URL,
	beatInfo *BeatInfo,
	systemBeat bool,
) (*BeatCollector, error) {
	bc := &BeatCollector{
		client:     client,
		beatURL:    beatURL,
		beatInfo:   *beatInfo,
		systemBeat: systemBeat,
	}

	beat := beatInfo.Beat

	// ---- target up / info ----
	upGauge, err := meter.Float64ObservableGauge(beat+"_up",
		metric.WithDescription("1 if the beat target is reachable, 0 otherwise"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating %s_up gauge: %w", beat, err)
	}

	instance := fmt.Sprintf("%s:%s", beatURL.Hostname(), beatURL.Port())
	targetInfo, err := meter.Float64ObservableGauge("beat_exporter_target_info",
		metric.WithDescription("Target information (version, beat type, URI)"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating beat_exporter_target_info gauge: %w", err)
	}

	// ---- beat cpu / memory / runtime ----
	cpuTimeSys, err := meter.Float64ObservableCounter(beat+"_cpu_time_seconds",
		metric.WithDescription("beat.cpu.time (system mode)"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}
	cpuTimeUser, err := meter.Float64ObservableCounter(beat+"_cpu_time_user_seconds",
		metric.WithDescription("beat.cpu.time (user mode)"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}
	cpuTicksSys, err := meter.Float64ObservableCounter(beat+"_cpu_ticks_system",
		metric.WithDescription("beat.cpu.ticks (system mode)"),
	)
	if err != nil {
		return nil, err
	}
	cpuTicksUser, err := meter.Float64ObservableCounter(beat+"_cpu_ticks_user",
		metric.WithDescription("beat.cpu.ticks (user mode)"),
	)
	if err != nil {
		return nil, err
	}
	uptime, err := meter.Float64ObservableCounter(beat+"_uptime_seconds",
		metric.WithDescription("beat.info.uptime"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}
	gcNext, err := meter.Float64ObservableCounter(beat+"_memstats_gc_next",
		metric.WithDescription("beat.memstats.gc_next"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}
	memAlloc, err := meter.Float64ObservableGauge(beat+"_memstats_memory_alloc",
		metric.WithDescription("beat.memstats.memory_alloc"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}
	memTotal, err := meter.Float64ObservableGauge(beat+"_memstats_memory_total",
		metric.WithDescription("beat.memstats.memory_total"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}
	rss, err := meter.Float64ObservableGauge(beat+"_memstats_rss",
		metric.WithDescription("beat.memstats.rss"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}
	goroutines, err := meter.Float64ObservableGauge(beat+"_runtime_goroutines",
		metric.WithDescription("beat.runtime.goroutines"),
	)
	if err != nil {
		return nil, err
	}

	// ---- libbeat ----
	lbConfigReloads, err := meter.Float64ObservableCounter(beat+"_libbeat_config_reloads",
		metric.WithDescription("libbeat.config.reloads"),
	)
	if err != nil {
		return nil, err
	}
	lbConfigModuleRunning, err := meter.Float64ObservableGauge(beat+"_libbeat_config_module_running",
		metric.WithDescription("libbeat.config.module.running"),
	)
	if err != nil {
		return nil, err
	}
	lbConfigModuleStarts, err := meter.Float64ObservableCounter(beat+"_libbeat_config_module_starts",
		metric.WithDescription("libbeat.config.module.starts"),
	)
	if err != nil {
		return nil, err
	}
	lbConfigModuleStops, err := meter.Float64ObservableCounter(beat+"_libbeat_config_module_stops",
		metric.WithDescription("libbeat.config.module.stops"),
	)
	if err != nil {
		return nil, err
	}
	lbOutReadBytes, err := meter.Float64ObservableCounter(beat+"_libbeat_output_read_bytes",
		metric.WithDescription("libbeat.output.read.bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}
	lbOutReadErrors, err := meter.Float64ObservableCounter(beat+"_libbeat_output_read_errors",
		metric.WithDescription("libbeat.output.read.errors"),
	)
	if err != nil {
		return nil, err
	}
	lbOutWriteBytes, err := meter.Float64ObservableCounter(beat+"_libbeat_output_write_bytes",
		metric.WithDescription("libbeat.output.write.bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}
	lbOutWriteErrors, err := meter.Float64ObservableCounter(beat+"_libbeat_output_write_errors",
		metric.WithDescription("libbeat.output.write.errors"),
	)
	if err != nil {
		return nil, err
	}
	lbOutEventsAcked, err := meter.Float64ObservableGauge(beat+"_libbeat_output_events_acked",
		metric.WithDescription("libbeat.output.events.acked"),
	)
	if err != nil {
		return nil, err
	}
	lbOutEventsActive, err := meter.Float64ObservableGauge(beat+"_libbeat_output_events_active",
		metric.WithDescription("libbeat.output.events.active"),
	)
	if err != nil {
		return nil, err
	}
	lbOutEventsBatches, err := meter.Float64ObservableGauge(beat+"_libbeat_output_events_batches",
		metric.WithDescription("libbeat.output.events.batches"),
	)
	if err != nil {
		return nil, err
	}
	lbOutEventsDropped, err := meter.Float64ObservableGauge(beat+"_libbeat_output_events_dropped",
		metric.WithDescription("libbeat.output.events.dropped"),
	)
	if err != nil {
		return nil, err
	}
	lbOutEventsDuplicates, err := meter.Float64ObservableGauge(beat+"_libbeat_output_events_duplicates",
		metric.WithDescription("libbeat.output.events.duplicates"),
	)
	if err != nil {
		return nil, err
	}
	lbOutEventsFailed, err := meter.Float64ObservableGauge(beat+"_libbeat_output_events_failed",
		metric.WithDescription("libbeat.output.events.failed"),
	)
	if err != nil {
		return nil, err
	}
	lbOutType, err := meter.Float64ObservableGauge(beat+"_libbeat_output_type",
		metric.WithDescription("libbeat.output.type"),
	)
	if err != nil {
		return nil, err
	}
	lbPipelineClients, err := meter.Float64ObservableGauge(beat+"_libbeat_pipeline_clients",
		metric.WithDescription("libbeat.pipeline.clients"),
	)
	if err != nil {
		return nil, err
	}
	lbPipelineQueueAcked, err := meter.Float64ObservableGauge(beat+"_libbeat_pipeline_queue_acked",
		metric.WithDescription("libbeat.pipeline.queue.acked"),
	)
	if err != nil {
		return nil, err
	}
	lbPipelineEventsActive, err := meter.Float64ObservableGauge(beat+"_libbeat_pipeline_events_active",
		metric.WithDescription("libbeat.pipeline.events.active"),
	)
	if err != nil {
		return nil, err
	}
	lbPipelineEventsDropped, err := meter.Float64ObservableGauge(beat+"_libbeat_pipeline_events_dropped",
		metric.WithDescription("libbeat.pipeline.events.dropped"),
	)
	if err != nil {
		return nil, err
	}
	lbPipelineEventsFailed, err := meter.Float64ObservableGauge(beat+"_libbeat_pipeline_events_failed",
		metric.WithDescription("libbeat.pipeline.events.failed"),
	)
	if err != nil {
		return nil, err
	}
	lbPipelineEventsFiltered, err := meter.Float64ObservableGauge(beat+"_libbeat_pipeline_events_filtered",
		metric.WithDescription("libbeat.pipeline.events.filtered"),
	)
	if err != nil {
		return nil, err
	}
	lbPipelineEventsPublished, err := meter.Float64ObservableGauge(beat+"_libbeat_pipeline_events_published",
		metric.WithDescription("libbeat.pipeline.events.published"),
	)
	if err != nil {
		return nil, err
	}
	lbPipelineEventsRetry, err := meter.Float64ObservableGauge(beat+"_libbeat_pipeline_events_retry",
		metric.WithDescription("libbeat.pipeline.events.retry"),
	)
	if err != nil {
		return nil, err
	}

	// ---- system (optional) ----
	var (
		sysCPUCores                                             metric.Float64ObservableCounter
		sysLoad1, sysLoad5, sysLoad15                          metric.Float64ObservableGauge
		sysLoadNorm1, sysLoadNorm5, sysLoadNorm15              metric.Float64ObservableGauge
	)
	if systemBeat {
		sysCPUCores, err = meter.Float64ObservableCounter(beat+"_system_cpu_cores",
			metric.WithDescription("system.cpu.cores"),
		)
		if err != nil {
			return nil, err
		}
		sysLoad1, err = meter.Float64ObservableGauge(beat+"_system_load_1",
			metric.WithDescription("system.load.1"),
		)
		if err != nil {
			return nil, err
		}
		sysLoad5, err = meter.Float64ObservableGauge(beat+"_system_load_5",
			metric.WithDescription("system.load.5"),
		)
		if err != nil {
			return nil, err
		}
		sysLoad15, err = meter.Float64ObservableGauge(beat+"_system_load_15",
			metric.WithDescription("system.load.15"),
		)
		if err != nil {
			return nil, err
		}
		sysLoadNorm1, err = meter.Float64ObservableGauge(beat+"_system_load_norm_1",
			metric.WithDescription("system.load.norm.1"),
		)
		if err != nil {
			return nil, err
		}
		sysLoadNorm5, err = meter.Float64ObservableGauge(beat+"_system_load_norm_5",
			metric.WithDescription("system.load.norm.5"),
		)
		if err != nil {
			return nil, err
		}
		sysLoadNorm15, err = meter.Float64ObservableGauge(beat+"_system_load_norm_15",
			metric.WithDescription("system.load.norm.15"),
		)
		if err != nil {
			return nil, err
		}
	}

	// ---- auditbeat ----
	auditKernelLost, err := meter.Float64ObservableGauge(beat+"_auditd_kernel_lost",
		metric.WithDescription("auditd.kernel_lost"),
	)
	if err != nil {
		return nil, err
	}
	auditReassemblerSeqGaps, err := meter.Float64ObservableGauge(beat+"_auditd_reassembler_seq_gaps",
		metric.WithDescription("auditd.reassembler_seq_gaps"),
	)
	if err != nil {
		return nil, err
	}
	auditReceivedMsgs, err := meter.Float64ObservableGauge(beat+"_auditd_received_msgs",
		metric.WithDescription("auditd.received_msgs"),
	)
	if err != nil {
		return nil, err
	}
	auditUserspaceLost, err := meter.Float64ObservableGauge(beat+"_auditd_userspace_lost",
		metric.WithDescription("auditd.userspace_lost"),
	)
	if err != nil {
		return nil, err
	}

	// ---- filebeat-specific ----
	var (
		fbEventsActive, fbEventsAdded, fbEventsDone               metric.Float64ObservableGauge
		fbHarvClosed, fbHarvOpenFiles, fbHarvRunning               metric.Float64ObservableGauge
		fbHarvSkipped, fbHarvStarted                               metric.Float64ObservableGauge
		fbInputLogRenamed, fbInputLogTruncated                     metric.Float64ObservableGauge
		regWritesFail, regWritesSuccess, regWritesTotal            metric.Float64ObservableGauge
		regStatesCleanup, regStatesCurrent, regStatesUpdate        metric.Float64ObservableGauge
	)
	if beat == "filebeat" {
		fbEventsActive, err = meter.Float64ObservableGauge(beat+"_filebeat_events_active",
			metric.WithDescription("filebeat.events.active"),
		)
		if err != nil {
			return nil, err
		}
		fbEventsAdded, err = meter.Float64ObservableGauge(beat+"_filebeat_events_added",
			metric.WithDescription("filebeat.events.added"),
		)
		if err != nil {
			return nil, err
		}
		fbEventsDone, err = meter.Float64ObservableGauge(beat+"_filebeat_events_done",
			metric.WithDescription("filebeat.events.done"),
		)
		if err != nil {
			return nil, err
		}
		fbHarvClosed, err = meter.Float64ObservableGauge(beat+"_filebeat_harvester_closed",
			metric.WithDescription("filebeat.harvester.closed"),
		)
		if err != nil {
			return nil, err
		}
		fbHarvOpenFiles, err = meter.Float64ObservableGauge(beat+"_filebeat_harvester_open_files",
			metric.WithDescription("filebeat.harvester.open_files"),
		)
		if err != nil {
			return nil, err
		}
		fbHarvRunning, err = meter.Float64ObservableGauge(beat+"_filebeat_harvester_running",
			metric.WithDescription("filebeat.harvester.running"),
		)
		if err != nil {
			return nil, err
		}
		fbHarvSkipped, err = meter.Float64ObservableGauge(beat+"_filebeat_harvester_skipped",
			metric.WithDescription("filebeat.harvester.skipped"),
		)
		if err != nil {
			return nil, err
		}
		fbHarvStarted, err = meter.Float64ObservableGauge(beat+"_filebeat_harvester_started",
			metric.WithDescription("filebeat.harvester.started"),
		)
		if err != nil {
			return nil, err
		}
		fbInputLogRenamed, err = meter.Float64ObservableGauge(beat+"_filebeat_input_log_files_renamed",
			metric.WithDescription("filebeat.input.log.files.renamed"),
		)
		if err != nil {
			return nil, err
		}
		fbInputLogTruncated, err = meter.Float64ObservableGauge(beat+"_filebeat_input_log_files_truncated",
			metric.WithDescription("filebeat.input.log.files.truncated"),
		)
		if err != nil {
			return nil, err
		}
		regWritesFail, err = meter.Float64ObservableGauge(beat+"_registrar_writes_fail",
			metric.WithDescription("registrar.writes.fail"),
		)
		if err != nil {
			return nil, err
		}
		regWritesSuccess, err = meter.Float64ObservableGauge(beat+"_registrar_writes_success",
			metric.WithDescription("registrar.writes.success"),
		)
		if err != nil {
			return nil, err
		}
		regWritesTotal, err = meter.Float64ObservableGauge(beat+"_registrar_writes_total",
			metric.WithDescription("registrar.writes.total"),
		)
		if err != nil {
			return nil, err
		}
		regStatesCleanup, err = meter.Float64ObservableGauge(beat+"_registrar_states_cleanup",
			metric.WithDescription("registrar.states.cleanup"),
		)
		if err != nil {
			return nil, err
		}
		regStatesCurrent, err = meter.Float64ObservableGauge(beat+"_registrar_states_current",
			metric.WithDescription("registrar.states.current"),
		)
		if err != nil {
			return nil, err
		}
		regStatesUpdate, err = meter.Float64ObservableGauge(beat+"_registrar_states_update",
			metric.WithDescription("registrar.states.update"),
		)
		if err != nil {
			return nil, err
		}
	}

	// ---- metricbeat-specific ----
	var (
		mbCPU, mbFilesystem, mbFsstat, mbLoad, mbMemory metric.Float64ObservableCounter
		mbNetwork, mbProcess, mbProcessSummary, mbUptime metric.Float64ObservableCounter
	)
	if beat == "metricbeat" {
		makeCounter := func(name, desc string) (metric.Float64ObservableCounter, error) {
			return meter.Float64ObservableCounter(name, metric.WithDescription(desc))
		}
		mbCPU, err = makeCounter(beat+"_metricbeat_system_cpu", "system.cpu")
		if err != nil {
			return nil, err
		}
		mbFilesystem, err = makeCounter(beat+"_metricbeat_system_filesystem", "system.filesystem")
		if err != nil {
			return nil, err
		}
		mbFsstat, err = makeCounter(beat+"_metricbeat_system_fsstat", "system.fsstat")
		if err != nil {
			return nil, err
		}
		mbLoad, err = makeCounter(beat+"_metricbeat_system_load", "system.load")
		if err != nil {
			return nil, err
		}
		mbMemory, err = makeCounter(beat+"_metricbeat_system_memory", "system.memory")
		if err != nil {
			return nil, err
		}
		mbNetwork, err = makeCounter(beat+"_metricbeat_system_network", "system.network")
		if err != nil {
			return nil, err
		}
		mbProcess, err = makeCounter(beat+"_metricbeat_system_process", "system.process")
		if err != nil {
			return nil, err
		}
		mbProcessSummary, err = makeCounter(beat+"_metricbeat_system_process_summary", "system.process_summary")
		if err != nil {
			return nil, err
		}
		mbUptime, err = makeCounter(beat+"_metricbeat_system_uptime", "system.uptime")
		if err != nil {
			return nil, err
		}
	}

	// Collect all observable instruments for the callback registration.
	instruments := []metric.Observable{
		upGauge, targetInfo,
		cpuTimeSys, cpuTimeUser, cpuTicksSys, cpuTicksUser,
		uptime, gcNext,
		memAlloc, memTotal, rss, goroutines,
		lbConfigReloads, lbConfigModuleRunning, lbConfigModuleStarts, lbConfigModuleStops,
		lbOutReadBytes, lbOutReadErrors, lbOutWriteBytes, lbOutWriteErrors,
		lbOutEventsAcked, lbOutEventsActive, lbOutEventsBatches,
		lbOutEventsDropped, lbOutEventsDuplicates, lbOutEventsFailed,
		lbOutType,
		lbPipelineClients, lbPipelineQueueAcked,
		lbPipelineEventsActive, lbPipelineEventsDropped, lbPipelineEventsFailed,
		lbPipelineEventsFiltered, lbPipelineEventsPublished, lbPipelineEventsRetry,
		auditKernelLost, auditReassemblerSeqGaps, auditReceivedMsgs, auditUserspaceLost,
	}
	if systemBeat {
		instruments = append(instruments,
			sysCPUCores,
			sysLoad1, sysLoad5, sysLoad15,
			sysLoadNorm1, sysLoadNorm5, sysLoadNorm15,
		)
	}
	if beat == "filebeat" {
		instruments = append(instruments,
			fbEventsActive, fbEventsAdded, fbEventsDone,
			fbHarvClosed, fbHarvOpenFiles, fbHarvRunning, fbHarvSkipped, fbHarvStarted,
			fbInputLogRenamed, fbInputLogTruncated,
			regWritesFail, regWritesSuccess, regWritesTotal,
			regStatesCleanup, regStatesCurrent, regStatesUpdate,
		)
	}
	if beat == "metricbeat" {
		instruments = append(instruments,
			mbCPU, mbFilesystem, mbFsstat, mbLoad, mbMemory,
			mbNetwork, mbProcess, mbProcessSummary, mbUptime,
		)
	}

	infoAttrs := metric.WithAttributes(
		attribute.String("version", beatInfo.Version),
		attribute.String("beat", beat),
		attribute.String("uri", instance),
	)
	attrSuccess := metric.WithAttributes(attribute.String("event", "success"))
	attrFailures := metric.WithAttributes(attribute.String("event", "failures"))

	_, err = meter.RegisterCallback(func(_ context.Context, obs metric.Observer) error {
		if fetchErr := bc.fetchStats(); fetchErr != nil {
			obs.ObserveFloat64(upGauge, 0)
			slog.Error("Failed to fetch beat stats", "err", fetchErr, "url", beatURL.String())
			return nil
		}

		bc.mu.RLock()
		s := bc.stats
		bc.mu.RUnlock()

		obs.ObserveFloat64(upGauge, 1)
		obs.ObserveFloat64(targetInfo, 1, infoAttrs)

		// beat cpu/memory/runtime
		obs.ObserveFloat64(cpuTimeSys, msToSeconds(s.Beat.CPU.System.Time.MS))
		obs.ObserveFloat64(cpuTimeUser, msToSeconds(s.Beat.CPU.User.Time.MS))
		obs.ObserveFloat64(cpuTicksSys, s.Beat.CPU.System.Ticks)
		obs.ObserveFloat64(cpuTicksUser, s.Beat.CPU.User.Ticks)
		obs.ObserveFloat64(uptime, msToSeconds(s.Beat.Info.Uptime.MS))
		obs.ObserveFloat64(gcNext, s.Beat.Memstats.GCNext)
		obs.ObserveFloat64(memAlloc, s.Beat.Memstats.MemoryAlloc)
		obs.ObserveFloat64(memTotal, s.Beat.Memstats.MemoryTotal)
		obs.ObserveFloat64(rss, s.Beat.Memstats.RSS)
		obs.ObserveFloat64(goroutines, float64(s.Beat.Runtime.Goroutines))

		// libbeat
		obs.ObserveFloat64(lbConfigReloads, s.LibBeat.Config.Reloads)
		obs.ObserveFloat64(lbConfigModuleRunning, s.LibBeat.Config.Module.Running)
		obs.ObserveFloat64(lbConfigModuleStarts, s.LibBeat.Config.Module.Starts)
		obs.ObserveFloat64(lbConfigModuleStops, s.LibBeat.Config.Module.Stops)
		obs.ObserveFloat64(lbOutReadBytes, s.LibBeat.Output.Read.Bytes)
		obs.ObserveFloat64(lbOutReadErrors, s.LibBeat.Output.Read.Errors)
		obs.ObserveFloat64(lbOutWriteBytes, s.LibBeat.Output.Write.Bytes)
		obs.ObserveFloat64(lbOutWriteErrors, s.LibBeat.Output.Write.Errors)
		obs.ObserveFloat64(lbOutEventsAcked, s.LibBeat.Output.Events.Acked)
		obs.ObserveFloat64(lbOutEventsActive, s.LibBeat.Output.Events.Active)
		obs.ObserveFloat64(lbOutEventsBatches, s.LibBeat.Output.Events.Batches)
		obs.ObserveFloat64(lbOutEventsDropped, s.LibBeat.Output.Events.Dropped)
		obs.ObserveFloat64(lbOutEventsDuplicates, s.LibBeat.Output.Events.Duplicates)
		obs.ObserveFloat64(lbOutEventsFailed, s.LibBeat.Output.Events.Failed)
		obs.ObserveFloat64(lbOutType, 1,
			metric.WithAttributes(attribute.String("type", s.LibBeat.Output.Type)))
		obs.ObserveFloat64(lbPipelineClients, s.LibBeat.Pipeline.Clients)
		obs.ObserveFloat64(lbPipelineQueueAcked, s.LibBeat.Pipeline.Queue.Acked)
		obs.ObserveFloat64(lbPipelineEventsActive, s.LibBeat.Pipeline.Events.Active)
		obs.ObserveFloat64(lbPipelineEventsDropped, s.LibBeat.Pipeline.Events.Dropped)
		obs.ObserveFloat64(lbPipelineEventsFailed, s.LibBeat.Pipeline.Events.Failed)
		obs.ObserveFloat64(lbPipelineEventsFiltered, s.LibBeat.Pipeline.Events.Filtered)
		obs.ObserveFloat64(lbPipelineEventsPublished, s.LibBeat.Pipeline.Events.Published)
		obs.ObserveFloat64(lbPipelineEventsRetry, s.LibBeat.Pipeline.Events.Retry)

		// auditd
		obs.ObserveFloat64(auditKernelLost, s.Auditd.KernelLost)
		obs.ObserveFloat64(auditReassemblerSeqGaps, s.Auditd.ReassemblerSeqGaps)
		obs.ObserveFloat64(auditReceivedMsgs, s.Auditd.ReceivedMsgs)
		obs.ObserveFloat64(auditUserspaceLost, s.Auditd.UserspaceLost)

		// system
		if systemBeat {
			obs.ObserveFloat64(sysCPUCores, float64(s.System.CPU.Cores))
			obs.ObserveFloat64(sysLoad1, s.System.Load.M1)
			obs.ObserveFloat64(sysLoad5, s.System.Load.M5)
			obs.ObserveFloat64(sysLoad15, s.System.Load.M15)
			obs.ObserveFloat64(sysLoadNorm1, s.System.Load.Norm.M1)
			obs.ObserveFloat64(sysLoadNorm5, s.System.Load.Norm.M5)
			obs.ObserveFloat64(sysLoadNorm15, s.System.Load.Norm.M15)
		}

		// filebeat-specific
		if beat == "filebeat" {
			obs.ObserveFloat64(fbEventsActive, s.Filebeat.Events.Active)
			obs.ObserveFloat64(fbEventsAdded, s.Filebeat.Events.Added)
			obs.ObserveFloat64(fbEventsDone, s.Filebeat.Events.Done)
			obs.ObserveFloat64(fbHarvClosed, s.Filebeat.Harvester.Closed)
			obs.ObserveFloat64(fbHarvOpenFiles, s.Filebeat.Harvester.OpenFiles)
			obs.ObserveFloat64(fbHarvRunning, s.Filebeat.Harvester.Running)
			obs.ObserveFloat64(fbHarvSkipped, s.Filebeat.Harvester.Skipped)
			obs.ObserveFloat64(fbHarvStarted, s.Filebeat.Harvester.Started)
			obs.ObserveFloat64(fbInputLogRenamed, s.Filebeat.Input.Log.Files.Renamed)
			obs.ObserveFloat64(fbInputLogTruncated, s.Filebeat.Input.Log.Files.Truncated)
			obs.ObserveFloat64(regWritesFail, s.Registrar.Writes.Fail)
			obs.ObserveFloat64(regWritesSuccess, s.Registrar.Writes.Success)
			obs.ObserveFloat64(regWritesTotal, s.Registrar.Writes.Total)
			obs.ObserveFloat64(regStatesCleanup, s.Registrar.States.Cleanup)
			obs.ObserveFloat64(regStatesCurrent, s.Registrar.States.Current)
			obs.ObserveFloat64(regStatesUpdate, s.Registrar.States.Update)
		}

		// metricbeat-specific
		if beat == "metricbeat" {
			obs.ObserveFloat64(mbCPU, s.Metricbeat.System.CPU.Success, attrSuccess)
			obs.ObserveFloat64(mbCPU, s.Metricbeat.System.CPU.Failures, attrFailures)
			obs.ObserveFloat64(mbFilesystem, s.Metricbeat.System.Filesystem.Success, attrSuccess)
			obs.ObserveFloat64(mbFilesystem, s.Metricbeat.System.Filesystem.Failures, attrFailures)
			obs.ObserveFloat64(mbFsstat, s.Metricbeat.System.Fsstat.Success, attrSuccess)
			obs.ObserveFloat64(mbFsstat, s.Metricbeat.System.Fsstat.Failures, attrFailures)
			obs.ObserveFloat64(mbLoad, s.Metricbeat.System.Load.Success, attrSuccess)
			obs.ObserveFloat64(mbLoad, s.Metricbeat.System.Load.Failures, attrFailures)
			obs.ObserveFloat64(mbMemory, s.Metricbeat.System.Memory.Success, attrSuccess)
			obs.ObserveFloat64(mbMemory, s.Metricbeat.System.Memory.Failures, attrFailures)
			obs.ObserveFloat64(mbNetwork, s.Metricbeat.System.Network.Success, attrSuccess)
			obs.ObserveFloat64(mbNetwork, s.Metricbeat.System.Network.Failures, attrFailures)
			obs.ObserveFloat64(mbProcess, s.Metricbeat.System.Process.Success, attrSuccess)
			obs.ObserveFloat64(mbProcess, s.Metricbeat.System.Process.Failures, attrFailures)
			obs.ObserveFloat64(mbProcessSummary, s.Metricbeat.System.ProcessSummary.Success, attrSuccess)
			obs.ObserveFloat64(mbProcessSummary, s.Metricbeat.System.ProcessSummary.Failures, attrFailures)
			obs.ObserveFloat64(mbUptime, s.Metricbeat.System.Uptime.Success, attrSuccess)
			obs.ObserveFloat64(mbUptime, s.Metricbeat.System.Uptime.Failures, attrFailures)
		}

		return nil
	}, instruments...)
	if err != nil {
		return nil, fmt.Errorf("registering callback: %w", err)
	}

	return bc, nil
}

// LoadBeatInfo fetches and decodes the beat root endpoint to discover the beat type.
func LoadBeatInfo(client *http.Client, beatURL url.URL) (*BeatInfo, error) {
	resp, err := client.Get(beatURL.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("beat returned HTTP %d from %s", resp.StatusCode, beatURL.String())
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading beat response: %w", err)
	}

	var info BeatInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, fmt.Errorf("parsing beat JSON: %w", err)
	}

	slog.Info("Discovered beat",
		"beat", info.Beat,
		"version", info.Version,
		"name", info.Name,
		"hostname", info.Hostname,
		"uuid", info.UUID,
	)
	return &info, nil
}

// fetchStats retrieves and parses the /stats endpoint, storing the result in bc.stats.
func (bc *BeatCollector) fetchStats() error {
	resp, err := bc.client.Get(bc.beatURL.String() + "/stats")
	if err != nil {
		return fmt.Errorf("fetching /stats: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading /stats body: %w", err)
	}

	// Normalise filebeat's non-standard `"time": 123` → `"time": {"ms": 123}`.
	body = hackfixRegex.ReplaceAll(body, []byte(`"time":{"ms":$1}`))

	var stats Stats
	if err := json.Unmarshal(body, &stats); err != nil {
		return fmt.Errorf("parsing /stats JSON: %w", err)
	}

	bc.mu.Lock()
	bc.stats = stats
	bc.mu.Unlock()
	return nil
}

// NewHTTPClientWithUnixSocket returns an *http.Client pre-configured for a
// Unix-domain socket and rewrites beatURL so it points at http://localhost.
func NewHTTPClientWithUnixSocket(beatURL *url.URL, timeout time.Duration) (*http.Client, *url.URL) {
	unixPath := beatURL.Path
	rewritten := *beatURL
	rewritten.Scheme = "http"
	rewritten.Host = "localhost"
	rewritten.Path = ""

	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", unixPath)
			},
		},
	}
	return client, &rewritten
}

func msToSeconds(ms float64) float64 {
	return (time.Duration(ms) * time.Millisecond).Seconds()
}
