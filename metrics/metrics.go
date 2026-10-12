package metrics

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	Playback      = "playback"
	PlaybackBW    = "playbackBandwidth"
	Channels      = "channels"
	ChannelsBW    = "channelsBandwidth"
	Recording     = "recording"
	RecordingTime = "recordingTime"
	RecordingSize = "recordingSize"
	WriteCost     = "writeCost"
	ReadCost      = "readCost"
)

type seriesKey struct {
	app     string
	channel string
	cluster string
}

type Registry struct {
	collector    *prometheus.Registry
	mutex        sync.Mutex
	cluster      string
	limit        int
	series       map[seriesKey]time.Time
	now          func() time.Time
	nextSweep    time.Time
	globalGauges map[string]float64
	gauges       map[string]*prometheus.GaugeVec
	counters     map[string]*prometheus.CounterVec
	summaries    map[string]*prometheus.SummaryVec
	rejected     prometheus.Counter
	proxyActive  prometheus.Gauge
	proxyAborted prometheus.Counter
}

func NewRegistry(cluster string, limit int) *Registry {
	if limit <= 0 {
		limit = 1024
	}
	registry := &Registry{
		collector:    prometheus.NewRegistry(),
		cluster:      cluster,
		limit:        limit,
		series:       make(map[seriesKey]time.Time),
		now:          time.Now,
		globalGauges: make(map[string]float64),
		gauges:       make(map[string]*prometheus.GaugeVec),
		counters:     make(map[string]*prometheus.CounterVec),
		summaries:    make(map[string]*prometheus.SummaryVec),
		rejected:     prometheus.NewCounter(prometheus.CounterOpts{Name: "streamd_metric_updates_rejected_total", Help: "Metric updates rejected by the label cardinality limit."}),
		proxyActive:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "streamd_proxy_requests_active", Help: "HTTP proxy requests whose response owner has not finished."}),
		proxyAborted: prometheus.NewCounter(prometheus.CounterOpts{Name: "streamd_proxy_responses_aborted_total", Help: "HTTP proxy responses finished after cancellation or a write failure."}),
	}
	labels := []string{"app", "channel", "cluster"}
	for name, exported := range map[string]string{Playback: "playback", Channels: "channels", Recording: "recording"} {
		metric := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "streamd_" + exported, Help: "Current number of " + exported + "."}, labels)
		registry.gauges[name] = metric
		registry.collector.MustRegister(metric)
	}
	for name, exported := range map[string]string{RecordingTime: "recording_seconds_total", RecordingSize: "recording_bytes_total"} {
		metric := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "streamd_" + exported, Help: "Cumulative recording activity."}, labels)
		registry.counters[name] = metric
		registry.collector.MustRegister(metric)
	}
	for name, exported := range map[string]string{PlaybackBW: "playback_bits_per_second", ChannelsBW: "channels_bits_per_second", ReadCost: "read_microseconds", WriteCost: "write_microseconds"} {
		metric := prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: "streamd_" + exported, Help: "Observations whose mean is sum divided by count."}, labels)
		registry.summaries[name] = metric
		registry.collector.MustRegister(metric)
	}
	registry.collector.MustRegister(registry.rejected, registry.proxyActive, registry.proxyAborted, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return registry
}

func (registry *Registry) labels(app, channel string) ([]string, bool) {
	if app == "" && channel == "" {
		return []string{app, channel, registry.cluster}, true
	}
	now := registry.now()
	key := seriesKey{app: app, channel: channel, cluster: registry.cluster}
	if _, exists := registry.series[key]; !exists {
		if len(registry.series) >= registry.limit && !now.Before(registry.nextSweep) {
			registry.expire(now)
		}
		if len(registry.series) >= registry.limit {
			registry.rejected.Inc()
			return nil, false
		}
	}
	registry.series[key] = now
	return []string{app, channel, registry.cluster}, true
}

func (registry *Registry) expire(now time.Time) {
	registry.nextSweep = now.Add(30 * time.Second)
	for key, updated := range registry.series {
		if now.Sub(updated) < 5*time.Minute {
			continue
		}
		labels := []string{key.app, key.channel, key.cluster}
		for _, metric := range registry.gauges {
			metric.DeleteLabelValues(labels...)
		}
		for _, metric := range registry.counters {
			metric.DeleteLabelValues(labels...)
		}
		for _, metric := range registry.summaries {
			metric.DeleteLabelValues(labels...)
		}
		delete(registry.series, key)
	}
}

func (registry *Registry) SetCluster(cluster string) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	if registry.cluster == cluster {
		return
	}
	registry.cluster = cluster
	registry.nextSweep = time.Time{}
	clear(registry.series)
	for _, metric := range registry.gauges {
		metric.Reset()
	}
	for _, metric := range registry.counters {
		metric.Reset()
	}
	for _, metric := range registry.summaries {
		metric.Reset()
	}
	for name, value := range registry.globalGauges {
		registry.gauges[name].WithLabelValues("", "", cluster).Set(value)
	}
}

func (registry *Registry) Gauge(app, channel, name string, value float64) {
	metric := registry.gauges[name]
	if metric == nil {
		return
	}
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	if labels, accepted := registry.labels(app, channel); accepted {
		metric.WithLabelValues(labels...).Set(value)
		if app == "" && channel == "" {
			registry.globalGauges[name] = value
		}
	}
}

func (registry *Registry) Counter(app, channel, name string, value float64) {
	metric := registry.counters[name]
	if metric == nil || value < 0 {
		return
	}
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	if labels, accepted := registry.labels(app, channel); accepted {
		metric.WithLabelValues(labels...).Add(value)
	}
}

func (registry *Registry) Average(app, channel, name string, value float64) {
	metric := registry.summaries[name]
	if metric == nil {
		return
	}
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	if labels, accepted := registry.labels(app, channel); accepted {
		metric.WithLabelValues(labels...).Observe(value)
	}
}

func (registry *Registry) Handler() http.Handler {
	handler := promhttp.HandlerFor(registry.collector, promhttp.HandlerOpts{})
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		registry.mutex.Lock()
		registry.expire(registry.now())
		registry.mutex.Unlock()
		handler.ServeHTTP(response, request)
	})
}

var defaultRegistry = NewRegistry("", 1024)

func (registry *Registry) ProxyRequestStarted() { registry.proxyActive.Inc() }
func (registry *Registry) ProxyRequestFinished(aborted bool) {
	registry.proxyActive.Dec()
	if aborted {
		registry.proxyAborted.Inc()
	}
}
func ProxyRequestStarted()              { defaultRegistry.ProxyRequestStarted() }
func ProxyRequestFinished(aborted bool) { defaultRegistry.ProxyRequestFinished(aborted) }

func SetCluster(cluster string)              { defaultRegistry.SetCluster(cluster) }
func Handler() http.Handler                  { return defaultRegistry.Handler() }
func GaugeGlobal(name string, value float64) { Gauge("", "", name, value) }
func Gauge(app, channel, name string, value float64) {
	defaultRegistry.Gauge(app, channel, name, value)
}
func Counter(app, channel, name string, value float64) {
	defaultRegistry.Counter(app, channel, name, value)
}
func Average(app, channel, name string, value float64) {
	defaultRegistry.Average(app, channel, name, value)
}

func ReportLoop(ctx context.Context, report func()) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			report()
		}
	}
}
