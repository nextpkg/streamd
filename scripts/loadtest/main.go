package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type options struct {
	mode        string
	input       string
	target      string
	concurrency int
	duration    time.Duration
	stagger     time.Duration
	output      string
	ffmpeg      string
	server      string
	minimum     int
	maximum     int
	step        int
	bitrate     string
	address     string
	query       string
	job         string
}

type report struct {
	Mode       string         `json:"mode"`
	Complete   bool           `json:"complete"`
	Cancelled  bool           `json:"cancelled"`
	Error      string         `json:"error,omitempty"`
	Elapsed    float64        `json:"elapsed_seconds"`
	Parameters map[string]any `json:"parameters"`
	Clients    []clientResult `json:"clients,omitempty"`
	Slabs      []slabResult   `json:"slabs,omitempty"`
	Metrics    []queryResult  `json:"metrics,omitempty"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := execute(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseOptions(arguments []string, output io.Writer) (options, error) {
	var settings options
	if len(arguments) == 0 || arguments[0] == "-h" || arguments[0] == "--help" {
		if _, err := fmt.Fprintln(output, "Usage: loadtest publish|play|slab|query [flags]; each command supports -h"); err != nil {
			return settings, err
		}
		return settings, flag.ErrHelp
	}
	settings.mode = arguments[0]
	flags := flag.NewFlagSet(settings.mode, flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&settings.output, "out", "artifacts", "parent for a new, private evidence directory")
	if settings.mode == "query" {
		flags.StringVar(&settings.address, "address", "http://127.0.0.1:9090", "Prometheus base URL")
		flags.StringVar(&settings.query, "query", "", "custom instant query; empty queries CPU and resident memory")
		flags.StringVar(&settings.job, "job", "streamd", "Prometheus job label for the default queries")
	} else {
		flags.StringVar(&settings.ffmpeg, "ffmpeg", "ffmpeg", "independent FFmpeg test executable")
		if settings.mode != "play" {
			flags.StringVar(&settings.input, "i", "", "input media file; publish defaults to a generated bitrate preset")
		}
		defaultDuration := time.Duration(0)
		if settings.mode == "slab" {
			defaultDuration = 100 * time.Second
		}
		flags.DurationVar(&settings.duration, "duration", defaultDuration, "media duration; zero runs until interrupted")
	}
	var preset128, preset256, preset512 bool
	switch settings.mode {
	case "publish", "play":
		flags.StringVar(&settings.target, "r", "rtmp://127.0.0.1:1935/live/sample", "RTMP URL; publishers append _1, _2, ... to its stream path")
		flags.IntVar(&settings.concurrency, "c", 1, "number of independent FFmpeg clients")
		flags.DurationVar(&settings.stagger, "stagger", 6553*time.Millisecond, "maximum random startup delay per client")
		if settings.mode == "publish" {
			flags.BoolVar(&preset128, "128k", false, "generate a nominal 128 kbit/s video preset")
			flags.BoolVar(&preset256, "256k", false, "generate a nominal 256 kbit/s video preset")
			flags.BoolVar(&preset512, "512k", false, "generate a nominal 512 kbit/s video preset")
		}
	case "slab":
		flags.StringVar(&settings.server, "server", "streamd", "service binary; only the started child is stopped")
		flags.IntVar(&settings.minimum, "min", 32, "first slab minimum in KiB; zero uses the service default")
		flags.IntVar(&settings.maximum, "max", 256, "fixed slab block size and final scan threshold in KiB")
		flags.IntVar(&settings.step, "step", 8, "minimum-threshold increment in KiB")
	case "query":
	default:
		return settings, fmt.Errorf("unknown loadtest command %q", settings.mode)
	}
	if err := flags.Parse(arguments[1:]); err != nil {
		return settings, err
	}
	if flags.NArg() != 0 {
		return settings, errors.New("unexpected positional arguments")
	}
	if settings.duration < 0 || settings.stagger < 0 {
		return settings, errors.New("duration and stagger must not be negative")
	}
	if settings.mode == "publish" || settings.mode == "play" {
		if settings.concurrency < 1 {
			return settings, errors.New("concurrency must be positive")
		}
		if _, err := streamURL(settings.target, 0); err != nil {
			return settings, err
		}
	}
	if settings.mode == "slab" && (settings.input == "" || settings.duration <= 0 || settings.minimum < 0 || settings.maximum <= 0 || settings.minimum > settings.maximum || settings.step <= 0 || settings.maximum > int(^uint(0)>>1)/1024) {
		return settings, errors.New("slab requires input, positive duration/step/max, and 0 <= min <= max in KiB")
	}
	presets := 0
	settings.bitrate = "128k"
	for _, entry := range []struct {
		selected bool
		bitrate  string
	}{{preset128, "128k"}, {preset256, "256k"}, {preset512, "512k"}} {
		if entry.selected {
			presets++
			settings.bitrate = entry.bitrate
		}
	}
	if presets > 1 || (presets > 0 && settings.input != "") {
		return settings, errors.New("choose one bitrate preset or an input file")
	}
	if settings.input != "" {
		info, err := os.Stat(settings.input)
		if err != nil {
			return settings, err
		}
		if !info.Mode().IsRegular() {
			return settings, errors.New("input must be a regular media file")
		}
	}
	return settings, nil
}

func execute(ctx context.Context, arguments []string, output, diagnostics io.Writer) (result error) {
	settings, err := parseOptions(arguments, diagnostics)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := os.MkdirAll(settings.output, 0700); err != nil {
		return err
	}
	directory, err := os.MkdirTemp(settings.output, "loadtest-"+settings.mode+"-")
	if err != nil {
		return err
	}
	directory, err = filepath.Abs(directory)
	if err != nil {
		return err
	}
	started := time.Now()
	evidence := report{Mode: settings.mode, Parameters: map[string]any{"concurrency": settings.concurrency, "duration_seconds": settings.duration.Seconds(), "maximum_stagger_seconds": settings.stagger.Seconds()}}
	if settings.mode == "publish" && settings.input == "" {
		evidence.Parameters["generated_video_bitrate"] = settings.bitrate
	}
	if settings.mode == "publish" || settings.mode == "play" {
		target, _ := url.Parse(settings.target)
		target.User = nil
		if target.RawQuery != "" {
			target.RawQuery = "redacted"
		}
		evidence.Parameters["target"] = target.String()
	}
	if settings.mode == "slab" {
		evidence.Parameters["minimum_kib"], evidence.Parameters["maximum_kib"], evidence.Parameters["step_kib"] = settings.minimum, settings.maximum, settings.step
	}
	defer func() {
		evidence.Elapsed = time.Since(started).Seconds()
		evidence.Complete, evidence.Cancelled = result == nil, ctx.Err() != nil
		if result != nil {
			evidence.Error = result.Error()
		}
		result = errors.Join(result, writeJSON(filepath.Join(directory, "results.json"), evidence))
		if _, err := fmt.Fprintln(output, directory); err != nil {
			result = errors.Join(result, err)
			evidence.Complete, evidence.Error = false, result.Error()
			result = errors.Join(result, writeJSON(filepath.Join(directory, "results.json"), evidence))
		}
	}()
	switch settings.mode {
	case "publish", "play":
		evidence.Clients, result = runLoad(ctx, settings, directory)
	case "slab":
		evidence.Slabs, result = scanSlabs(ctx, settings, directory, output)
	case "query":
		evidence.Metrics, result = queryMetrics(ctx, settings)
		if result == nil {
			if _, err := fmt.Fprintln(output, "metric\tlabels\tvalue"); err != nil {
				return err
			}
			for _, metric := range evidence.Metrics {
				for _, sample := range metric.Samples {
					labels, _ := json.Marshal(sample.Labels)
					if _, err := fmt.Fprintf(output, "%s\t%s\t%g\n", metric.Name, labels, sample.Value); err != nil {
						return err
					}
				}
			}
		}
	}
	return result
}

func streamURL(raw string, index int) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "rtmp" || parsed.Hostname() == "" || parsed.Fragment != "" || len(strings.Split(strings.Trim(parsed.Path, "/"), "/")) < 2 {
		return "", errors.New("expected an RTMP URL with application and stream path")
	}
	if index > 0 {
		suffix := fmt.Sprintf("_%d", index)
		parsed.Path += suffix
		if parsed.RawPath != "" {
			parsed.RawPath += suffix
		}
	}
	return parsed.String(), nil
}

func writeJSON(filename string, value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filename, append(encoded, '\n'), 0600)
}
