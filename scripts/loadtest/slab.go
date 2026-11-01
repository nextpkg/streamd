package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type slabResult struct {
	Threshold    int          `json:"minimum_kib"`
	Block        int          `json:"maximum_kib"`
	Max          int64        `json:"oversized_allocations"`
	Medium       int64        `json:"standalone_allocations"`
	Min          int64        `json:"slab_blocks"`
	ServerPID    int          `json:"server_pid"`
	ServerJoined bool         `json:"server_joined"`
	Publisher    clientResult `json:"publisher"`
}

var slabMessage = regexp.MustCompile(`^Connection from ([^ ]+) is closed,slab stat,max:([0-9]+),medium:([0-9]+),min:([0-9]+)$`)

func slabCounts(reader io.Reader) ([3]int64, error) {
	var total [3]int64
	connections := map[string][3]int64{}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		var record struct{ Level, Msg string }
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return total, fmt.Errorf("invalid service log: %w", err)
		}
		if record.Level == "ERROR" {
			return total, errors.New("service reported an error; inspect its log")
		}
		matched := slabMessage.FindStringSubmatch(record.Msg)
		if matched == nil {
			if strings.Contains(record.Msg, "slab stat") {
				return total, errors.New("malformed slab statistics")
			}
			continue
		}
		var counts [3]int64
		for index := range counts {
			value, err := strconv.ParseInt(matched[index+2], 10, 64)
			if err != nil {
				return total, err
			}
			counts[index] = value
		}
		if previous, exists := connections[matched[1]]; exists && previous != counts {
			return total, errors.New("conflicting slab statistics for one connection")
		}
		connections[matched[1]] = counts
	}
	if err := scanner.Err(); err != nil {
		return total, err
	}
	if len(connections) != 1 {
		return total, fmt.Errorf("expected one measured publication, found %d slab connections", len(connections))
	}
	for _, counts := range connections {
		total = counts
	}
	if total == [3]int64{} {
		return total, errors.New("empty slab measurement")
	}
	return total, nil
}

func localAddresses() (string, string, error) {
	first, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", "", err
	}
	defer func() { _ = first.Close() }()
	second, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", "", err
	}
	defer func() { _ = second.Close() }()
	return first.Addr().String(), second.Addr().String(), nil
}

func isolatedEnvironment() []string {
	var result []string
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		if key != "STREAMD_HTTP_ADDR" && key != "STREAMD_RTMP_ADDR" && key != "STREAMD_LOG_LEVEL" {
			result = append(result, value)
		}
	}
	return result
}

func waitReady(ctx context.Context, process *child, address string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{Proxy: nil}}
	defer client.CloseIdleConnections()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/readyz", nil)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-process.done:
			return errors.Join(errors.New("service exited before readiness"), process.err)
		case <-ticker.C:
		}
	}
}

func measureSlab(ctx context.Context, settings options, directory string, threshold int) (result slabResult, failure error) {
	result.Threshold, result.Block = threshold, settings.maximum
	if err := os.Mkdir(directory, 0700); err != nil {
		return result, err
	}
	httpAddress, rtmpAddress, err := localAddresses()
	if err != nil {
		return result, err
	}
	configuration := map[string]any{"log_level": "DEBUG-4", "http": map[string]string{"addr": httpAddress}, "rtmp": map[string]string{"addr": rtmpAddress}, "application": []map[string]any{{"enable": true, "app_name": "loadtest"}}}
	configPath := filepath.Join(directory, "config.json")
	if err := writeJSON(configPath, configuration); err != nil {
		return result, err
	}
	logPath := filepath.Join(directory, "server.log")
	server, err := startChild(settings.server, []string{"-config", configPath, "-slab-min", strconv.Itoa(threshold), "-slab-max", strconv.Itoa(settings.maximum)}, isolatedEnvironment(), io.Discard, logPath)
	if err != nil {
		return result, err
	}
	result.ServerPID = server.command.Process.Pid
	defer func() {
		failure = errors.Join(failure, server.stop(15*time.Second))
		result.ServerJoined = true
	}()
	if err := waitReady(ctx, server, httpAddress); err != nil {
		return result, err
	}
	result.Publisher = runMedia(ctx, settings, "rtmp://"+rtmpAddress+"/loadtest/sample", filepath.Join(directory, "publisher.log"), false)
	if !result.Publisher.Passed {
		return result, errors.New(result.Publisher.Error)
	}
	if err := waitSlabClose(ctx, server, logPath); err != nil {
		return result, err
	}
	if err := server.stop(15 * time.Second); err != nil {
		return result, err
	}
	logFile, err := os.Open(logPath)
	if err != nil {
		return result, err
	}
	defer func() { _ = logFile.Close() }()
	counts, err := slabCounts(logFile)
	result.Max, result.Medium, result.Min = counts[0], counts[1], counts[2]
	return result, err
}

func waitSlabClose(ctx context.Context, server *child, filename string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		logFile, err := os.Open(filename)
		if err != nil {
			return err
		}
		_, scanErr := slabCounts(logFile)
		closeErr := logFile.Close()
		if scanErr == nil {
			return closeErr
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), scanErr, closeErr)
		case <-server.done:
			return errors.Join(errors.New("service exited before publication closure was observed"), server.err, scanErr)
		case <-ticker.C:
		}
	}
}

func scanSlabs(ctx context.Context, settings options, directory string, output io.Writer) ([]slabResult, error) {
	var results []slabResult
	if _, err := fmt.Fprintln(output, "minimum_KiB\tmaximum_KiB\tMax\tMedium\tMin"); err != nil {
		return results, err
	}
	for threshold := settings.minimum; threshold <= settings.maximum; {
		if err := ctx.Err(); err != nil {
			return results, err
		}
		result, err := measureSlab(ctx, settings, filepath.Join(directory, "min-"+strconv.Itoa(threshold)), threshold)
		results = append(results, result)
		if err != nil {
			return results, err
		}
		if _, err := fmt.Fprintf(output, "%d\t%d\t%d\t%d\t%d\n", result.Threshold, result.Block, result.Max, result.Medium, result.Min); err != nil {
			return results, err
		}
		if settings.maximum-threshold < settings.step {
			break
		}
		threshold += settings.step
	}
	return results, nil
}
