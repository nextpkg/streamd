package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

type loadClient struct {
	PID      int    `json:"pid"`
	Passed   bool   `json:"passed"`
	Joined   bool   `json:"joined"`
	Error    string `json:"error"`
	Progress struct {
		Frames       int64 `json:"frames"`
		Microseconds int64 `json:"media_microseconds"`
		End          bool  `json:"end"`
	} `json:"progress"`
}

type loadResult struct {
	Complete  bool         `json:"complete"`
	Cancelled bool         `json:"cancelled"`
	Clients   []loadClient `json:"clients"`
	Slabs     []loadSlab   `json:"slabs"`
}

type loadSlab struct {
	Threshold int        `json:"minimum_kib"`
	Block     int        `json:"maximum_kib"`
	Max       int64      `json:"oversized_allocations"`
	Medium    int64      `json:"standalone_allocations"`
	Min       int64      `json:"slab_blocks"`
	PID       int        `json:"server_pid"`
	Joined    bool       `json:"server_joined"`
	Publisher loadClient `json:"publisher"`
}

func validateLoadClients(result loadResult, count int, interrupted bool) error {
	if result.Complete == interrupted || result.Cancelled != interrupted || len(result.Clients) != count {
		return errors.New("load test completion or client count mismatch")
	}
	identities := map[int]bool{}
	for _, client := range result.Clients {
		if client.PID <= 0 || identities[client.PID] || !client.Joined || client.Passed == interrupted || client.Progress.Frames <= 0 || client.Progress.Microseconds <= 0 {
			return errors.New("load client did not run, transfer media and join independently")
		}
		identities[client.PID] = true
		if !interrupted && (!client.Progress.End || client.Error != "") {
			return errors.New("normal load client did not finish cleanly")
		}
	}
	return nil
}

func validateLoadSlabs(result loadResult) error {
	if !result.Complete || result.Cancelled || len(result.Slabs) != 5 {
		return errors.New("slab sweep incomplete")
	}
	for index, row := range result.Slabs {
		if row.Threshold != 1+index*8 || row.Block != 33 || row.PID <= 0 || !row.Joined || row.Max < 0 || row.Medium < 0 || row.Min < 0 || row.Max+row.Medium+row.Min <= 0 {
			return errors.New("slab threshold, counts or child ownership invalid")
		}
		if err := validateLoadClients(loadResult{Complete: true, Clients: []loadClient{row.Publisher}}, 1, false); err != nil {
			return err
		}
	}
	return nil
}

func loadProcessesGone(result loadResult) error {
	var identities []int
	for _, client := range result.Clients {
		identities = append(identities, client.PID)
	}
	for _, row := range result.Slabs {
		identities = append(identities, row.PID, row.Publisher.PID)
	}
	for _, identity := range identities {
		process, err := os.FindProcess(identity)
		if err != nil {
			return err
		}
		err = process.Signal(syscall.Signal(0))
		_ = process.Release()
		if !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
			return fmt.Errorf("load test child %d still present or unverified: %v", identity, err)
		}
	}
	return nil
}

func (harness *harness) loadResult(name string) (loadResult, error) {
	var result loadResult
	body, err := os.ReadFile(filepath.Join(harness.evidence, name+".log"))
	if err != nil {
		return result, err
	}
	if strings.Contains(string(body), "WARNING: DATA RACE") {
		return result, errors.New("load tool data race")
	}
	directory := ""
	for _, line := range strings.Split(string(body), "\n") {
		if filepath.IsAbs(line) && filepath.Dir(line) == filepath.Join(harness.evidence, "tools") {
			directory = line
		}
	}
	if directory == "" {
		return result, errors.New("load tool evidence directory missing")
	}
	if err := filepath.WalkDir(directory, func(filename string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(filename) != ".log" {
			return nil
		}
		content, err := os.ReadFile(filename)
		if err != nil {
			return err
		}
		if strings.Contains(string(content), "WARNING: DATA RACE") || strings.Contains(string(content), "panic:") {
			return fmt.Errorf("nested tool process panic or race: %s", filename)
		}
		return nil
	}); err != nil {
		return result, err
	}
	body, err = os.ReadFile(filepath.Join(directory, "results.json"))
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return result, err
	}
	var complete map[string]any
	if err := json.Unmarshal(body, &complete); err != nil {
		return result, err
	}
	harness.results[name] = complete
	return result, nil
}

func (harness *harness) waitLoadPublications(prefix string, count int) error {
	return harness.until("all load publishers sending video", 10*time.Second, func() (bool, error) {
		for index := 1; index <= count; index++ {
			status, body, err := harness.fetch(fmt.Sprintf("/admin/stat/pub/info?key=flv/%s_%d", prefix, index))
			if err != nil || status != http.StatusOK {
				return false, err
			}
			var envelope struct{ Data subscriberReport }
			if err := json.Unmarshal(body, &envelope); err != nil {
				return false, err
			}
			if envelope.Data.Count != 1 || len(envelope.Data.List) != 1 || envelope.Data.List[0].VideoReceived <= 0 {
				return false, nil
			}
		}
		return true, nil
	})
}

func (harness *harness) loadTools() (failure error) {
	tool := filepath.Join(harness.evidence, "loadtest")
	arguments := []string{"build", "-o", tool}
	cgo := "0"
	if harness.results["race_build"] == true {
		arguments = append(arguments, "-race")
		cgo = "1"
	}
	arguments = append(arguments, "./scripts/loadtest")
	ctx, cancel := context.WithTimeout(harness.ctx, time.Minute)
	command := exec.CommandContext(ctx, "go", arguments...)
	command.Dir, command.Env = harness.root, append(os.Environ(), "CGO_ENABLED="+cgo)
	body, err := command.CombinedOutput()
	cancel()
	if err != nil {
		return fmt.Errorf("build load tool: %w: %s", err, body)
	}
	harness.recordingApplications("immediate", 4, 6)
	if err := harness.start(); err != nil {
		return err
	}
	publishers, err := harness.publishApps("publish-", "bframes", 60)
	if err != nil {
		return err
	}
	players, err := harness.playersForApps(45)
	if err != nil {
		return err
	}
	watch := harness.watchPlayback(players)
	defer func() { failure = errors.Join(failure, watch.finish(harness)) }()
	select {
	case <-watch.ready:
	case <-watch.done:
		return fmt.Errorf("baseline playback warmup: %w", watch.err)
	case <-harness.ctx.Done():
		return harness.ctx.Err()
	}
	baseline, err := harness.subscribers()
	if err != nil {
		return err
	}
	output := filepath.Join(harness.evidence, "tools")
	input := filepath.Join(harness.root, "testdata", "generated", "bframes.flv")
	base := fmt.Sprintf("rtmp://127.0.0.1:%d/flv/", harness.rtmpPort)
	loadPublisher, err := harness.launch("tool-publish", tool, "publish", "-i", input, "-r", base+"load", "-c", "4", "-duration", "18s", "-stagger", "300ms", "-out", output)
	if err != nil {
		return err
	}
	loadPlayer, err := harness.launch("tool-play", tool, "play", "-r", base+"sample", "-c", "6", "-duration", "18s", "-stagger", "300ms", "-out", output)
	if err != nil {
		return err
	}
	if err := harness.waitLoadPublications("load", 4); err != nil {
		return err
	}
	if err := harness.until("six additional copy-load players", 10*time.Second, func() (bool, error) {
		current, err := harness.subscribers()
		return current["flv"].Count == baseline["flv"].Count+6, err
	}); err != nil {
		return err
	}
	harness.results["load_concurrency_observed"] = map[string]int{"publishers": 4, "additional_players": 6}
	slab, err := harness.launch("tool-slab", tool, "slab", "-server", filepath.Join(harness.evidence, "streamd"), "-i", input, "-min", "1", "-max", "33", "-duration", "5s", "-out", output)
	if err != nil {
		return err
	}
	if err := harness.finish("tool-slab-exit", slab, 30*time.Second); err != nil {
		return err
	}
	sweep, err := harness.loadResult("tool-slab")
	if err != nil {
		return err
	}
	if err := errors.Join(validateLoadSlabs(sweep), loadProcessesGone(sweep)); err != nil {
		return err
	}
	for _, sample := range []struct {
		name  string
		child *process
		count int
	}{{"tool-publish", loadPublisher, 4}, {"tool-play", loadPlayer, 6}} {
		if err := harness.finish(sample.name+"-exit", sample.child, 30*time.Second); err != nil {
			return err
		}
		result, err := harness.loadResult(sample.name)
		if err != nil {
			return err
		}
		if err := errors.Join(validateLoadClients(result, sample.count, false), loadProcessesGone(result)); err != nil {
			return err
		}
	}
	interrupted, err := harness.launch("tool-cancel", tool, "publish", "-i", input, "-r", base+"cancel", "-c", "2", "-stagger", "0", "-out", output)
	if err != nil {
		return err
	}
	if err := harness.waitLoadPublications("cancel", 2); err != nil {
		return err
	}
	if err := harness.sleep(2 * time.Second); err != nil {
		return err
	}
	if err := interrupted.command.Process.Signal(syscall.SIGTERM); err != nil {
		return err
	}
	code, err := interrupted.wait(harness.ctx, 10*time.Second)
	if err != nil || code != 1 {
		return fmt.Errorf("cancelled tool exit=%d: %w", code, err)
	}
	interruption, err := harness.loadResult("tool-cancel")
	if err != nil {
		return err
	}
	if err := errors.Join(validateLoadClients(interruption, 2, true), loadProcessesGone(interruption)); err != nil {
		return err
	}
	if !harness.server.alive() || !publishers["flv"].alive() || !publishers["ts"].alive() {
		return errors.New("tool cleanup stopped an unrelated service or publisher")
	}
	harness.results["cancelled_tool_exit"] = code
	harness.results["owned_process_cleanup_verified"] = true
	if err := harness.cancelSlabTool(tool, input, output); err != nil {
		return err
	}
	for _, preset := range []string{"128k", "256k", "512k"} {
		name := "tool-preset-" + preset
		process, err := harness.launch(name, tool, "publish", "-"+preset, "-r", base+preset, "-c", "2", "-duration", "6s", "-stagger", "0", "-out", output)
		if err != nil {
			return err
		}
		if err := harness.finish(name+"-exit", process, 15*time.Second); err != nil {
			return err
		}
		result, err := harness.loadResult(name)
		if err != nil {
			return err
		}
		if err := errors.Join(validateLoadClients(result, 2, false), loadProcessesGone(result)); err != nil {
			return err
		}
	}
	var queryCount atomic.Int32
	queryServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/query" || !strings.Contains(request.URL.Query().Get("query"), `job="streamd"`) {
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		queryCount.Add(1)
		fmt.Fprint(response, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"instance":"contract"},"value":[1,"12.5"]}]}}`)
	}))
	defer queryServer.Close()
	query, err := harness.launch("tool-query", tool, "query", "-address", queryServer.URL, "-out", output)
	if err != nil {
		return err
	}
	if err := harness.finish("tool-query-exit", query, 10*time.Second); err != nil {
		return err
	}
	queryResult, err := harness.loadResult("tool-query")
	if err != nil || !queryResult.Complete || queryCount.Load() != 2 {
		return fmt.Errorf("query CLI contract incomplete: %w", err)
	}
	harness.results["query_scope"] = "two real HTTP requests through the CLI to a local Prometheus API contract; not a Prometheus engine or resource-performance baseline"
	if err := harness.finishPlayers(players, 1000); err != nil {
		return err
	}
	for app, publisher := range publishers {
		if err := harness.finish("publish-"+app, publisher, 65*time.Second); err != nil {
			return err
		}
	}
	if err := harness.stopServer(); err != nil {
		return err
	}
	for _, app := range []string{"flv", "ts"} {
		if err := harness.recordings(filepath.Join("recordings", app), 1, true); err != nil {
			return err
		}
	}
	harness.results["load_tools_verified"] = true
	return harness.checkLogs("play-*.log", "publish-*.log")
}

func (harness *harness) cancelSlabTool(tool, input, output string) error {
	pattern := filepath.Join(output, "loadtest-slab-*", "min-1", "server.log")
	previous, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	known := map[string]bool{}
	for _, filename := range previous {
		known[filename] = true
	}
	process, err := harness.launch("tool-slab-cancel", tool, "slab", "-server", filepath.Join(harness.evidence, "streamd"), "-i", input, "-min", "1", "-max", "33", "-duration", "1h", "-out", output)
	if err != nil {
		return err
	}
	if err := harness.until("cancellable analyzer owns a ready service", 10*time.Second, func() (bool, error) {
		files, err := filepath.Glob(pattern)
		if err != nil {
			return false, err
		}
		for _, filename := range files {
			if !known[filename] {
				body, err := os.ReadFile(filename)
				return strings.Contains(string(body), "HTTP listening"), err
			}
		}
		return false, nil
	}); err != nil {
		return err
	}
	if err := harness.sleep(500 * time.Millisecond); err != nil {
		return err
	}
	if !process.alive() {
		return errors.New("analyzer finished before interruption could be exercised")
	}
	if err := process.command.Process.Signal(syscall.SIGTERM); err != nil {
		return err
	}
	code, err := process.wait(harness.ctx, 20*time.Second)
	if err != nil || code != 1 {
		return fmt.Errorf("interrupted analyzer exit=%d: %v", code, err)
	}
	result, err := harness.loadResult("tool-slab-cancel")
	if err != nil {
		return err
	}
	if result.Complete || !result.Cancelled || len(result.Slabs) == 0 {
		return errors.New("analyzer interruption did not preserve incomplete evidence")
	}
	for _, row := range result.Slabs {
		if row.PID <= 0 || !row.Joined || row.Publisher.PID <= 0 || !row.Publisher.Joined {
			return errors.New("interrupted analyzer did not join its server and publisher")
		}
	}
	if err := loadProcessesGone(result); err != nil {
		return err
	}
	if !harness.server.alive() {
		return errors.New("analyzer cleanup stopped an unrelated service")
	}
	harness.results["cancelled_analyzer_exit"] = code
	harness.results["analyzer_cancellation_scope"] = "one-hour media target interrupted shortly after its private service became ready; not a one-hour stability run"
	return nil
}
