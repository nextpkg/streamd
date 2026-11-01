package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type querySample struct {
	Labels map[string]string `json:"labels"`
	Value  float64           `json:"value"`
}

type queryResult struct {
	Name    string        `json:"name"`
	Query   string        `json:"query"`
	Samples []querySample `json:"samples"`
}

func queryMetrics(ctx context.Context, settings options) ([]queryResult, error) {
	queries := []queryResult{{Name: "custom", Query: settings.query}}
	if settings.query == "" {
		if !regexp.MustCompile(`^[A-Za-z0-9_.-]+$`).MatchString(settings.job) {
			return nil, errors.New("job must contain only letters, digits, dots, underscores or hyphens")
		}
		label := "{job=" + strconv.Quote(settings.job) + "}"
		queries = []queryResult{
			{Name: "cpu_percent", Query: "max_over_time(rate(process_cpu_seconds_total" + label + "[1m])[10m:2s])*100"},
			{Name: "resident_memory_mib", Query: "max_over_time(process_resident_memory_bytes" + label + "[10m:2s])/(1024*1024)"},
		}
	}
	client := &http.Client{Timeout: 10 * time.Second, Transport: http.DefaultTransport.(*http.Transport).Clone()}
	defer client.CloseIdleConnections()
	for index := range queries {
		values, err := queryInstant(ctx, client, settings.address, queries[index].Query)
		if err != nil {
			return queries, err
		}
		queries[index].Samples = values
	}
	return queries, nil
}

func queryInstant(ctx context.Context, client *http.Client, address, expression string) ([]querySample, error) {
	endpoint, err := url.Parse(address)
	if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Hostname() == "" || endpoint.Fragment != "" || endpoint.RawQuery != "" {
		return nil, errors.New("expected an HTTP(S) Prometheus base URL without query or fragment")
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/api/v1/query"
	endpoint.RawPath = ""
	endpoint.RawQuery = url.Values{"query": {expression}}.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("prometheus returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024+1))
	if err != nil {
		return nil, err
	}
	if len(body) > 1024*1024 {
		return nil, errors.New("prometheus response exceeds 1 MiB")
	}
	var result struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			Type   string `json:"resultType"`
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	if result.Status != "success" || result.Data.Type != "vector" || len(result.Data.Result) == 0 {
		return nil, errors.New("prometheus query must return a nonempty successful instant vector")
	}
	var samples []querySample
	for _, sample := range result.Data.Result {
		if len(sample.Value) != 2 {
			return nil, errors.New("invalid Prometheus sample")
		}
		var number string
		if err := json.Unmarshal(sample.Value[1], &number); err != nil {
			return nil, err
		}
		parsed, err := strconv.ParseFloat(number, 64)
		if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return nil, errors.New("prometheus sample must be finite and numeric")
		}
		samples = append(samples, querySample{Labels: sample.Metric, Value: parsed})
	}
	return samples, nil
}
