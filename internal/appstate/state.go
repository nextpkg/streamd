// Package appstate tracks component runtime status independently of configuration.
package appstate

import (
	"sort"
	"sync"
	"time"
)

type Status struct {
	Component     string `json:"component"`
	Stream        string `json:"stream"`
	Revision      uint64 `json:"revision"`
	State         string `json:"state"`
	Failures      uint64 `json:"failures"`
	LastFailureAt int64  `json:"last_failure_at,omitempty"`
	owner         string
}

var applications sync.Map

func Start(component, stream, owner string, revision uint64, state string) {
	applications.Store(component+":"+stream, Status{Component: component, Stream: stream, Revision: revision, State: state, owner: owner})
}

func Report(component, stream, owner string, revision uint64, state string) {
	updateApplication(component, stream, owner, revision, state, false)
}

func ReportFailure(component, stream, owner string, revision uint64) {
	updateApplication(component, stream, owner, revision, "failed", true)
}

func ReportBackgroundFailure(component, stream, owner string) {
	key := component + ":" + stream
	for {
		previous, exists := applications.Load(key)
		if !exists || previous.(Status).owner != owner {
			return
		}
		next := previous.(Status)
		next.Failures++
		next.LastFailureAt = time.Now().Unix()
		if applications.CompareAndSwap(key, previous, next) {
			return
		}
	}
}

func updateApplication(component, stream, owner string, revision uint64, state string, failure bool) {
	key := component + ":" + stream
	for {
		previous, exists := applications.Load(key)
		if !exists || previous.(Status).owner != owner {
			return
		}
		next := previous.(Status)
		next.Revision, next.State = revision, state
		if failure {
			next.Failures++
			next.LastFailureAt = time.Now().Unix()
		}
		if applications.CompareAndSwap(key, previous, next) {
			return
		}
	}
}

func Forget(component, stream, owner string) {
	key := component + ":" + stream
	if value, ok := applications.Load(key); ok && value.(Status).owner == owner {
		applications.CompareAndDelete(key, value)
	}
}

func Snapshot() []Status {
	result := []Status{}
	applications.Range(func(key, value any) bool { result = append(result, value.(Status)); return true })
	sort.Slice(result, func(left, right int) bool {
		if result[left].Component != result[right].Component {
			return result[left].Component < result[right].Component
		}
		return result[left].Stream < result[right].Stream
	})
	return result
}
