package config

import (
	"sort"
	"sync"
	"time"
)

type ApplicationStatus struct {
	Component     string `json:"component"`
	Stream        string `json:"stream"`
	Revision      uint64 `json:"revision"`
	State         string `json:"state"`
	Failures      uint64 `json:"failures"`
	LastFailureAt int64  `json:"last_failure_at,omitempty"`
	owner         string
}

var applications sync.Map

func StartApplication(component, stream, owner string, revision uint64, state string) {
	applications.Store(component+":"+stream, ApplicationStatus{Component: component, Stream: stream, Revision: revision, State: state, owner: owner})
}

func ReportApplication(component, stream, owner string, revision uint64, state string) {
	updateApplication(component, stream, owner, revision, state, false)
}

func ReportApplicationFailure(component, stream, owner string, revision uint64) {
	updateApplication(component, stream, owner, revision, "failed", true)
}

func ReportApplicationBackgroundFailure(component, stream, owner string) {
	key := component + ":" + stream
	for {
		previous, exists := applications.Load(key)
		if !exists || previous.(ApplicationStatus).owner != owner {
			return
		}
		next := previous.(ApplicationStatus)
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
		if !exists || previous.(ApplicationStatus).owner != owner {
			return
		}
		next := previous.(ApplicationStatus)
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

func ForgetApplication(component, stream, owner string) {
	key := component + ":" + stream
	if value, ok := applications.Load(key); ok && value.(ApplicationStatus).owner == owner {
		applications.CompareAndDelete(key, value)
	}
}

func ApplicationStates() []ApplicationStatus {
	result := []ApplicationStatus{}
	applications.Range(func(key, value any) bool { result = append(result, value.(ApplicationStatus)); return true })
	sort.Slice(result, func(left, right int) bool {
		if result[left].Component != result[right].Component {
			return result[left].Component < result[right].Component
		}
		return result[left].Stream < result[right].Stream
	})
	return result
}
