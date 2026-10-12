package main

import "testing"

func validLoadClient(identity int) loadClient {
	client := loadClient{PID: identity, Passed: true, Joined: true}
	client.Progress.Frames, client.Progress.Microseconds, client.Progress.End = 125, 5000000, true
	return client
}

func TestLoadToolValidatorRequiresIndependentCompletedClients(test *testing.T) {
	result := loadResult{Complete: true, Clients: []loadClient{validLoadClient(1), validLoadClient(2)}}
	if err := validateLoadClients(result, 2, false); err != nil {
		test.Fatal(err)
	}
	for _, mutate := range []func(*loadResult){
		func(result *loadResult) { result.Complete = false },
		func(result *loadResult) { result.Cancelled = true },
		func(result *loadResult) { result.Clients = result.Clients[:1] },
		func(result *loadResult) { result.Clients[1].PID = 1 },
		func(result *loadResult) { result.Clients[0].PID = 0 },
		func(result *loadResult) { result.Clients[0].Joined = false },
		func(result *loadResult) { result.Clients[0].Progress.Frames = 0 },
		func(result *loadResult) { result.Clients[0].Progress.End = false },
		func(result *loadResult) { result.Clients[0].Error = "unexpected failure" },
	} {
		candidate := result
		candidate.Clients = append([]loadClient(nil), result.Clients...)
		mutate(&candidate)
		if err := validateLoadClients(candidate, 2, false); err == nil {
			test.Fatalf("accepted invalid load evidence: %+v", candidate)
		}
	}
	result.Complete, result.Cancelled = false, true
	for index := range result.Clients {
		result.Clients[index].Passed = false
		result.Clients[index].Error = "context canceled"
	}
	if err := validateLoadClients(result, 2, true); err != nil {
		test.Fatal(err)
	}
	if err := validateLoadClients(result, 2, false); err == nil {
		test.Fatal("intentional cancellation counted as normal completion")
	}
}

func TestLoadSlabValidatorRequiresEveryThresholdAndJoinedService(test *testing.T) {
	var result loadResult
	result.Complete = true
	for index := 0; index < 5; index++ {
		result.Slabs = append(result.Slabs, loadSlab{Threshold: 1 + index*8, Block: 33, Min: 2, PID: index + 10, Joined: true, Publisher: validLoadClient(index + 20)})
	}
	if err := validateLoadSlabs(result); err != nil {
		test.Fatal(err)
	}
	result.Slabs[2].Threshold++
	if err := validateLoadSlabs(result); err == nil {
		test.Fatal("wrong threshold accepted")
	}
	result.Slabs[2].Threshold--
	result.Slabs[2].Joined = false
	if err := validateLoadSlabs(result); err == nil {
		test.Fatal("unjoined service accepted")
	}
	result.Slabs[2].Joined = true
	result.Slabs[2].Min = 0
	if err := validateLoadSlabs(result); err == nil {
		test.Fatal("empty measurement accepted")
	}
}
