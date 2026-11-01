package record

import (
	"path/filepath"

	"github.com/nextpkg/streamd/config"
	"github.com/nextpkg/streamd/internal/appstate"
	"github.com/nextpkg/streamd/internal/logging"
)

const maxFinalizingRecordings = 3

type finalizingRecording struct {
	done     chan struct{}
	settings *config.AppRecorder
}

func (writer *switchingWriter) reapFinalizations() {
	pending := writer.finalizing[:0]
	for _, recording := range writer.finalizing {
		select {
		case <-recording.done:
		default:
			pending = append(pending, recording)
		}
	}
	clear(writer.finalizing[len(pending):])
	writer.finalizing = pending
}

func (writer *switchingWriter) retire() bool {
	writer.reapFinalizations()
	if len(writer.finalizing) == maxFinalizingRecordings {
		return false
	}
	terminal, failed := writer.terminal, writer.terminalFailed
	job := finalizingRecording{done: make(chan struct{}), settings: writer.settings}
	writer.finalizing = append(writer.finalizing, job)
	writer.terminal, writer.store, writer.terminalFailed = nil, nil, false
	info := writer.Info()
	go func() {
		defer close(job.done)
		if failed {
			terminal.After()
		} else if err := terminal.Finish(); err != nil {
			logging.Errorf("[%s]recording configuration finalization: %s", info.Key, err)
			appstate.ReportBackgroundFailure("recording", info.Key, info.UID)
		}
	}()
	return true
}

func (writer *switchingWriter) finalizingTarget(settings *config.AppRecorder) bool {
	for _, recording := range writer.finalizing {
		previous := recording.settings
		if previous == nil || previous.SaveAs != settings.SaveAs {
			continue
		}
		previousFile := previous.Driver == "" || previous.Driver == config.DriverFs
		nextFile := settings.Driver == "" || settings.Driver == config.DriverFs
		if !previousFile && !nextFile {
			return true
		}
		if previousFile && nextFile && recordingDirectory(previous.SaveTo) == recordingDirectory(settings.SaveTo) {
			return true
		}
	}
	return false
}

func recordingDirectory(directory string) string {
	if resolved, err := filepath.EvalSymlinks(directory); err == nil {
		return resolved
	}
	return filepath.Clean(directory)
}

func (writer *switchingWriter) fail(revision uint64) {
	appstate.ReportFailure("recording", writer.Info().Key, writer.Info().UID, revision)
	writer.terminalFailed, writer.paused = true, true
	writer.retire()
}
