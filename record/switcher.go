package record

import (
	"crypto/sha256"
	"encoding/json"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	log "github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/packet"
	"github.com/nextpkg/streamd/record/flv"
	"github.com/nextpkg/streamd/record/storage"
	"github.com/nextpkg/streamd/record/ts"
	"github.com/nextpkg/streamd/rtmp/core"
)

type segmentWriter interface {
	packet.Writer
	After()
	Finish() error
}

type switchingWriter struct {
	*core.Universal
	connect            *base.ConnectInfo
	headers            packet.SequenceHeaders
	settings           *config.AppRecorder
	fingerprint        [32]byte
	terminal           segmentWriter
	store              *storage.Storage
	start              uint32
	seenRevision       uint64
	finalizing         []finalizingRecording
	terminalFailed     bool
	paused             bool
	finalizationBudget *storage.FinalizationBudget
}

func newSwitchingWriter(info *base.Info, connect *base.ConnectInfo) *switchingWriter {
	writer := &switchingWriter{Universal: core.NewUniversal(info), connect: connect, finalizationBudget: storage.NewFinalizationBudget()}
	config.StartApplication("recording", info.Key, info.UID, config.Get().Revision, "waiting_keyframe")
	return writer
}

func (writer *switchingWriter) Name() string { return "recording" }

func (writer *switchingWriter) Write(frame *packet.Packet) error {
	writer.headers.Observe(frame)
	snapshot := config.Get()
	if writer.seenRevision != snapshot.Revision {
		writer.seenRevision = snapshot.Revision
		config.ReportApplication("recording", writer.Info().Key, writer.Info().UID, snapshot.Revision, "waiting_keyframe")
	}
	if packet.IsVideoBoundary(frame) {
		writer.apply(snapshot, frame.Baseline)
	}
	if writer.terminal == nil || writer.paused {
		return nil
	}
	normalized := *frame
	normalized.Baseline -= writer.start
	if int32(normalized.Baseline) < 0 {
		normalized.Baseline = 0
	}
	err := writer.terminal.Write(&normalized)
	if err == nil {
		err = writer.store.Error()
	}
	if err != nil {
		if writer.store.WaitingFinalization() {
			config.ReportApplication("recording", writer.Info().Key, writer.Info().UID, snapshot.Revision, "waiting_finalization")
			return nil
		}
		log.Errorf("[%s]recording failed: %s", writer.Info().Key, err)
		writer.fail(snapshot.Revision)
	}
	return nil
}

func (writer *switchingWriter) apply(snapshot *config.Config, timestamp uint32) {
	writer.reapFinalizations()
	settings := writer.settings
	state := "active"
	if app := snapshot.GetApp(writer.Info().App); app != nil {
		settings = &app.Recorder
	} else {
		state = "retained_for_existing_stream"
	}
	if settings == nil {
		return
	}
	fingerprint, err := recordingFingerprint(settings)
	if err != nil {
		log.Error("recording settings: ", err)
		return
	}
	if writer.terminal != nil && (fingerprint != writer.fingerprint || writer.terminalFailed) && !writer.retire() {
		writer.paused = true
		config.ReportApplication("recording", writer.Info().Key, writer.Info().UID, snapshot.Revision, "waiting_finalization")
		return
	}
	if settings.Enable && writer.finalizingTarget(settings) {
		writer.paused = true
		config.ReportApplication("recording", writer.Info().Key, writer.Info().UID, snapshot.Revision, "waiting_finalization")
		return
	}
	writer.paused = false
	if writer.store != nil && fingerprint == writer.fingerprint {
		writer.store.RefreshSettings(settings)
	}
	writer.settings, writer.fingerprint = settings, fingerprint
	if !settings.Enable {
		config.ReportApplication("recording", writer.Info().Key, writer.Info().UID, snapshot.Revision, "disabled")
		return
	}
	if writer.terminal == nil {
		events := &storage.Event{
			OnRecord:     storage.FireEvent("OnRecord", settings.Event.OnRecord, writer.Info().Key, writer.connect),
			OnRecordDone: storage.FireEvent("OnRecordDone", settings.Event.OnRecordDone, writer.Info().Key, writer.connect),
		}
		writer.store = storage.NewStorage(writer.Info(), settings, events)
		writer.store.SetFinalizationBudget(writer.finalizationBudget)
		switch settings.SaveAs {
		case config.AsFlv:
			writer.terminal = flv.NewTerminal(writer.Info(), writer.store)
		case config.AsTs:
			writer.terminal = ts.NewTerminal(writer.Info(), writer.store)
		default:
			config.ReportApplicationFailure("recording", writer.Info().Key, writer.Info().UID, snapshot.Revision)
			return
		}
		writer.start = timestamp
		if err := writer.headers.Replay(writer.terminal); err != nil {
			log.Error("recording sequence replay: ", err)
			writer.fail(snapshot.Revision)
			return
		}
	}
	config.ReportApplication("recording", writer.Info().Key, writer.Info().UID, snapshot.Revision, state)
}

func recordingFingerprint(settings *config.AppRecorder) ([32]byte, error) {
	identity := *settings
	if settings.AwsCfg != nil {
		credentials := *settings.AwsCfg
		credentials.SecretID, credentials.SecretKey, credentials.SessionToken = "", "", ""
		identity.AwsCfg = &credentials
	}
	if settings.CosCfg != nil {
		credentials := *settings.CosCfg
		credentials.SecretID, credentials.SecretKey, credentials.SessionToken = "", "", ""
		identity.CosCfg = &credentials
	}
	encoded, err := json.Marshal(identity)
	return sha256.Sum256(encoded), err
}

func (writer *switchingWriter) After() {
	if writer.terminal != nil {
		writer.terminal.After()
	}
	for _, recording := range writer.finalizing {
		<-recording.done
	}
	config.ForgetApplication("recording", writer.Info().Key, writer.Info().UID)
}
