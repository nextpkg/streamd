package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	errors "github.com/nextpkg/streamd/internal/errutil"
	log "github.com/nextpkg/streamd/internal/logging"
	"github.com/nextpkg/streamd/internal/mediatime"
	"github.com/nextpkg/streamd/metrics"
	"github.com/nextpkg/streamd/record/storage/driver"
	"github.com/nextpkg/streamd/record/storage/driver/aws"
	"github.com/nextpkg/streamd/record/storage/driver/cos"
	"github.com/nextpkg/streamd/record/storage/driver/file"
	"sync/atomic"
)

// Storage 存储服务
type Storage struct {
	cfg      *config.AppRecorder
	event    *Event
	fragment *base.Fragment
	info     *base.Info

	dr                 driver.Driver
	isOpen             atomic.Bool
	append             bool
	initErr            error
	lastError          error
	forceComplete      bool
	resumeFailed       bool
	release            func()
	releaseDriver      func()
	finalizationBudget *FinalizationBudget
	finalizations      []*segmentFinalization
	finalizationError  error

	// time
	cTime time.Time // 文件被创建时的真正时间
	nTime time.Time // 文件被创建时的标准时间
	size  int       // 文件尺寸，单位：B

	// path
	basePath string // 最终被存储的完整路径名（不含文件名）
	fullPath string // 最终被存储的完整路径名（含文件名）
	baseRela string // 最终被存储的相对路径名（不含文件名）
	fullRela string // 最终被存储的相对路径名（含文件名）
}

// NewStorage 存储服务
func NewStorage(info *base.Info, cfg *config.AppRecorder, event *Event) *Storage {
	baseRela := "/" + info.Key + "/"
	basePath := cfg.SaveTo + baseRela

	var err error
	if !base.ValidAppName(info.App) || !base.ValidStreamName(info.Instance) || info.Key != info.App+"/"+info.Instance || !filepath.IsLocal(info.Key) {
		err = errors.New("invalid recording stream path")
	}

	return &Storage{
		cfg:                cfg,
		event:              event,
		fragment:           base.NewFragment(0),
		baseRela:           baseRela,
		basePath:           basePath,
		append:             true,
		info:               info,
		initErr:            err,
		release:            cfg.RetainClients(),
		finalizationBudget: NewFinalizationBudget(),
	}
}

func (store *Storage) RefreshSettings(settings *config.AppRecorder) {
	if store.cfg == settings {
		return
	}
	release := settings.RetainClients()
	if store.release != nil {
		store.release()
	}
	store.cfg, store.release = settings, release
}

func (store *Storage) Release() {
	_ = store.reapFinalizations(true) // Finalization failures are retained and reported by the worker.
	if store.release != nil {
		store.release()
		store.release = nil
	}
}

// Open 打开存储器
func (s *Storage) Open() (result error) {
	defer func() {
		if result != nil {
			s.lastError = result
		}
	}()
	if s.initErr != nil {
		return s.initErr
	}
	if s.isOpen.Load() {
		return nil
	}
	_ = s.reapFinalizations(false) // Collect completed segments; retain errors for the blocking close.

	err := s.prepare()
	if err != nil {
		return err
	}

	if err = s.reserveCloudRecording(); err != nil {
		return err
	}
	err = s.open()
	if err != nil {
		if s.releaseDriver != nil {
			s.releaseDriver()
			s.releaseDriver = nil
		}
		return err
	}

	metrics.AddGlobal(metrics.Recording, 1)
	s.size = 0
	s.resumeFailed = false
	s.lastError = nil
	s.isOpen.Store(true)

	return nil
}

func (s *Storage) prepare() error {
	// prepare fragment
	s.fragment.Create()

	// prepare time
	s.cTime = time.Now()
	s.nTime = time.Unix(s.cfg.Normalize(s.cTime.Unix()), 0)

	// prepare path
	switch s.cfg.SaveMode {
	case "round":
		filename := fmt.Sprintf("%d.%s", s.nTime.Unix(), s.cfg.SaveAs)

		s.fullPath = s.basePath + filename
		s.fullRela = s.baseRela + filename
	case "immediate":
		filename := fmt.Sprintf("%d.%s", s.cTime.Unix(), s.cfg.SaveAs)

		s.fullPath = s.basePath + filename
		s.fullRela = s.baseRela + filename
	default:
		filename := fmt.Sprintf("%d.%s", s.cTime.Unix(), s.cfg.SaveAs)

		s.fullPath = s.basePath + filename
		s.fullRela = s.baseRela + filename
	}

	return nil
}

func (s *Storage) open() error {
	var path string
	switch s.cfg.Driver {
	case "", config.DriverFs:
		if err := os.MkdirAll(s.basePath, 0700); err != nil {
			return err
		}
		path = s.fullPath
		s.dr = file.NewDriver(path)
	case config.DriverCos:
		client := s.cfg.CosClient()
		if client == nil {
			return errors.New("cos client is uninitialized")
		}

		path = s.cfg.CosCfg.Path + s.fullRela
		s.dr = cos.NewDriver(client, path, s.cfg.Segment, s.append)
	case config.DriverAws:
		client := s.cfg.AwsClient()
		if client == nil {
			return errors.New("aws client is uninitialized")
		}

		path = s.cfg.AwsCfg.Path + s.fullRela
		s.dr = aws.NewDriver(client, path, s.cfg.Segment, s.cfg.AwsCfg.Bucket, s.append)
	default:
		return errors.New("unknown driver")
	}

	log.Tracef("open=%s,driver=%s", path, s.cfg.Driver)

	err := s.dr.Open()
	if err != nil {
		return errors.Wrap(err, "open failed")
	}

	s.event.OnRecord(s.fullRela)
	return nil
}

// Close 关闭当前片段；block 为 true 时等待片段收尾完成并返回累计错误。
func (s *Storage) Close(block bool) (result error) {
	_ = s.reapFinalizations(false) // Collect completed segments; retain errors for the blocking close.
	if !s.isOpen.CompareAndSwap(true, false) {
		if block {
			if s.WaitingFinalization() {
				s.lastError = nil
			}
			return s.reapFinalizations(true)
		}
		return nil
	}

	metrics.AddGlobal(metrics.Recording, -1)
	metrics.Counter(s.info.App, s.info.Instance, metrics.RecordingTime, float64(time.Now().Unix()-s.cTime.Unix()))
	metrics.Counter(s.info.App, s.info.Instance, metrics.RecordingSize, float64(s.size))

	log.Tracef("close=%s,driver=%s", s.fullRela, s.cfg.Driver)
	complete := !s.resumeFailed
	if s.cfg.Driver == config.DriverCos || s.cfg.Driver == config.DriverAws {
		if s.cfg.SaveMode == config.ModeRound && !s.CanSplit() && !s.forceComplete {
			log.Debugf("prevent to complete task(%s) because of non-splittable round mode,driver=%s",
				s.fullRela, s.cfg.Driver)
			complete = false
		}
	}
	var prior error
	if s.resumeFailed {
		prior = s.lastError
	}
	result = s.finalizeSegment(complete, block, prior)
	if result != nil {
		s.lastError = result
	}
	if block {
		result = errors.Join(result, s.reapFinalizations(true))
	}
	return result
}

// Write frame
func (s *Storage) Write(data []byte, next ...uint64) (result error) {
	defer func() {
		if result != nil {
			s.lastError = result
		}
	}()
	if !s.isOpen.Load() {
		return nil
	}
	if s.resumeFailed {
		return s.lastError
	}

	var err error
	if writer, ok := s.dr.(interface {
		WriteMedia([]byte, mediatime.State) error
	}); ok && len(next) != 0 {
		err = writer.WriteMedia(data, mediatime.State{Format: s.cfg.SaveAs, Next: next[0]})
	} else {
		err = s.dr.Write(data)
	}
	if err != nil {
		return err
	}
	s.size += len(data)
	return nil
}

func (s *Storage) ForceCompletion() { s.forceComplete = true }
func (s *Storage) Error() error     { return s.lastError }

func (store *Storage) Resume(initialize func(io.ReaderAt, int64) (uint32, error), restart ...func(uint32) (uint32, error)) (offset uint32, result error) {
	defer func() {
		if result != nil {
			store.lastError = result
			store.resumeFailed = true
		}
	}()
	if cloud, ok := store.dr.(interface {
		ResumeMedia() (*mediatime.State, int64)
	}); ok {
		state, size := cloud.ResumeMedia()
		if size != 0 {
			if err := state.Validate(size); err != nil {
				return 0, err
			}
			if state.Format != store.cfg.SaveAs || len(restart) == 0 {
				return 0, errors.New("recording media checkpoint cannot be applied to this format")
			}
			return restart[0](uint32(state.Next))
		}
	}
	var reader io.ReaderAt
	var size int64
	if existing, ok := store.dr.(interface{ Existing() (io.ReaderAt, int64) }); ok {
		reader, size = existing.Existing()
	}
	return initialize(reader, size)
}

func (s *Storage) UpdateTime(timestamp uint32) { s.fragment.Update(timestamp) }

// WriteAt update pts and write byte
func (s *Storage) WriteAt(pts uint32, data []byte, next ...uint64) error {
	s.fragment.Update(pts)
	return s.Write(data, next...)
}

// ReadLastFile 读取最后一个文件的内存（哪怕已经关闭）(测试用）
func (s *Storage) ReadLastFile() ([]byte, error) {
	return os.ReadFile(s.fullPath)
}

// CanSplit 指示是否能被分割
func (s *Storage) CanSplit() bool {
	switch s.cfg.SaveMode {
	case config.ModeRound:
		if s.cfg.Normalize(time.Now().Unix()) != s.nTime.Unix() {
			return true
		}
	case config.ModeImmediate:
		if s.fragment.Duration() >= s.cfg.Segment*1000 {
			return true
		}
	default:
		if s.fragment.Duration() >= s.cfg.Segment*1000 {
			return true
		}
	}
	return false
}

// CreatedTime 文件创建时间（单位：微秒）
func (s *Storage) CreatedTime() int64 {
	return s.cTime.UnixNano() / int64(time.Millisecond)
}

// IsOpened 存储器是否在运行状态
func (s *Storage) IsOpened() bool {
	return s.isOpen.Load()
}
