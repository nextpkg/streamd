package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/nextpkg/streamd/internal/objectstore"
)

const (
	ModeImmediate = "immediate"
	ModeRound     = "round"
	AsFlv         = "flv"
	AsTs          = "ts"
	DriverFs      = "fs"
	DriverCos     = "cos"
	DriverAws     = "aws"
)

type AppRecorder struct {
	Enable   bool                   `json:"enable"`
	SaveTo   string                 `json:"save_to"`
	SaveAs   string                 `json:"save_as"`
	SaveMode string                 `json:"save_mode"`
	Driver   string                 `json:"driver"`
	Segment  int64                  `json:"segment"`
	Event    RecorderEvent          `json:"event"`
	CosCfg   *objectstore.COSConfig `json:"cos"`
	AwsCfg   *objectstore.S3Config  `json:"aws"`
	cc       *objectstore.Client
	ac       *objectstore.Client
}

func (recorder *AppRecorder) validate() error {
	if !recorder.Enable {
		return nil
	}
	if recorder.SaveTo == "" {
		recorder.SaveTo = filepath.Join(os.TempDir(), "streamd")
	}
	if !filepath.IsAbs(recorder.SaveTo) {
		return errors.New("recording save_to must be absolute")
	}
	if recorder.SaveAs != AsFlv && recorder.SaveAs != AsTs {
		return fmt.Errorf("unsupported save_as %q", recorder.SaveAs)
	}
	if recorder.SaveMode != ModeRound && recorder.SaveMode != ModeImmediate {
		return fmt.Errorf("unsupported save_mode %q", recorder.SaveMode)
	}
	if recorder.Segment <= 0 {
		return errors.New("recording segment must be positive")
	}
	if err := recorder.Event.load(); err != nil {
		return err
	}
	switch recorder.Driver {
	case "", DriverFs:
		return nil
	case DriverCos:
		return recorder.CosCfg.Validate()
	case DriverAws:
		return recorder.AwsCfg.Validate()
	default:
		return errors.New("unsupported recording driver")
	}
}

func (recorder *AppRecorder) prepare(ctx context.Context, previous *AppRecorder, app string) error {
	if !recorder.Enable {
		return nil
	}
	var err error
	switch recorder.Driver {
	case DriverCos:
		recorder.cc, err = objectstore.NewCOS(recorder.CosCfg)
		if err == nil && (previous == nil || !previous.Enable || previous.Driver != DriverCos || previous.cc == nil || previous.CosCfg == nil || *previous.CosCfg != *recorder.CosCfg) {
			err = recorder.cc.Preflight(ctx, path.Join(recorder.CosCfg.Path, app))
		}
	case DriverAws:
		recorder.ac, err = objectstore.NewS3(recorder.AwsCfg)
		if err == nil && (previous == nil || !previous.Enable || previous.Driver != DriverAws || previous.ac == nil || previous.AwsCfg == nil || *previous.AwsCfg != *recorder.AwsCfg) {
			err = recorder.ac.Preflight(ctx, path.Join(recorder.AwsCfg.Path, app))
		}
	case "", DriverFs:
		err = os.MkdirAll(recorder.SaveTo, 0700)
	}
	return err
}

func (recorder *AppRecorder) Normalize(timestamp int64) int64 {
	return timestamp / recorder.Segment * recorder.Segment
}
func (recorder *AppRecorder) CosClient() *objectstore.Client { return recorder.cc }
func (recorder *AppRecorder) AwsClient() *objectstore.Client { return recorder.ac }
func (recorder *AppRecorder) closeClients()                  { recorder.cc.Close(); recorder.ac.Close() }

func (recorder *AppRecorder) RetainClients() func() {
	cosRelease, awsRelease := recorder.cc.Retain(), recorder.ac.Retain()
	return func() { cosRelease(); awsRelease() }
}
