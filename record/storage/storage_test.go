package storage

import (
	"os"
	"testing"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/config"
	"github.com/stretchr/testify/assert"
)

func TestNewStorage(t *testing.T) {
	at := assert.New(t)
	directory := t.TempDir()

	flv := &config.AppRecorder{
		SaveTo:   directory,
		SaveAs:   "flv",
		SaveMode: "round",
		Driver:   "fs",
		Segment:  900,
	}

	info := base.NewInfo("t_app", "t_ins", false)
	s := NewStorage(info, flv, NewEvent())

	at.False(s.IsOpened())
	at.Nil(s.Open())
	at.True(s.IsOpened())

	at.Equal("/t_app/t_ins/", s.baseRela)
	at.Equal(directory+"/t_app/t_ins/", s.basePath)

	_, err := os.Stat(s.fullPath)
	at.Nil(err)

	err = s.Write([]byte{1, 2, 3})
	at.Nil(err)

	at.Nil(s.Close(false))
	at.False(s.IsOpened())

	at.Nil(os.RemoveAll(directory))
}
