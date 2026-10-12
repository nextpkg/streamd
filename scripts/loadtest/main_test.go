package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadOptionsPreservePresetConcurrencyAndScanDefaults(test *testing.T) {
	settings, err := parseOptions([]string{"publish", "-512k", "-c", "19", "-duration", "1m", "-stagger", "2s"}, io.Discard)
	require.NoError(test, err)
	require.Equal(test, "512k", settings.bitrate)
	require.Equal(test, 19, settings.concurrency)
	require.Equal(test, time.Minute, settings.duration)
	require.Equal(test, 2*time.Second, settings.stagger)
	input := filepath.Join(test.TempDir(), "input.flv")
	require.NoError(test, os.WriteFile(input, []byte("fixture"), 0600))
	settings, err = parseOptions([]string{"slab", "-i", input}, io.Discard)
	require.NoError(test, err)
	require.Equal(test, 32, settings.minimum)
	require.Equal(test, 256, settings.maximum)
	require.Equal(test, 8, settings.step)
	require.Equal(test, 100*time.Second, settings.duration)
	settings, err = parseOptions([]string{"play"}, io.Discard)
	require.NoError(test, err)
	require.Zero(test, settings.duration)
	require.Equal(test, 6553*time.Millisecond, settings.stagger)
}

func TestLoadOptionsRejectInvalidInputsBeforeCreatingEvidence(test *testing.T) {
	for _, arguments := range [][]string{
		{"invalid"}, {"publish", "-c", "0"}, {"play", "-c", "-1"}, {"play", "-duration", "-1s"}, {"publish", "-stagger", "-1s"},
		{"publish", "-128k", "-256k"}, {"publish", "-i", "does-not-exist"}, {"play", "-r", "http://127.0.0.1/live/name"},
		{"play", "-r", "rtmp://127.0.0.1/live"}, {"play", "-i", "ignored"}, {"slab"}, {"slab", "-max", "0"}, {"slab", "-step", "0"}, {"slab", "-min", "-1"}, {"play", "extra"},
	} {
		_, err := parseOptions(arguments, io.Discard)
		require.Error(test, err, arguments)
	}
	var output bytes.Buffer
	_, err := parseOptions([]string{"slab", "-h"}, &output)
	require.ErrorIs(test, err, flag.ErrHelp)
	require.Contains(test, output.String(), "default 1m40s")
}

func TestLoadPublisherSuffixPreservesPathAndQuery(test *testing.T) {
	actual, err := streamURL("rtmp://user:password@127.0.0.1:1935/live/a%2Fb?token=a%2Bb", 3)
	require.NoError(test, err)
	require.Equal(test, "rtmp://user:password@127.0.0.1:1935/live/a%2Fb_3?token=a%2Bb", actual)
	actual, err = streamURL("rtmp://127.0.0.1/live/name?key=value", 0)
	require.NoError(test, err)
	require.Equal(test, "rtmp://127.0.0.1/live/name?key=value", actual)
}

func TestLoadFFmpegCommandsRetainLoopPacingAndCopySemantics(test *testing.T) {
	settings := options{mode: "publish", input: "fixture.flv", duration: time.Minute}
	arguments := ffmpegArguments(settings, "rtmp://localhost/live/name_1", true)
	require.Contains(test, strings.Join(arguments, " "), "-stream_loop -1 -re -i fixture.flv")
	require.Contains(test, strings.Join(arguments, " "), "-t 60.000000")
	require.Equal(test, "rtmp://localhost/live/name_1", arguments[len(arguments)-1])
	settings.mode = "slab"
	require.NotContains(test, ffmpegArguments(settings, "rtmp://localhost/live/name", false), "-re")
	settings.mode, settings.duration = "play", 0
	arguments = ffmpegArguments(settings, "rtmp://localhost/live/name", true)
	require.NotContains(test, arguments, "-stream_loop")
	require.NotContains(test, arguments, "-t")
	require.Contains(test, strings.Join(arguments, " "), "-c copy -f flv")
	require.Equal(test, os.DevNull, arguments[len(arguments)-1])
}

func TestLoadProgressHandlesFragmentsAndBoundsInput(test *testing.T) {
	writer := &progressWriter{}
	for _, body := range []string{"fra", "me= 25\nout_time_us=N/A\n", "out_time_us=1000000\npro", "gress=end\n"} {
		written, err := writer.Write([]byte(body))
		require.NoError(test, err)
		require.Equal(test, len(body), written)
	}
	require.Equal(test, mediaProgress{Frames: 25, Microseconds: 1000000, End: true}, writer.snapshot())
	_, err := writer.Write(bytes.Repeat([]byte("x"), 4097))
	require.Error(test, err)
	_, err = (&progressWriter{}).Write([]byte("frame=not-numeric\n"))
	require.Error(test, err)
}

func TestLoadCancellationPreventsDelayedClientLaunch(test *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results, err := runLoad(ctx, options{mode: "play", concurrency: 16, stagger: time.Minute, target: "rtmp://localhost/live/name", ffmpeg: "must-not-execute"}, test.TempDir())
	require.True(test, errors.Is(err, context.Canceled))
	require.Len(test, results, 16)
	for _, result := range results {
		require.Zero(test, result.PID)
		require.False(test, result.Passed)
	}
}

func TestLoadFailedAdmissionCancelsAndJoinsOtherClients(test *testing.T) {
	results, err := runLoad(context.Background(), options{mode: "play", concurrency: 16, target: "rtmp://localhost/live/name", ffmpeg: filepath.Join(test.TempDir(), "absent")}, test.TempDir())
	require.Error(test, err)
	require.Len(test, results, 16)
	for _, result := range results {
		require.Zero(test, result.PID)
		require.False(test, result.Passed)
		require.NotEmpty(test, result.Error)
	}
}
