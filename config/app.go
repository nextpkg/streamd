package config

import (
	"strings"

	"github.com/nextpkg/streamd/base"
	"github.com/nextpkg/streamd/internal/errutil"
)

// App RTMP直播Application
type App struct {
	Enable     bool        `json:"enable"`
	AppName    string      `json:"app_name"`
	StaticPush []string    `json:"static_push"`
	StaticPull []string    `json:"static_pull"`
	Passerby   bool        `json:"passerby"`
	HLS        AppHLS      `json:"hls"`
	Event      AppEvent    `json:"event"`
	Recorder   AppRecorder `json:"recorder"`
}

func (a *App) validate() error {
	if !a.Enable {
		return nil
	}

	if !base.ValidAppName(a.AppName) {
		return errutil.New("invalid app_name")
	}

	// static pull
	a.StaticPull = filterStringSlice(a.StaticPull, isNotRTMP)

	// static push
	a.StaticPush = filterStringSlice(a.StaticPush, isNotRTMP)

	err := a.HLS.validate()
	if err != nil {
		return errutil.Wrapf(err, "validate hls failed,`app_name`='%s'", a.AppName)
	}

	err = a.Recorder.validate()
	if err != nil {
		return errutil.Wrapf(err, "validate recorder failed,`app_name`='%s'", a.AppName)
	}

	err = a.Event.validate()
	if err != nil {
		return errutil.Wrapf(err, "validate event failed,`app_name`='%s'", a.AppName)
	}

	return nil
}

// StaticRtmp 组装RTMP地址
func (a *App) StaticRtmp(host string, instance string) string {
	return "rtmp://" + strings.Join([]string{host, a.AppName, instance}, "/")
}

// StaticPushList 静态推送列表(rtmp://ip:port/instance)
func (a *App) StaticPushList(instance string) []string {
	urls := a.StaticPush
	if urls == nil {
		return nil
	}

	pushList := make([]string, len(urls))

	for i, u := range urls {
		pushURL := u + "/" + instance
		pushList[i] = pushURL
	}

	return pushList
}

// StaticPullList 静态拉取列表(rtmp://ip:port/instance)
func (a *App) StaticPullList(instance string) []string {
	urls := a.StaticPull
	if len(urls) == 0 {
		return nil
	}

	pullList := make([]string, len(urls))

	for i, u := range urls {
		pullURL := u + "/" + instance
		pullList[i] = pullURL
	}

	return pullList
}
