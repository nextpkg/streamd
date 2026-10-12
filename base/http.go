package base

import "net/http"

type Router interface {
	Init(router *http.ServeMux) *http.ServeMux
}
