package httpx

import (
	"encoding/json"
	"net/http"
)

type Writer struct{ http.ResponseWriter }

func NewWriter(response http.ResponseWriter) *Writer { return &Writer{response} }

func (writer *Writer) QuickSendJSON(code int, message string, data any) {
	writer.Header().Set("Content-Type", "application/json")
	status := code
	if status == 0 {
		status = http.StatusOK
	}
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]any{"code": code, "message": message, "data": data})
}
