package config

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"unicode"
)

type discoveryRecord struct {
	Version int    `json:"version"`
	RTMP    string `json:"rtmp"`
	HTTP    string `json:"http"`
}

func discoveryAuthority(value string) (string, error) {
	invalid := errors.New("discovery endpoint must be a host or host:port without URL components")
	if value == "" || len(value) > 1024 || strings.ContainsAny(value, "@/?#\\\"'{}") || strings.IndexFunc(value, func(character rune) bool { return unicode.IsSpace(character) || unicode.IsControl(character) }) >= 0 {
		return "", invalid
	}
	host, port, err := net.SplitHostPort(value)
	if err == nil {
		numeric, err := strconv.Atoi(port)
		if err != nil || numeric < 1 || numeric > 65535 || host == "" || strings.IndexFunc(port, func(character rune) bool { return character < '0' || character > '9' }) >= 0 {
			return "", invalid
		}
		if address, err := netip.ParseAddr(host); err == nil && address.IsUnspecified() {
			return "", invalid
		}
		if strings.ContainsAny(host, ":%[]") {
			address, err := netip.ParseAddr(host)
			if err != nil || !address.Is6() {
				return "", invalid
			}
		}
		return net.JoinHostPort(host, port), nil
	}
	host = value
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	if address, err := netip.ParseAddr(host); err == nil {
		if address.IsUnspecified() {
			return "", invalid
		}
		if address.Is6() {
			return "[" + host + "]", nil
		}
	}
	if strings.ContainsAny(value, ":%[]") {
		return "", invalid
	}
	return value, nil
}

func discoveryAdvertisement(rtmp, http string) (string, error) {
	if http == "" {
		return rtmp, nil
	}
	rtmp, err := discoveryAuthority(rtmp)
	if err != nil {
		return "", err
	}
	http, err = discoveryAuthority(http)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(discoveryRecord{Version: 1, RTMP: rtmp, HTTP: http})
	return string(encoded), err
}

func DiscoveryEndpoint(value, protocol string) (string, error) {
	if protocol != "http" && protocol != "rtmp" {
		return "", errors.New("unsupported discovery protocol")
	}
	if value == "" {
		return "", nil
	}
	if len(value) > 4096 {
		return "", errors.New("discovery record exceeds size limit")
	}
	if !strings.HasPrefix(strings.TrimSpace(value), "{") {
		return discoveryAuthority(value)
	}
	var record discoveryRecord
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return "", errors.New("invalid discovery record")
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF || record.Version != 1 {
		return "", errors.New("unsupported or trailing discovery record")
	}
	rtmp, err := discoveryAuthority(record.RTMP)
	if err != nil {
		return "", err
	}
	http, err := discoveryAuthority(record.HTTP)
	if err != nil {
		return "", err
	}
	if protocol == "http" {
		return http, nil
	}
	return rtmp, nil
}
