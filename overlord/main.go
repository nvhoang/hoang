package main

// Endpoint: first level metric name, host/instance/controller/component_name/time/collector

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type MTSV []byte
type Metric struct {
	m    string
	tags []Tag
}
type Tag struct {
	k string
	v string
}
type Timestamp int64
type Value float64

func main() {
	service := ":10001"
	udpAddr, err := net.ResolveUDPAddr("udp4", service)
	checkError(err)
	conn, err := net.ListenUDP("udp", udpAddr)
	checkError(err)

	var buf [1024]byte

	fmt.Println("Listening for UDP on port ", service)
	metricChannel := make(chan MTSV)
	go processMetric(metricChannel)
	for {
		len, _, err := conn.ReadFromUDP(buf[0:])
		checkError(err)
		metricChannel <- buf[:len]
	}
}

func processMetric(metricChannel chan MTSV) {
	for {
		rawMetric := <-metricChannel
		m, t, v := parseMTSV(rawMetric)
		fmt.Printf("%d: %s -> %f\n", t, m, v)
	}
}

func parseMTSV(m MTSV) (metric Metric, t Timestamp, v Value) {
	s := strings.Split(m.String(), ":")
	metric, _ = parseMetric(s[0])
	if l, err := strconv.ParseInt(strings.TrimSpace(s[1]), 10, 64); err == nil {
		fmt.Println("ts:", l)
		t = Timestamp(l)
	} else {
		panic("Invalid timestamp: " + s[1])
	}
	if f, err := strconv.ParseFloat(strings.TrimSpace(s[2]), 64); err == nil {
		fmt.Println("v:", f)
		v = Value(f)
	} else {
		panic("Invalid value: " + s[2])
	}
	return
}

func parseMetric(m string) (metric Metric, e error) {
	s := strings.FieldsFunc(m, func(r rune) bool {
		switch r {
		case '{', '}', ',':
			return true
		}
		return false
	})
	metric.m = s[0]
	for _, kvs := range s[1:] {
		s := strings.Split(kvs, "=")
		metric.tags = append(metric.tags, Tag{strings.TrimSpace(s[0]), strings.TrimSpace(s[1])})
	}
	return
}

func (m MTSV) String() string {
	return string(m)
}

func (m Metric) String() string {
	s := m.m + "{"
	for _, tag := range m.tags {
		s = s + tag.String()
	}
	s = s + "}"
	return s
}

func (t Tag) String() string {
	return t.k + "=" + t.v
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error %s", err.Error())
		os.Exit(1)
	}
}
