package observability

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"time"
)

// EvidenceClock belongs to the external coordinator process, outside Actor snapshots.
type EvidenceClock struct {
	SourceInstanceID string `json:"source_instance_id"`
	HostBootID       string `json:"host_boot_id"`
	ClockDomainID    string `json:"clock_domain_id"`
	MonotonicNS      string `json:"monotonic_ns"`
	TimestampNS      string `json:"timestamp_ns"`
	ObservedAtNS     string `json:"observed_at_ns"`
}

type Clock struct {
	origin         time.Time
	instance, boot string
}

func NewClock() (*Clock, error) {
	var identity [16]byte
	if _, err := rand.Read(identity[:]); err != nil {
		return nil, err
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return nil, err
	}
	return &Clock{origin: time.Now(), instance: hex.EncodeToString(identity[:]), boot: strings.TrimSpace(string(boot))}, nil
}

func (c *Clock) Stamp() EvidenceClock {
	now := time.Now()
	stamp := strconv.FormatInt(now.UnixNano(), 10)
	return EvidenceClock{SourceInstanceID: c.instance, HostBootID: c.boot,
		ClockDomainID: "coordinator-process:" + c.instance,
		MonotonicNS:   strconv.FormatInt(now.Sub(c.origin).Nanoseconds(), 10),
		TimestampNS:   stamp, ObservedAtNS: stamp}
}
