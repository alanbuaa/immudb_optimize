package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	immudb "github.com/codenotary/immudb/pkg/client"
)

const (
	modeStandard = "standard"
	modeVerified = "verified"

	phaseWrite     = "write"
	phaseRead      = "read"
	phaseWriteRead = "write-read"
)

type config struct {
	Host           string        `json:"host"`
	Port           int           `json:"port"`
	Username       string        `json:"username"`
	Password       string        `json:"-"`
	Database       string        `json:"database"`
	Mode           string        `json:"mode"`
	Phase          string        `json:"phase"`
	Workers        int           `json:"workers"`
	WriteCount     int           `json:"write_count"`
	ReadCount      int           `json:"read_count"`
	PreparedCount  int           `json:"prepared_count"`
	ValueSize      int           `json:"value_size"`
	KeyPrefix      string        `json:"key_prefix"`
	Seed           int64         `json:"seed"`
	StateDir       string        `json:"state_dir"`
	ConnectTimeout time.Duration `json:"connect_timeout"`
	OpTimeout      time.Duration `json:"op_timeout"`
	JSONOutput     string        `json:"json_output"`
}

type phaseSummary struct {
	Name           string        `json:"name"`
	RequestedOps   int           `json:"requested_ops"`
	CompletedOps   int           `json:"completed_ops"`
	Errors         int           `json:"errors"`
	TotalBytes     int64         `json:"total_bytes"`
	Elapsed        time.Duration `json:"elapsed"`
	OpsPerSecond   float64       `json:"ops_per_second"`
	BytesPerSecond float64       `json:"bytes_per_second"`
	AvgLatency     time.Duration `json:"avg_latency"`
	MinLatency     time.Duration `json:"min_latency"`
	MedianLatency  time.Duration `json:"median_latency"`
	P95Latency     time.Duration `json:"p95_latency"`
	P99Latency     time.Duration `json:"p99_latency"`
	MaxLatency     time.Duration `json:"max_latency"`
}

type runSummary struct {
	StartedAt  time.Time      `json:"started_at"`
	FinishedAt time.Time      `json:"finished_at"`
	Config     config         `json:"config"`
	Phases     []phaseSummary `json:"phases"`
}

type workerMeasurement struct {
	Latencies []time.Duration
	Bytes     int64
	Err       error
}

type benchOperation func(ctx context.Context, client immudb.ImmuClient, workerID int, opIndex int, rng *rand.Rand) (int, error)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg := parseFlags()
	if err := cfg.validate(); err != nil {
		log.Fatalf("invalid arguments: %v", err)
	}

	if cfg.KeyPrefix == "" && cfg.Phase != phaseRead {
		cfg.KeyPrefix = fmt.Sprintf("baseline-%s", time.Now().Format("20060102-150405"))
	}

	if cfg.Phase == phaseRead && cfg.KeyPrefix == "" {
		log.Fatal("phase=read requires -key-prefix so the program knows which existing keys to read")
	}

	log.Printf("immudb baseline benchmark starting")
	log.Printf("target=%s:%d db=%s mode=%s phase=%s workers=%d prefix=%s value-size=%dB",
		cfg.Host, cfg.Port, cfg.Database, cfg.Mode, cfg.Phase, cfg.Workers, cfg.KeyPrefix, cfg.ValueSize)

	summary := runSummary{
		StartedAt: time.Now(),
		Config:    cfg,
	}

	switch cfg.Phase {
	case phaseWrite:
		writeSummary, err := runPhase(cfg, "write", cfg.WriteCount, newWriteOperation(cfg))
		summary.Phases = append(summary.Phases, writeSummary)
		if err != nil {
			log.Fatalf("write phase failed: %v", err)
		}
	case phaseRead:
		readSummary, err := runPhase(cfg, "read", cfg.ReadCount, newReadOperation(cfg, cfg.PreparedCount))
		summary.Phases = append(summary.Phases, readSummary)
		if err != nil {
			log.Fatalf("read phase failed: %v", err)
		}
	case phaseWriteRead:
		writeSummary, err := runPhase(cfg, "write", cfg.WriteCount, newWriteOperation(cfg))
		summary.Phases = append(summary.Phases, writeSummary)
		if err != nil {
			log.Fatalf("write phase failed: %v", err)
		}

		readTarget := cfg.WriteCount
		if cfg.PreparedCount > 0 {
			readTarget = cfg.PreparedCount
		}
		readSummary, err := runPhase(cfg, "read", cfg.ReadCount, newReadOperation(cfg, readTarget))
		summary.Phases = append(summary.Phases, readSummary)
		if err != nil {
			log.Fatalf("read phase failed: %v", err)
		}
	default:
		log.Fatalf("unsupported phase %q", cfg.Phase)
	}

	summary.FinishedAt = time.Now()

	if cfg.JSONOutput != "" {
		if err := writeJSONSummary(cfg.JSONOutput, summary); err != nil {
			log.Fatalf("failed to write JSON summary: %v", err)
		}
		log.Printf("json summary written to %s", cfg.JSONOutput)
	}
}

func parseFlags() config {
	cfg := config{}

	flag.StringVar(&cfg.Host, "host", "127.0.0.1", "immudb host")
	flag.IntVar(&cfg.Port, "port", 3322, "immudb port")
	flag.StringVar(&cfg.Username, "user", "immudb", "immudb username")
	flag.StringVar(&cfg.Password, "password", "immudb", "immudb password")
	flag.StringVar(&cfg.Database, "db", "defaultdb", "immudb database name")

	flag.StringVar(&cfg.Mode, "mode", modeStandard, "api mode: standard or verified")
	flag.StringVar(&cfg.Phase, "phase", phaseWriteRead, "benchmark phase: write, read or write-read")
	flag.IntVar(&cfg.Workers, "workers", 8, "number of parallel workers")
	flag.IntVar(&cfg.WriteCount, "write-count", 10000, "number of write operations")
	flag.IntVar(&cfg.ReadCount, "read-count", 10000, "number of read operations")
	flag.IntVar(&cfg.PreparedCount, "prepared-count", 0, "number of existing keys available for read-only mode")
	flag.IntVar(&cfg.ValueSize, "value-size", 1024, "value size in bytes")
	flag.StringVar(&cfg.KeyPrefix, "key-prefix", "", "key prefix used for this dataset; auto-generated except in read-only mode")
	flag.Int64Var(&cfg.Seed, "seed", 42, "random seed used to choose read keys")
	flag.StringVar(&cfg.StateDir, "state-dir", ".immu-bench-state", "directory used by the client to store local state")
	flag.DurationVar(&cfg.ConnectTimeout, "connect-timeout", 10*time.Second, "timeout for opening a session")
	flag.DurationVar(&cfg.OpTimeout, "op-timeout", 15*time.Second, "timeout for each Set/Get operation")
	flag.StringVar(&cfg.JSONOutput, "json-output", "", "optional path for writing a JSON summary")

	flag.Parse()
	return cfg
}

func (cfg config) validate() error {
	if cfg.Mode != modeStandard && cfg.Mode != modeVerified {
		return fmt.Errorf("unsupported mode %q", cfg.Mode)
	}

	switch cfg.Phase {
	case phaseWrite:
		if cfg.WriteCount <= 0 {
			return errors.New("write phase requires -write-count > 0")
		}
	case phaseRead:
		if cfg.ReadCount <= 0 {
			return errors.New("read phase requires -read-count > 0")
		}
		if cfg.PreparedCount <= 0 {
			return errors.New("read phase requires -prepared-count > 0")
		}
	case phaseWriteRead:
		if cfg.WriteCount <= 0 {
			return errors.New("write-read phase requires -write-count > 0")
		}
		if cfg.ReadCount <= 0 {
			return errors.New("write-read phase requires -read-count > 0")
		}
	default:
		return fmt.Errorf("unsupported phase %q", cfg.Phase)
	}

	if cfg.Workers <= 0 {
		return errors.New("workers must be > 0")
	}
	if cfg.ValueSize <= 0 {
		return errors.New("value-size must be > 0")
	}
	if cfg.ConnectTimeout <= 0 {
		return errors.New("connect-timeout must be > 0")
	}
	if cfg.OpTimeout <= 0 {
		return errors.New("op-timeout must be > 0")
	}

	return nil
}

func newWriteOperation(cfg config) benchOperation {
	return func(ctx context.Context, client immudb.ImmuClient, _ int, opIndex int, _ *rand.Rand) (int, error) {
		key := buildKey(cfg.KeyPrefix, opIndex)
		value := buildValue(cfg.KeyPrefix, opIndex, cfg.ValueSize)

		var err error
		if cfg.Mode == modeVerified {
			_, err = client.VerifiedSet(ctx, key, value)
		} else {
			_, err = client.Set(ctx, key, value)
		}
		if err != nil {
			return 0, err
		}

		return len(key) + len(value), nil
	}
}

func newReadOperation(cfg config, availableKeys int) benchOperation {
	return func(ctx context.Context, client immudb.ImmuClient, _ int, _ int, rng *rand.Rand) (int, error) {
		keyIndex := rng.Intn(availableKeys)
		key := buildKey(cfg.KeyPrefix, keyIndex)
		expected := buildValue(cfg.KeyPrefix, keyIndex, cfg.ValueSize)

		var (
			value []byte
			err   error
		)

		if cfg.Mode == modeVerified {
			entry, getErr := client.VerifiedGet(ctx, key)
			if getErr != nil {
				return 0, getErr
			}
			value = entry.Value
		} else {
			entry, getErr := client.Get(ctx, key)
			if getErr != nil {
				return 0, getErr
			}
			value = entry.Value
		}

		if !bytes.Equal(value, expected) {
			return 0, fmt.Errorf("value mismatch for key %q", string(key))
		}

		return len(key) + len(value), err
	}
}

func runPhase(cfg config, name string, totalOps int, op benchOperation) (phaseSummary, error) {
	start := time.Now()
	rootCtx, cancelAll := context.WithCancel(context.Background())
	defer cancelAll()

	var nextOp atomic.Int64
	var completed atomic.Int64

	progressDone := make(chan struct{})
	go reportProgress(name, totalOps, &completed, progressDone)

	measurements := make(chan workerMeasurement, cfg.Workers)
	var wg sync.WaitGroup

	for workerID := 0; workerID < cfg.Workers; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			client, err := openClient(rootCtx, cfg, workerID)
			if err != nil {
				measurements <- workerMeasurement{Err: fmt.Errorf("worker %d failed to open session: %w", workerID, err)}
				cancelAll()
				return
			}
			defer closeClient(client)

			rng := rand.New(rand.NewSource(cfg.Seed + int64(workerID)*7919))
			latencies := make([]time.Duration, 0, totalOps/cfg.Workers+1)
			var totalBytes int64

			for {
				if rootCtx.Err() != nil {
					break
				}

				opIndex := int(nextOp.Add(1)) - 1
				if opIndex >= totalOps {
					break
				}

				opCtx, cancel := context.WithTimeout(rootCtx, cfg.OpTimeout)
				opStart := time.Now()
				n, err := op(opCtx, client, workerID, opIndex, rng)
				cancel()

				if err != nil {
					measurements <- workerMeasurement{
						Latencies: latencies,
						Bytes:     totalBytes,
						Err:       fmt.Errorf("worker %d operation %d failed: %w", workerID, opIndex, err),
					}
					cancelAll()
					return
				}

				latencies = append(latencies, time.Since(opStart))
				totalBytes += int64(n)
				completed.Add(1)
			}

			measurements <- workerMeasurement{
				Latencies: latencies,
				Bytes:     totalBytes,
			}
		}(workerID)
	}

	wg.Wait()
	close(progressDone)
	close(measurements)

	summary := phaseSummary{
		Name:         name,
		RequestedOps: totalOps,
		Elapsed:      time.Since(start),
	}

	var allLatencies []time.Duration
	var firstErr error
	for measurement := range measurements {
		allLatencies = append(allLatencies, measurement.Latencies...)
		summary.CompletedOps += len(measurement.Latencies)
		summary.TotalBytes += measurement.Bytes
		if measurement.Err != nil {
			summary.Errors++
			if firstErr == nil {
				firstErr = measurement.Err
			}
		}
	}

	fillLatencyStats(&summary, allLatencies)
	if summary.Elapsed > 0 {
		summary.OpsPerSecond = float64(summary.CompletedOps) / summary.Elapsed.Seconds()
		summary.BytesPerSecond = float64(summary.TotalBytes) / summary.Elapsed.Seconds()
	}

	log.Printf("[%s] completed=%d/%d errors=%d elapsed=%s ops/s=%.2f bytes/s=%s avg=%s p50=%s p95=%s p99=%s max=%s",
		name,
		summary.CompletedOps,
		summary.RequestedOps,
		summary.Errors,
		summary.Elapsed,
		summary.OpsPerSecond,
		humanRate(summary.BytesPerSecond),
		summary.AvgLatency,
		summary.MedianLatency,
		summary.P95Latency,
		summary.P99Latency,
		summary.MaxLatency,
	)

	return summary, firstErr
}

func openClient(parent context.Context, cfg config, workerID int) (immudb.ImmuClient, error) {
	stateDir := filepath.Join(cfg.StateDir, sanitizePathSegment(cfg.KeyPrefix), fmt.Sprintf("worker-%02d", workerID))
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, err
	}

	opts := immudb.DefaultOptions().
		WithAddress(cfg.Host).
		WithPort(cfg.Port).
		WithDir(stateDir)

	client := immudb.NewClient().WithOptions(opts)

	ctx, cancel := context.WithTimeout(parent, cfg.ConnectTimeout)
	defer cancel()

	if err := client.OpenSession(ctx, []byte(cfg.Username), []byte(cfg.Password), cfg.Database); err != nil {
		return nil, err
	}

	return client, nil
}

func closeClient(client immudb.ImmuClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.CloseSession(ctx); err != nil {
		log.Printf("close session warning: %v", err)
	}
}

func reportProgress(name string, totalOps int, completed *atomic.Int64, done <-chan struct{}) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			doneOps := completed.Load()
			percent := 0.0
			if totalOps > 0 {
				percent = float64(doneOps) / float64(totalOps) * 100
			}
			log.Printf("[%s] progress=%d/%d (%.1f%%)", name, doneOps, totalOps, percent)
		}
	}
}

func fillLatencyStats(summary *phaseSummary, latencies []time.Duration) {
	if len(latencies) == 0 {
		return
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	var total time.Duration
	for _, latency := range latencies {
		total += latency
	}

	summary.MinLatency = latencies[0]
	summary.MaxLatency = latencies[len(latencies)-1]
	summary.AvgLatency = total / time.Duration(len(latencies))
	summary.MedianLatency = percentile(latencies, 50)
	summary.P95Latency = percentile(latencies, 95)
	summary.P99Latency = percentile(latencies, 99)
}

func percentile(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	if p <= 0 {
		return latencies[0]
	}
	if p >= 100 {
		return latencies[len(latencies)-1]
	}

	rank := int(math.Ceil((p/100)*float64(len(latencies)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(latencies) {
		rank = len(latencies) - 1
	}
	return latencies[rank]
}

func buildKey(prefix string, index int) []byte {
	return []byte(fmt.Sprintf("%s:%012d", prefix, index))
}

func buildValue(prefix string, index int, size int) []byte {
	if size <= 0 {
		return nil
	}

	seed := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", prefix, index)))
	current := seed
	value := make([]byte, size)
	offset := 0

	for offset < size {
		n := copy(value[offset:], current[:])
		offset += n
		if offset < size {
			current = sha256.Sum256(current[:])
		}
	}

	return value
}

func sanitizePathSegment(segment string) string {
	replacer := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		":", "_",
		"*", "_",
		"?", "_",
		"\"", "_",
		"<", "_",
		">", "_",
		"|", "_",
	)

	cleaned := replacer.Replace(segment)
	if cleaned == "" {
		return "default"
	}
	return cleaned
}

func humanRate(bytesPerSecond float64) string {
	units := []string{"B/s", "KB/s", "MB/s", "GB/s", "TB/s"}
	value := bytesPerSecond
	unit := 0

	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}

	return fmt.Sprintf("%.2f %s", value, units[unit])
}

func writeJSONSummary(path string, summary runSummary) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0o644)
}
