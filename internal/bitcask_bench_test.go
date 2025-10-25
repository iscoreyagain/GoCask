package internal

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"
)

func init() {
	runtime.GOMAXPROCS(1) // Force Go to use only 1 CPU core
}

// Benchmark 1: Sustained throughput over sync period
// This measures actual disk write throughput
func BenchmarkBitCask_SustainedThroughput(b *testing.B) {
	dir := "D:\\bitcask_bench\\sustained"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	bc, err := Open(dir)
	if err != nil {
		b.Fatalf("failed to open: %v", err)
	}
	defer bc.Close()

	// Use 1KB values for realistic test
	valueBytes := make([]byte, 1024)
	for i := range valueBytes {
		valueBytes[i] = 'x'
	}
	value := string(valueBytes)

	b.SetBytes(1024) // Report MB/s based on 1KB values
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		if err := bc.Put(key, value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

	b.StopTimer()

	// Wait for final sync
	time.Sleep(syncInterval + 100*time.Millisecond)
	bc.Sync()
}

// Benchmark 2: Write for fixed duration (real-world simulation)
func BenchmarkBitCask_DiskThroughput_10Seconds(b *testing.B) {
	dir := "D:\\bitcask_bench\\10sec"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	bc, err := Open(dir)
	if err != nil {
		b.Fatalf("failed to open: %v", err)
	}
	defer bc.Close()

	valueBytes := make([]byte, 1024)
	for i := range valueBytes {
		valueBytes[i] = 'x'
	}
	value := string(valueBytes)
	duration := 10 * time.Second

	b.ResetTimer()
	start := time.Now()
	writes := 0
	bytesWritten := int64(0)

	for time.Since(start) < duration {
		key := fmt.Sprintf("key_%d", writes)
		if err := bc.Put(key, value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
		writes++
		bytesWritten += 1024
	}

	// Final sync to ensure everything is on disk
	bc.Sync()
	elapsed := time.Since(start)

	b.StopTimer()

	// Report results
	throughputMBps := float64(bytesWritten) / elapsed.Seconds() / (1024 * 1024)
	qps := float64(writes) / elapsed.Seconds()

	b.ReportMetric(qps, "writes/sec")
	b.ReportMetric(throughputMBps, "MB/s")
	b.ReportMetric(float64(writes), "total_writes")

	fmt.Printf("\n")
	fmt.Printf("Duration:     %v\n", elapsed)
	fmt.Printf("Total writes: %d\n", writes)
	fmt.Printf("Throughput:   %.2f MB/s\n", throughputMBps)
	fmt.Printf("QPS:          %.0f writes/sec\n", qps)
	fmt.Printf("Avg latency:  %.3f ms\n", elapsed.Seconds()*1000/float64(writes))
}

// Benchmark 3: With manual Sync() calls (for comparison)
func BenchmarkBitCask_WithManualSync(b *testing.B) {
	dir := "D:\\bitcask_bench\\manual_sync"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	bc, err := Open(dir)
	if err != nil {
		b.Fatalf("failed to open: %v", err)
	}
	defer bc.Close()

	valueBytes := make([]byte, 1024)
	for i := range valueBytes {
		valueBytes[i] = 'x'
	}
	value := string(valueBytes)
	syncEvery := 100 // Sync every 100 writes

	b.SetBytes(1024)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		bc.Put(key, value)

		if i%syncEvery == 0 {
			bc.Sync()
		}
	}

	b.StopTimer()
	bc.Sync()
}

// Benchmark 4: Measure different value sizes
func BenchmarkBitCask_DiskThroughput_100B(b *testing.B) {
	benchmarkDiskThroughputWithSize(b, 100)
}

func BenchmarkBitCask_DiskThroughput_1KB(b *testing.B) {
	benchmarkDiskThroughputWithSize(b, 1024)
}

func BenchmarkBitCask_DiskThroughput_10KB(b *testing.B) {
	benchmarkDiskThroughputWithSize(b, 10*1024)
}

func BenchmarkBitCask_DiskThroughput_100KB(b *testing.B) {
	benchmarkDiskThroughputWithSize(b, 100*1024)
}

func benchmarkDiskThroughputWithSize(b *testing.B, valueSize int) {
	dir := fmt.Sprintf("D:\\bitcask_bench\\size_%d", valueSize)
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	bc, err := Open(dir)
	if err != nil {
		b.Fatalf("failed to open: %v", err)
	}
	defer bc.Close()

	valueBytes := make([]byte, valueSize)
	for i := range valueBytes {
		valueBytes[i] = byte(i % 256)
	}
	value := string(valueBytes)
	duration := 5 * time.Second

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	start := time.Now()
	writes := 0

	for time.Since(start) < duration {
		key := fmt.Sprintf("key_%d", writes)
		bc.Put(key, value)
		writes++
	}

	bc.Sync()
	elapsed := time.Since(start)

	b.StopTimer()

	throughput := float64(writes*valueSize) / elapsed.Seconds() / (1024 * 1024)
	b.ReportMetric(throughput, "MB/s")
	b.ReportMetric(float64(writes)/elapsed.Seconds(), "writes/sec")
}

// Benchmark 5: Sustained throughput with file rotation
func BenchmarkBitCask_WithFileRotation(b *testing.B) {
	dir := "D:\\bitcask_bench\\rotation"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	bc, err := Open(dir)
	if err != nil {
		b.Fatalf("failed to open: %v", err)
	}
	defer bc.Close()

	// Use large values to trigger file rotation
	valueBytes := make([]byte, 10*1024)
	for i := range valueBytes {
		valueBytes[i] = byte(i % 256)
	}
	value := string(valueBytes) // 10KB
	duration := 10 * time.Second

	b.ResetTimer()
	start := time.Now()
	writes := 0
	bytesWritten := int64(0)
	rotations := 0

	lastFileId := bc.currentFileId

	for time.Since(start) < duration {
		key := fmt.Sprintf("key_%d", writes)
		bc.Put(key, value)
		writes++
		bytesWritten += int64(len(value))

		// Count file rotations
		if bc.currentFileId != lastFileId {
			rotations++
			lastFileId = bc.currentFileId
		}
	}

	bc.Sync()
	elapsed := time.Since(start)

	b.StopTimer()

	throughput := float64(bytesWritten) / elapsed.Seconds() / (1024 * 1024)

	fmt.Printf("\n")
	fmt.Printf("Duration:      %v\n", elapsed)
	fmt.Printf("Total writes:  %d\n", writes)
	fmt.Printf("Throughput:    %.2f MB/s\n", throughput)
	fmt.Printf("QPS:           %.0f writes/sec\n", float64(writes)/elapsed.Seconds())
	fmt.Printf("File rotations: %d\n", rotations)
}

// Test function: Measure actual disk I/O
func TestDiskThroughput(t *testing.T) {
	dir := "D:\\bitcask_bench\\disk_test"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	bc, err := Open(dir)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer bc.Close()

	// Test parameters
	duration := 10 * time.Second
	valueSize := 1024 // 1KB
	valueBytes := make([]byte, valueSize)
	for i := range valueBytes {
		valueBytes[i] = byte(i % 256)
	}
	value := string(valueBytes)

	fmt.Printf("\nRunning sustained write test for %v...\n", duration)
	fmt.Printf("Value size: %d bytes\n", valueSize)
	fmt.Printf("Sync interval: %v\n\n", syncInterval)

	start := time.Now()
	writes := 0

	// Write continuously
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	lastWrites := 0

	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				currentWrites := writes
				qps := currentWrites - lastWrites
				fmt.Printf("[%2.0fs] Writes: %6d | QPS: %5d | Total: %6d\n",
					time.Since(start).Seconds(), qps, qps, currentWrites)
				lastWrites = currentWrites
			case <-done:
				return
			}
		}
	}()

	for time.Since(start) < duration {
		key := fmt.Sprintf("key_%d", writes)
		if err := bc.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		writes++
	}

	close(done)

	// Final sync
	bc.Sync()
	elapsed := time.Since(start)

	// Calculate results
	totalBytes := int64(writes * valueSize)
	throughputMBps := float64(totalBytes) / elapsed.Seconds() / (1024 * 1024)
	avgQPS := float64(writes) / elapsed.Seconds()

	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("Duration:        %v\n", elapsed)
	fmt.Printf("Total writes:    %d\n", writes)
	fmt.Printf("Total data:      %.2f MB\n", float64(totalBytes)/(1024*1024))
	fmt.Printf("Avg QPS:         %.0f writes/sec\n", avgQPS)
	fmt.Printf("Throughput:      %.2f MB/s\n", throughputMBps)
	fmt.Printf("Avg latency:     %.3f ms\n", elapsed.Seconds()*1000/float64(writes))
	fmt.Printf("Files created:   %d\n", len(bc.Files))
}
