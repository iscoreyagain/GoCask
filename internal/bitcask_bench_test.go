package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkBitCask_Put(b *testing.B) {
	dir := filepath.Join(os.TempDir(), "bitcask_bench_put")
	os.RemoveAll(dir)
	bc := Init()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		bc.Put(key, value)
	}
}

func BenchmarkBitCask_Get(b *testing.B) {
	dir := filepath.Join(os.TempDir(), "bitcask_bench_get")
	os.RemoveAll(dir)
	bc := Init()

	// Preload keys
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		bc.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%100000)
		bc.Get(key)
	}
}

func BenchmarkBitCask_Delete(b *testing.B) {
	dir := filepath.Join(os.TempDir(), "bitcask_bench_delete")
	os.RemoveAll(dir)
	bc := Init()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		bc.Put(key, "some_value")
		bc.Delete(key)
	}
}
