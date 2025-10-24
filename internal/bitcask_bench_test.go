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
	bc := InitWithDir(dir)

	// Precompute keys and values
	keys := make([]string, b.N)
	values := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
		values[i] = fmt.Sprintf("value_%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bc.Put(keys[i], values[i])
	}
}

func BenchmarkBitCask_Get(b *testing.B) {
	dir := filepath.Join(os.TempDir(), "bitcask_bench_get")
	os.RemoveAll(dir)
	bc := InitWithDir(dir)

	numKeys := 10000
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		bc.Put(key, value)
		keys[i] = key // precompute
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%numKeys] // just reuse precomputed string
		bc.Get(key)
	}
}

func BenchmarkBitCask_Delete(b *testing.B) {
	dir := filepath.Join(os.TempDir(), "bitcask_bench_delete")
	os.RemoveAll(dir)
	bc := InitWithDir(dir)

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		bc.Put(key, "some_value")
		bc.Delete(key)
	}
}
