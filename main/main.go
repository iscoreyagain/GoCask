package main

import (
	"fmt"

	"github.com/iscoreyagain/GoCask/internal"
)

func MustGet(db *internal.BitCask, key string) string {
	v, err := db.Get(key)
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return v
}

// Giả sử BitCask, ValuePointer, NewLogEntry, writeLogEntry, readLogEntryAt
// đã được định nghĩa trong cùng package hoặc import từ internal

func main() {
	fmt.Println("=== TESTING WITH WRITE DATA ===")
	db := internal.Init()

	db.Put("name", "Nguyen Le Quoc Thai")
	db.Put("city", "Ho Chi Minh")
	db.Put("age", "22")

	fmt.Println("GET name =", MustGet(db, "name"))
	fmt.Println("GET city =", MustGet(db, "city"))
	fmt.Println("GET age =", MustGet(db, "age"))

	fmt.Println("\n🟢 TẮT CHƯƠNG TRÌNH (giả lập crash)...")
	fmt.Println("🔁 Chạy lại để kiểm tra loadFiles()...\n")
}
