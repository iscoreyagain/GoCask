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

func main() {
	fmt.Println("=== TESTING WITH RECOVERY ===")
	db := internal.Init() // <-- loadFiles() được gọi trong Init()

	fmt.Println("GET name =", MustGet(db, "name"))
	fmt.Println("GET city =", MustGet(db, "city"))
	fmt.Println("GET age =", MustGet(db, "age"))
}
