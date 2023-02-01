package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: %s server BINDING_KEY...", os.Args[0])
		os.Exit(1)
	}
	binding_keys := os.Args[1:]
	server(binding_keys)
}
