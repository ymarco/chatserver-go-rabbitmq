package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	mode := os.Args[1]
	switch mode {
	case "client":
		if len(os.Args) < 4 {
			fmt.Printf("Usage: %s client BINDING_KEY BODY...", os.Args[0])
			os.Exit(1)
		}
		binding_key := os.Args[2]
		body := strings.Join(os.Args[3:], " ")
		client(binding_key, body)
	case "server":
		if len(os.Args) < 3 {
			fmt.Printf("Usage: %s server BINDING_KEY...", os.Args[0])
			os.Exit(1)
		}
		binding_keys := os.Args[2:]
		server(binding_keys)
	}
}
