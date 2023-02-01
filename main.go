package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: %s USERNAME\n", os.Args[0])
	}
	name := os.Args[1]
	RunClient(name)
}
