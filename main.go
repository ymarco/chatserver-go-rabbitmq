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
	if !isValidBindingKeyComponent(name) {
		fmt.Println("Invalid name:", ErrInvalidTopicComponent)
		os.Exit(1)
	}
	RunClient(name)
}
