package main

import (
	"bufio"
	"io"
	"log"
)

// ScanLine is a wrapper around Scanner.Scan() that returns EOF as errors
// instead of bools
func ScanLine(s *bufio.Scanner) (string, error) {
	if !s.Scan() {
		if s.Err() == nil {
			return "", io.EOF
		} else {
			return "", s.Err()
		}
	}
	return s.Text(), nil
}

func ClosePrintErr(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Println(err)
	}
}
