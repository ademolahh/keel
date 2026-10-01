package main

import (
	"log"

	"github.com/ademolahh/raftkv/internal/server"
)

func main() {
	if err := server.Serve(); err != nil {
		log.Fatal(err)
	}
}
