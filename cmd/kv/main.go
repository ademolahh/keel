package main

import (
	"log"

	"github.com/ademolahh/keel/internal/server"
)

func main() {
	if err := server.Serve(); err != nil {
		log.Fatal(err)
	}
}
