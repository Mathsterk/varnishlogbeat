package main

import (
	"os"

	"github.com/mathsterk/varnishlogbeat/cmd"

	_ "github.com/mathsterk/varnishlogbeat/include"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
