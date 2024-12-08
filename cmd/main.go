package main

import (
	"flag"
	"fmt"
	"os"
	"strconv" 
	"sync"

	"distsys/raft/replica"
)

func main() {
	myPortPtr := flag.String("port", "", "Port for the replica instance (required)")
	flag.Parse()

	if *myPortPtr == "" {
		fmt.Fprintln(os.Stderr, "Error: --port argument is required")
		os.Exit(1)
	}

	MyPort, err := strconv.Atoi(*myPortPtr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid port value '%s': %v\n", *myPortPtr, err)
		os.Exit(1)
	}
	ports := []int{5030, 5031, 5032}
	var wg sync.WaitGroup
	replica.CreateReplica(MyPort, ports).Start(&wg)
	wg.Wait()
}