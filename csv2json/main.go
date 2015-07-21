package main

import (
	"fmt"
	"encoding/json"
	"encoding/csv"
	"os"
	"io"
//	"strconv"
	"bufio"
)

func zipMap(keys []string, values []string) map[string]string {
	retmap := make(map[string]string, len(keys))
	for index, item := range keys {
		retmap[item] = values[index]
	}
	return retmap
}

func converter(keys []string, line []string) string {
	val, err := json.Marshal(zipMap(keys, line))
	if err != nil {
		fmt.Println("ERROR IN CONVERTER!",err)
	}
	return string(val)
}

func convertWorker(worker int, keys []string, c <-chan []string, done chan<- bool) {
//	fmt.Println("Worker",worker,"created!")
	for {
		s, more := <- c
		if more == true {
			fmt.Println(converter(keys, s))
		} else {
//			fmt.Println("Worker",worker,"ending!")
			break
		}
	}
	done <- true
}

func main() {
	workers := 4
	rows := make(chan []string, workers)
	worker_done := make(chan bool, workers)

	in := bufio.NewReader(os.Stdin)
	reader := csv.NewReader(in)
	reader.Comma = ','

	// Read first line.
	first, err := reader.Read()
	if err == io.EOF {
		fmt.Println("Must have input from STDIN.")
		return
	} else if err != nil {
		fmt.Println("Unexpected error reading header:",err)
	}
	//	fmt.Println("Header: ",first)

	for i := 0; i < workers; i++ {
		go convertWorker(i, first, rows, worker_done)
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
	//			fmt.Println("EOF")
			break
		} else if err != nil {
			fmt.Println("Unexpected error reading line", err)
			return
		}
		rows <- record
	}
	close(rows)
	rows_done := workers
	for rows_done > 0 {
		<- worker_done
		rows_done -= 1
	}
}
