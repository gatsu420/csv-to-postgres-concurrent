package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func openCsv(path string) (*csv.Reader, *os.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	reader := csv.NewReader(file)
	return reader, file, nil
}

func readCsv(rowCh chan []string, csvReader *csv.Reader) {
	defer close(rowCh)

	firstIdx := true
	for {
		row, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			continue
		}

		if firstIdx {
			firstIdx = false
			continue
		}

		rowCh <- row
	}
}

func newDbPool(dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

func distributeWorker(rowCh chan []string, dbPool *pgxpool.Pool, maxWorkerNum int) {
	var wg sync.WaitGroup
	for i := range maxWorkerNum {
		wg.Add(1)
		go func() {
			defer wg.Done()
			insertRow(rowCh, dbPool, i)
		}()
	}
	wg.Wait()
}

func insertRow(rowCh chan []string, dbPool *pgxpool.Pool, workerNum int) {
	jobCount := 0

	for c := range rowCh {
		_, err := dbPool.Exec(context.Background(),
			"insert into chess (id, rated, created_at, last_move_at, turns, worker_num) values ($1, $2, $3, $4, $5, $6)",
			c[0], c[1], c[2], c[3], c[4], workerNum)
		if err != nil {
			fmt.Println(err)
			continue
		}

		jobCount++
		logWorker(jobCount, workerNum)

		time.Sleep(200 * time.Millisecond)
	}
}

func logWorker(jobCount int, workerNum int) {
	if jobCount%10 == 0 {
		fmt.Printf("worker %v has inserted %v rows\n", workerNum, jobCount)
	}
}

func main() {
	csvReader, file, err := openCsv("games.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	rowCh := make(chan []string)
	go readCsv(rowCh, csvReader)

	dbPool, err := newDbPool("postgres://marybeth:marybeth@localhost:5432/marybeth?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	distributeWorker(rowCh, dbPool, 50)
}
