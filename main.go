package main

import (
	"context"
	"encoding/csv"
	"log"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func readCsv(path string, maxRows int) ([][]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(rows)-1 <= maxRows {
		return rows[1:], nil
	}
	return rows[1 : maxRows+1], nil
}

func newDbPool(dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

func insertChessRow(rowCh chan chessRow, workerNum int) {
	dbPool, err := newDbPool("postgres://marybeth:marybeth@localhost:5432/marybeth?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	for c := range rowCh {
		c.worker_num = workerNum
		_, err := dbPool.Exec(context.Background(),
			"insert into chess (id, rated, created_at, last_move_at, turns, worker_num) values ($1, $2, $3, $4, $5, $6)",
			c.id, c.rated, c.created_at, c.last_move_at, c.turns, c.worker_num)
		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(200 * time.Millisecond)
	}
}

func distributeWorker(rowCh chan chessRow, maxWorkers int) {
	var wg sync.WaitGroup
	for i := range maxWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			insertChessRow(rowCh, i)
		}()
	}
	wg.Wait()
}

type chessRow struct {
	id           string
	rated        string
	created_at   string
	last_move_at string
	turns        string
	worker_num   int
}

type chess struct {
	data []chessRow
}

func main() {
	chessData := chess{}
	rows, err := readCsv("games.csv", 9)
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range rows {
		chessData.data = append(chessData.data, chessRow{
			id:           row[0],
			rated:        row[1],
			created_at:   row[2],
			last_move_at: row[3],
			turns:        row[4],
		})
	}

	chessRowCh := make(chan chessRow)
	go func() {
		defer close(chessRowCh)
		for _, row := range chessData.data {
			chessRowCh <- row
		}
	}()

	distributeWorker(chessRowCh, 5)
}
