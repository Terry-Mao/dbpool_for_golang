package pool

import (
	"database/sql"
)

type DBPool1 struct {
    Pool chan *sql.DB
    MaxSize int
}

func (pool *DBPool1) Init(maxSize int, driver string, url string) {
    pool.MaxSize = maxSize
    pool.Pool = make(chan *sql.DB, maxSize)

    go func() {
        for i := 0; i < maxSize; i++ {
            db, err := sql.Open(driver, url)
            if err == nil {
                pool.Pool <- db
            }
        }
    }()
}

func (pool *DBPool1) Open() *sql.DB {
    return <-pool.Pool
}

func (pool *DBPool1) Close(conn *sql.DB) {
    if conn != nil {
        pool.Pool <- conn
    }
}
