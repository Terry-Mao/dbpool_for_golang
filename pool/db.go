package pool

import (
	"container/list"
	"database/sql"
	"errors"
	"ijinshan.com/container/set"
	"log"
	"os"
	"sync"
	"time"
)

const (
	MIN_SIZE         = 2   // pool min connectins
	MAX_SIZE         = 5   // pool max connections
	KEEP_SIZE        = 2   // pool free connections
	INIT_SIZE        = 2   // pool init connections
	INCR_SIZE        = 2   // pool increment connections when needed
	MAX_FREE_TIME    = 600 // max connection free time
	FREE_TEST_PERIOD = 60   // check idel connections routines period
	OPEN_RETRY_COUNT = 30  // max retry acquire connection from pool
	OPEN_RETRY_DELAY = 1000
	CLOSE_ON_FAILURE = false // if connecion failed ,close or back to pool
	TEST_ON_OPEN     = false // get a connection from pool, check valid or not
	TEST_SQL_STMT    = "SELECT @@VERSION"
)

type Data struct {
	freeCheck bool
	expired   bool
	valid     bool
	hold      bool
	closeTime int64
	openTime  int64
}

type DBPool struct {
	Driver         string
	Url            string
	TestSqlStmt    string
	MinSize        int
	MaxSize        int
	KeepSize       int
	InitSize       int
	IncrSize       int
	MaxFreeTime    int
	FreeTestPeriod int
	OpenRetryCount int
	OpenRetryDelay int
	CloseOnFailure bool
	TestOnOpen     bool

	resizeFlag bool

	freeConn *list.List
	usedConn *set.Set
	connData map[*sql.DB]*Data
	mutex    *sync.Mutex
	cond     *sync.Cond
	log      *log.Logger
}

func setDefInt(obj *int, val int) {
	if *obj == 0 {
		*obj = val
	}
}

func setDefStr(obj *string, val string) {
	if *obj == "" {
		*obj = val
	}
}

func (pool *DBPool) newConn() (*sql.DB, error) {

	for i := 0; i < pool.OpenRetryCount; i++ {

		conn, err := sql.Open(pool.Driver, pool.Url)

		if err != nil {
			time.Sleep(time.Duration(pool.OpenRetryDelay) * time.Millisecond)
		}

		return conn, nil
	}

	return nil, errors.New("Can't create connection")
}

// must sync
func (pool *DBPool) pushFree(conn *sql.DB) {
	now := time.Now().Unix()

	if cd, ok := pool.connData[conn]; !ok {
		pool.connData[conn] = &Data{hold: false, freeCheck: false, valid: true,
			expired: false, openTime: 0,
			closeTime: now}
	} else {
		cd.closeTime = now
		cd.openTime = 0
	}

	pool.freeConn.PushBack(conn)
}

func (pool *DBPool) CurSize() int {
	return pool.usedConn.Len() + pool.freeConn.Len()
}

func (pool *DBPool) FreeSize() int {
	return pool.freeConn.Len()
}

func (pool *DBPool) UsedSize() int {
	return pool.usedConn.Len()
}

// Ensure pool.minSize and pool.keepSize 
func (pool *DBPool) resize() {

	if pool.resizeFlag || pool.MinSize <= pool.CurSize() &&
		pool.KeepSize <= pool.FreeSize() {
		return
	}

	pool.resizeFlag = true

	go pool._resize()
}

func (pool *DBPool) _resize() {
	var incrSize, tmpSize int

	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	if incrSize = pool.MinSize - pool.CurSize(); pool.KeepSize > incrSize {
		incrSize = pool.KeepSize
	}

	if tmpSize = pool.CurSize() + incrSize; pool.MaxSize < tmpSize {
		incrSize = pool.MaxSize - pool.CurSize()
	}

	for i := 0; i < incrSize; i++ {
		conn, err := pool.newConn()

		if err != nil {
			// log
		}

		pool.pushFree(conn)
	}

	pool.resizeFlag = false
}

func (pool *DBPool) testSQL(conn *sql.DB) bool {
	var tmp string
	err := conn.QueryRow(pool.TestSqlStmt).Scan(&tmp)
	return err == nil
}

func (pool *DBPool) freeCheck() {
	for true {
		time.Sleep(time.Duration(pool.FreeTestPeriod) * time.Second)
		pool.mutex.Lock()
		for d := pool.freeConn.Front(); d != nil; d = d.Next() {

			conn, ok := d.Value.(*sql.DB)

			if !ok {
				pool.mutex.Unlock()
				panic("freeConn element type assection failed")
			}

			pool.connData[conn].freeCheck = true
		}

		pool.mutex.Unlock()

		for d := pool.freeConn.Front(); d != nil; d = d.Next() {

			conn, ok := d.Value.(*sql.DB)

			if !ok {
				panic("freeConn element type assection failed")
			}

			cd := pool.connData[conn]

			cd.expired = int(time.Now().Unix()-cd.closeTime) >
				pool.MaxFreeTime

			// test SQL
			cd.valid = pool.testSQL(conn)

			pool.mutex.Lock()
			cd.freeCheck = false
			if cd.expired || !cd.valid {
				pool.freeConn.Remove(d)
				err := conn.Close()
				if err != nil {
					// log
				}
				pool.resize()
			}

			if cd.hold {
				pool.cond.Signal()
			}

			pool.mutex.Unlock()
		}
	}
}

func (pool *DBPool) Init() error {

	if pool.Driver == "" || pool.Url == "" {
		return errors.New("Driver or Url must be set")
	}

	if pool.log == nil {
		// if no user define logger, user stdout as default
		pool.log = log.New(os.Stdout, "", 0)
	}

	setDefInt(&pool.MinSize, MIN_SIZE)
	setDefInt(&pool.MaxSize, MAX_SIZE)
	setDefInt(&pool.KeepSize, KEEP_SIZE)
	setDefInt(&pool.InitSize, INIT_SIZE)
	setDefInt(&pool.IncrSize, INCR_SIZE)
	setDefInt(&pool.MaxFreeTime, MAX_FREE_TIME)
	setDefInt(&pool.FreeTestPeriod, FREE_TEST_PERIOD)
	setDefInt(&pool.OpenRetryDelay, OPEN_RETRY_DELAY)
	setDefInt(&pool.OpenRetryCount, OPEN_RETRY_COUNT)
	setDefStr(&pool.TestSqlStmt, TEST_SQL_STMT)

	if pool.MinSize > pool.MaxSize {
		pool.MinSize = pool.MaxSize
	}

	if pool.KeepSize > pool.MaxSize {
		pool.KeepSize = pool.MaxSize
	}

	if pool.MinSize > pool.InitSize {
		pool.InitSize = pool.MinSize
	}

	pool.connData = make(map[*sql.DB]*Data, pool.MaxSize)
	pool.freeConn = list.New()
	pool.usedConn = set.New()
	pool.mutex = &sync.Mutex{}
	pool.cond = sync.NewCond(pool.mutex)

	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	for i := 0; i < pool.InitSize; i++ {
		if conn, err := pool.newConn(); err != nil {
			return err
		} else {
			pool.pushFree(conn)
		}
	}

	go pool.freeCheck()
	return nil
}

func (pool *DBPool) Open() (*sql.DB, error) {
	var (
		e    *list.Element
		conn *sql.DB
		err  error
		cd   *Data
		ok   bool
	)

	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	if pool.FreeSize() > 0 {

		for e = pool.freeConn.Front(); e != nil; e = e.Next() {
			conn, ok = e.Value.(*sql.DB)
			if !ok {
				panic("freeConn element type assection failed")
			}

			cd, ok = pool.connData[conn]
			if ok && !cd.hold {
				break
			}
		}

		if e == nil {
			time.Sleep(time.Duration(pool.OpenRetryDelay) * time.Millisecond)
			return pool.Open()
		}

		// free check
		for cd.freeCheck {
			cd.hold = true
			pool.cond.Wait()
			cd.hold = false
		}

		if !cd.valid || cd.expired {
			delete(pool.connData, conn)
			return pool.Open()
		}

		cd.openTime = time.Now().Unix()
		cd.closeTime = 0
		pool.freeConn.Remove(e)
		pool.usedConn.Add(conn)

		pool.resize()
		return conn, nil
	}

	// if no free connection, create a new one 
	// for giving quickly, async ensure keep_size and min_size

	if pool.UsedSize() >= pool.MaxSize {
		return nil, errors.New("No more connections, exceed max pool size")
	}

	conn, err = pool.newConn()
	if err != nil {
		return nil, err
	}

	pool.connData[conn] = &Data{hold: false, freeCheck: false, valid: true,
		expired: false, openTime: time.Now().Unix(), closeTime: 0}
	pool.usedConn.Add(conn)

	pool.resize()
	return conn, nil
}

func (pool *DBPool) Close(conn *sql.DB) error {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	pool.usedConn.Remove(conn)
	pool.pushFree(conn)
	return nil
}

func (pool *DBPool) Free() error {
	// TODO
	return nil
}
