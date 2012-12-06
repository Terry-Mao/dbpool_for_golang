package pool

import (
	_ "github.com/ziutek/mymysql/godrv"
	"testing"
)

func TestDBPool(t *testing.T) {

	dbPool := &DBPool{Driver: "mymysql", Url: "tcp:127.0.0.1:3306*test/test/test", MaxSize: 100}

	dbPool.Init()

	conn, err := dbPool.Open()

	if err != nil {
		t.Errorf("dbPool.Open(), err : %s", err.Error())
	}

    dbPool.Close(conn)
}

func BenchmarkDBPoolOpenAndClose(b *testing.B) {
	b.StopTimer()
	dbPool := &DBPool{Driver: "mymysql", Url: "tcp:127.0.0.1:3306*test/test/test", MaxSize: 100}
	dbPool.Init()
	b.StartTimer()
	for i := 0; i < 10000000; i++ {
		conn, _ := dbPool.Open()
		dbPool.Close(conn)
	}
}
