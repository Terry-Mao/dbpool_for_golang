package pool

import (
	_ "github.com/ziutek/mymysql/godrv"
	"testing"
)

func TestDBPool(t *testing.T) {

	dbPool := &DBPool{Driver: "mymysql", Url: "tcp:10.20.216.122:3306*K8_v2/test/test", MaxSize: 100}

	dbPool.Init()

	conn, err := dbPool.Open()

	if err != nil {
		t.Errorf("dbPool.Open(), err : %s", err.Error())
	}

	rows, err := conn.Query("SELECT ID FROM TVS LIMIT 10")

	if err != nil {
		t.Errorf("conn.Query(...), err : %s", err.Error())
	}

	for rows.Next() {
		var id int
		err := rows.Scan(&id)
		if err != nil {
			panic(err)
		}
	}

    dbPool.Close(conn)
}

func BenchmarkDBPoolOpenAndClose(b *testing.B) {
	b.StopTimer()
	dbPool := &DBPool{Driver: "mymysql", Url: "tcp:10.20.216.122:3306*K8_v2/test/test", MaxSize: 100}
	dbPool.Init()
	b.StartTimer()
	for i := 0; i < 10000000; i++ {
		conn, _ := dbPool.Open()
		dbPool.Close(conn)
	}
}

func BenchmarkDBPoolOpenAndClose1(b *testing.B) {
	b.StopTimer()
	dbPool := &DBPool1{}
	dbPool.Init(10, "mymysql", "tcp:10.20.216.122:3306*K8_v2/test/test")
	b.StartTimer()
	for i := 0; i < 10000000; i++ {
		conn := dbPool.Open()
		dbPool.Close(conn)
	}
}
