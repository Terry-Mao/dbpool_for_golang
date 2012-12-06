package set

import (
    "fmt"
    "testing"
)


func TestSet(t *testing.T) {
    s := New()
    s.Add("123")
    s.Add("312")

    if !s.Contains("123") {
        t.Errorf("s.Contains(\"%s\") failed", "123")
    }

    fmt.Println(s.Contains("123"))
}
