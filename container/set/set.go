package set

type Set struct {
    data map[interface{}] bool
}

func (s *Set) Add(e interface{}) {
    s.data[e] = true
}

func (s *Set) Contains(e interface{}) (exists bool) {
    _, exists = s.data[e]
    return
}

func (s *Set) Len() int {
    return len(s.data)
}

func New() *Set {
    return &Set{make(map[interface{}] bool)}
}

func (s *Set) Remove(e interface{}) {
    delete(s.data, e)
}
