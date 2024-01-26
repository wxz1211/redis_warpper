package redis_warpper

import (
	"testing"

	"github.com/go-redis/redis/v8"
)

type Foo struct {
	Name string `json:"name"`
	Sex  int    `json:"sex"`
}

func TestList(t *testing.T) {
	r := redis.NewClient(&redis.Options{
		Addr: "172.16.3.101:6379",
		DB:   2,
	})
	l := NewSingleList(r, "bhi:user:column:414389321883354279")
	rr, err := l.BatchContains("2", "444", "555")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("exist %v", rr)
}

func TestHash(t *testing.T) {
	r := redis.NewClient(&redis.Options{
		Addr: "172.16.3.99:6399",
		DB:   0,
	})
	h := NewSingleHash(r, "account")
	h.Set("0", &Foo{
		Name: "lihao",
		Sex:  1,
	})
	h.Set("1", &Foo{
		Name: "lihao",
		Sex:  1,
	})

	h.Del(&Foo{}, "0")
}
