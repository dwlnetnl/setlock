package setlock

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

var redisAddr = flag.String("redis.addr", "localhost:6379", "Address for Redis to connect on during tests")

const testingKey = "testing.lock"

func absd(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func TestLock(t *testing.T) {
	p := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", *redisAddr)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	// remove key from redis
	_, err := p.Get().Do("DEL", testingKey)
	if err != nil {
		t.Skip("redis not available")
	}

	ctx := context.Background()
	l := &Lock{Key: testingKey, Pool: p}
	const hold = time.Minute

	begin := time.Now()
	expiry, err := l.Lock(ctx, hold)
	if err != nil {
		t.Fatal(err)
	}
	if expiry != 0 {
		t.Errorf("got expiry %s, want %s", expiry, time.Duration(0))
	}

	curr := hold - time.Since(begin)
	expiry, err = l.Lock(ctx, hold)
	if err != ErrAlreadyHeld {
		t.Fatal(err)
	}
	if absd(expiry-curr) > 5*time.Millisecond {
		t.Errorf("got expiry %s, want %s", expiry, curr)
	}

	// unlock lock l
	_, err = p.Get().Do("DEL", testingKey)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lockScript.Do(p.Get(), testingKey, randomToken(), dtoms(hold))
	if err != nil {
		t.Fatal(err)
	}
	// check lock l is unheld
	expiry, err = l.Lock(ctx, hold)
	if err != ErrNotHeld {
		t.Errorf("got error %q, want %q", err, ErrNotHeld)
	}
	// check lock l is unheld
	err = l.Unlock(ctx)
	if err != ErrNotHeld {
		t.Errorf("got error %q, want %q", err, ErrNotHeld)
	}
	// relock lock l
	_, err = p.Get().Do("DEL", testingKey)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lockScript.Do(p.Get(), testingKey, l.token, dtoms(hold))
	if err != nil {
		t.Fatal(err)
	}

	err = l.Unlock(ctx)
	if err != nil {
		t.Fatal(err)
	}
}
