package setlock

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

var redisURL = flag.String("redis.url", "redis://localhost:6379/3", "URL for Redis to connect on during tests")

const testingKey = "testing.lock"

func absd(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func parseDuration(reply interface{}, err error) (time.Duration, error) {
	ms, err := redis.Int64(reply, err)
	if err != nil {
		return 0, err
	}
	d := time.Duration(mstod(ms))
	return d, nil
}

func resetRedis(t *testing.T) *redis.Pool {
	p := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.DialURL(*redisURL)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	_, err := p.Get().Do("DEL", testingKey)
	if err != nil {
		t.Skip("redis not available")
	}
	return p
}

func TestLock(t *testing.T) {
	// remove lock key from Redis
	p := resetRedis(t)

	ctx := context.Background()
	l := &Lock{Key: testingKey, Pool: p}
	const hold = time.Minute

	// acquire lock l
	begin := time.Now()
	expiry, err := l.Lock(ctx, hold)
	if err != nil {
		t.Fatal(err)
	}
	if expiry != 0 {
		t.Errorf("got expiry %s, want %s", expiry, time.Duration(0))
	}

	// check returned lock expiry
	curr := hold - time.Since(begin)
	expiry, err = l.Lock(ctx, hold)
	if err != ErrAlreadyHeld {
		t.Fatal(err)
	}
	if absd(expiry-curr) > 10*time.Millisecond {
		t.Errorf("got expiry %s, want %s", expiry, curr)
	}

	// extend lock l
	err = l.Extend(ctx, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	conn := p.Get()
	expiry, err = parseDuration(conn.Do("PTTL", testingKey))
	if err != nil {
		t.Fatal(err)
	}
	const threshold = time.Minute + 10*time.Second
	if expiry < threshold {
		t.Errorf("got expiry %s, want %s", expiry, threshold)
	}

	// unlock lock l
	_, err = p.Get().Do("DEL", testingKey)
	if err != nil {
		t.Fatal(err)
	}

	// lock with random token to simulate
	// different instance holding the lock
	_, err = lockScript.Do(p.Get(), testingKey, randomToken(), dtoms(hold))
	if err != nil {
		t.Fatal(err)
	}
	// check lock l is unheld
	_, err = l.Lock(ctx, hold)
	if err != ErrNotHeld {
		t.Errorf("got error %q, want %q", err, ErrNotHeld)
	}
	// check lock l is unheld
	err = l.Extend(ctx, time.Minute)
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

	// finally release lock
	err = l.Unlock(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDo(t *testing.T) {
	// remove lock key from Redis
	p := resetRedis(t)

	ctx := context.Background()
	l := &Lock{Key: testingKey, Pool: p}
	f := func(ctx context.Context) error {
		time.Sleep(time.Second / 2)
		return nil
	}

	// execute f while holding lock l
	err := l.do(ctx, f, (time.Second / 3))
	if err != nil {
		t.Fatal(err)
	}

	// check lock l is unheld
	err = l.Unlock(ctx)
	if err != ErrNotHeld {
		t.Errorf("got error %q, want %q", err, ErrNotHeld)
	}
}

func TestDo_ExtendError(t *testing.T) {
	// remove lock key from Redis
	p := resetRedis(t)

	ctx := context.Background()
	l := &Lock{Key: testingKey, Pool: p}
	f := func(ctx context.Context) error {
		l.token = randomToken()
		time.Sleep(time.Second)
		return nil
	}

	// execute f while holding lock l
	err := l.do(ctx, f, (time.Second / 2))
	if err != nil {
		t.Fatal(err)
	}

	// check lock l is unheld
	err = l.Unlock(ctx)
	if err != ErrNotHeld {
		t.Errorf("got error %q, want %q", err, ErrNotHeld)
	}
}

func TestDo_Panic(t *testing.T) {
	// remove lock key from Redis
	p := resetRedis(t)

	ctx := context.Background()
	l := &Lock{Key: testingKey, Pool: p}
	f := func(ctx context.Context) error {
		time.Sleep(time.Second)
		panic("panic happend")
	}

	// execute f while holding lock l
	err := l.do(ctx, f, (time.Second / 2))
	if err != nil {
		t.Fatal(err)
	}

	// check lock l is unheld
	err = l.Unlock(ctx)
	if err != ErrNotHeld {
		t.Errorf("got error %q, want %q", err, ErrNotHeld)
	}
}
