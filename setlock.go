// Package setlock implements a very simple locking strategy
// described at https://redis.io/commands/set.
package setlock

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Lock is a simple SET+GET/DEL Redis lock.
// The lock is safe for concurrent use.
type Lock struct {
	// Key is the Redis key used.
	Key string

	// Pool is the Redis connection pool used.
	Pool *redis.Pool

	mu    sync.Mutex // protects operations and token
	token []byte
}

// ErrAlreadyHeld is returned if the lock was already held.
var ErrAlreadyHeld = errors.New("already held")

// ErrNotHeld is returned if the lock was not held.
var ErrNotHeld = errors.New("not held")

var lockScript = redis.NewScript(1, `
	if redis.call("get", KEYS[1]) == ARGV[1]
	then
		return {"noop", redis.call("pttl", KEYS[1])}
	end
	if redis.call("set", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) == false
	then
		return {"pttl", redis.call("pttl", KEYS[1])}
	else
		return {"lock", 0}
	end`)

// Lock tries to lock for a specified duration d.
// If the returned duration is != 0 and the returned
// error is ErrNotHeld, this signals that the caller
// is not holding the lock.
// If ErrAlreadyHeld is returned with a non 0 duration,
// this signals that the caller is already holding the
// lock and for the returned duration left.
//
// If duration d is less than a millisecond the
// lock will be held for a millisecond.
func (l *Lock) Lock(ctx context.Context, d time.Duration) (time.Duration, error) {
	if d < time.Millisecond {
		d = time.Millisecond
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	conn, err := l.Pool.GetContext(ctx)
	if err != nil {
		return 0, err
	}
	if l.token == nil {
		l.token = randomToken()
	}

	return parseLockReply(lockScript.Do(conn, l.Key, l.token, dtoms(d)))
}

func parseLockReply(reply interface{}, err error) (time.Duration, error) {
	result, err := redis.Int64Map(reply, err)
	if err != nil {
		return 0, err
	}
	for typ, ms := range result {
		switch typ {
		case "lock":
			return 0, nil
		case "noop":
			return mstod(ms), ErrAlreadyHeld
		case "pttl":
			return mstod(ms), ErrNotHeld
		}
	}
	panic("lock script inconsistency")
}

func dtoms(d time.Duration) int64 {
	return int64(d / time.Millisecond)
}

func mstod(ms int64) time.Duration {
	return time.Duration(ms) * time.Millisecond
}

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

func randomToken() []byte {
	const ntoken = 20
	token := make([]byte, ntoken)
	rng.Read(token)
	return token
}

var unlockScript = redis.NewScript(1, `
	if redis.call("get", KEYS[1]) == ARGV[1]
	then
		return redis.call("del", KEYS[1])
	else
		return 0
	end`)

// Unlock unlocks lock l.
func (l *Lock) Unlock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	conn, err := l.Pool.GetContext(ctx)
	if err != nil {
		return err
	}

	if l.token == nil {
		l.token = randomToken()
	}
	reply, err := redis.Int(unlockScript.Do(conn, l.Key, l.token))
	if err != nil {
		return err
	}

	if reply != 1 {
		return ErrNotHeld
	}
	l.token = nil
	return nil
}
