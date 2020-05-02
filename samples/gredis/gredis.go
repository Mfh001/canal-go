package gredis

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

var RedisConn *redis.Pool

// Setup Initialize the Redis instance
func init() {
	RedisConn = &redis.Pool{
		MaxIdle:     500,
		MaxActive:   500,
		IdleTimeout: 200,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "127.0.0.1:6379")
			if err != nil {
				return nil, err
			}
			if _, err := c.Do("AUTH", "HYSH%&@3sd@KK"); err != nil {
				c.Close()
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

// Set a key/value
func Set(key string, data interface{}, time int) error {
	conn := RedisConn.Get()
	defer conn.Close()

	value, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_, err = conn.Do("SET", key, value)
	if err != nil {
		return err
	}

	_, err = conn.Do("EXPIRE", key, time)
	if err != nil {
		return err
	}

	return nil
}

// Exists check a key
func Exists(key string) bool {
	conn := RedisConn.Get()
	defer conn.Close()

	exists, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		return false
	}

	return exists
}

// Get get a key
func Get(key string) ([]byte, error) {
	conn := RedisConn.Get()
	defer conn.Close()

	reply, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		return nil, err
	}

	return reply, nil
}

func HMGet(key string, fields ...string) ([]interface{}, error) {
	conn := RedisConn.Get()
	defer conn.Close()

	reply, err := redis.Values(conn.Do("HMGET", key, fields))
	if err != nil {
		return nil, err
	}

	return reply, nil
}

func HMSet(key string, fields map[string]string) (bool, error) {
	conn := RedisConn.Get()
	defer conn.Close()

	res, err := redis.Bool(conn.Do("HMSET", key, fields))
	if err != nil {
		fmt.Println(err.Error())
		return false, err
	}

	return res, nil
}

// Delete delete a kye
func Delete(key string) (bool, error) {
	conn := RedisConn.Get()
	defer conn.Close()

	return redis.Bool(conn.Do("DEL", key))
}

// LikeDeletes batch delete
func LikeDeletes(key string) error {
	conn := RedisConn.Get()
	defer conn.Close()

	keys, err := redis.Strings(conn.Do("KEYS", "*"+key+"*"))
	if err != nil {
		return err
	}

	for _, key := range keys {
		_, err = Delete(key)
		if err != nil {
			return err
		}
	}

	return nil
}

// Incr a key
func Incr(key string) (int, error) {
	conn := RedisConn.Get()
	defer conn.Close()

	incr, err := redis.Int(conn.Do("INCR", key))
	if err != nil {
		return incr, err
	}
	if incr == 0 {
		incr, err = redis.Int(conn.Do("INCR", key))
		if err != nil {
			return incr, err
		}
	}
	return incr, nil
}
