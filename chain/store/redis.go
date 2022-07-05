package store

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	Redis    *RedisClient
	useRedis bool
)

func init() {
	con := os.Getenv("LOTUS_REDIS")
	if con == ""{
		return
	}

	ss := strings.Split(con,",")

	opt :=RedisOption{
		Addr:     ss[0],
		Password: "",
		DB:       0,
	}
	if len(ss)>1{
		opt.Password=ss[1]
	}

	if len(ss)>2 {
		db, err := strconv.Atoi(ss[2])
		if err != nil {
			panic(err)
		}
		opt.DB = db
	}
	Redis = &RedisClient{newRedis(opt)}
	useRedis = true
}

type RedisClient struct {
	*redis.Client
}

func newRedis(opt RedisOption) *redis.Client {
	return redis.NewClient(&redis.Options{
		Network:   opt.Network,
		Addr:      opt.Addr,
		Dialer:    nil,
		OnConnect: nil,
		Username:  opt.UserName,
		Password:  opt.Password,
		DB:        opt.DB,
	})
}

type RedisOption struct {
	// The network type, either tcp or unix.
	// Default is tcp.
	Network string
	// host:port address.
	Addr string
	// Use the specified Username to authenticate the current connection
	// with one of the connections defined in the ACL list when connecting
	// to a Redis 6.0 instance, or greater, that is using the Redis ACL system.
	UserName string
	// Optional password. Must match the password specified in the
	// requirepass server configuration option (if connecting to a Redis 5.0 instance, or lower),
	// or the User Password when connecting to a Redis 6.0 instance, or greater,
	// that is using the Redis ACL system.
	Password string

	// Database to be selected after connecting to the server.
	DB int
}

func (r *RedisClient) GetValue(ctx context.Context, key string, out interface{}) (bool, error) {
	if !useRedis {
		return false, nil
	}
	val, err := r.Get(context.Background(), key).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, err
	}

	if val == "" {
		return false, nil
	}

	err = json.Unmarshal([]byte(val), &out)
	return true, err
}

// SetValue redis `SET key value [expiration]` command.
// Use expiration for `SETEX`-like behavior.
// value为可序列化的值
// Zero expiration means the key has no expiration time.
// KeepTTL(-1) expiration is a redis KEEPTTL option to keep existing TTL.
func (r *RedisClient) SetValue(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	if !useRedis {
		return nil
	}
	val, err := jsoniter.Marshal(value)
	if err != nil {
		return err
	}

	_, err = r.Set(ctx, key, val, expiration).Result()
	return err
}
