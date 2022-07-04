package store

import (
	"encoding/json"
	"errors"
	"github.com/lynkdb/ssdbgo"
	"os"
	"strconv"
	"strings"
)

type Option struct {
	Host string
	Port int
	Auth string
	// The connection timeout to a ssdb server (seconds)
	Timeout int
	// Maximum number of connections
	MaxConn int
}

const (
	ResultOK          = "ok"
	ResultNotFound    = "not_found"
	ResultError       = "error"
	ResultFail        = "fail"
	ResultClientError = "client_error"

)
var (
	ErrNotFound = errors.New(ResultNotFound)
	ssdb *ssdbClient
	use_ssdb bool
)

type ssdbClient struct {
	conn *ssdbgo.Connector
}

func init()  {
	ssdb = &ssdbClient{}
	con := os.Getenv("LOTUS_SSDB")
	if con == ""{
		return
	}

	ss := strings.Split(con,":")
	if len(ss)<2{
		return
	}

	port, err := strconv.Atoi(ss[1])
	if err != nil {
		return
	}

	opt :=Option{
		Host:    ss[0],
		Port:    port,
		Auth:    "",
		Timeout: 20,
		MaxConn: 50,
	}
	ssdb, err = newSsdbClient(opt)
	if err != nil {
		panic(err)
	}

	use_ssdb = true
}

func newSsdbClient(opt Option) (*ssdbClient, error) {
	conn, err := ssdbgo.NewConnector(ssdbgo.Config{
		Host:        opt.Host,
		Port:        uint16(opt.Port),
		Auth:        opt.Auth,
		Timeout:     opt.Timeout,
		MaxConn:     opt.MaxConn,
		CmdRetryNum: 0,
	})

	if err != nil {
		return nil, err
	}

	return &ssdbClient{conn: conn}, nil
}

// HMSet ("myhash", []string{"key1", "value1", "key2", "value2"})
func (s *ssdbClient) HMSet(name string, keyValues []string) error {
	if !use_ssdb{
		return nil
	}
	result := s.conn.Cmd("multi_hset", name, keyValues)
	if result.Status == ResultOK {
		return nil
	}
	if len(result.Items) > 0 {
		return errors.New(string(result.Items[0]))
	}
	return errors.New(result.Status)
}

func (s *ssdbClient) HGet(name, key string) (string, error) {
	if !use_ssdb{
		return "",nil
	}
	result := s.conn.Cmd("hget", name, key)
	if result.Status == ResultOK {
		return result.String(), nil
	}
	if result.Status == ResultNotFound {
		return "", nil
	}
	if len(result.Items) > 0 {
		return "", errors.New(string(result.Items[0]))
	}
	return "", errors.New(result.Status)
}

func (s *ssdbClient) HGetBytes(name, key string) ([]byte, error) {
	if !use_ssdb{
		return nil,nil
	}
	result := s.conn.Cmd("hget", name, key)
	if result.Status == ResultOK {
		return result.Bytes(), nil
	}
	if result.Status == ResultNotFound {
		return nil, nil
	}

	if len(result.Items) > 0 {
		return nil, errors.New(string(result.Items[0]))
	}

	return nil, errors.New(result.Status)
}

func (s *ssdbClient) Set(key, val string) error {
	if !use_ssdb{
		return nil
	}
	result := s.conn.Cmd("set", key, val)
	if result.Status == ResultOK {
		return nil
	}
	if len(result.Items) > 0 {
		return  errors.New(string(result.Items[0]))
	}
	return errors.New(result.Status)
}

func (s *ssdbClient)SetObject(key string, val interface{})error{
	if !use_ssdb{
		return nil
	}
	v, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return s.Set(key,string(v))
}

func (s *ssdbClient) Get(key string) (string, error) {
	if !use_ssdb{
		return "",nil
	}
	result := s.conn.Cmd("get", key)
	if result.Status == ResultOK {
		return result.String(), nil
	}
	if result.Status == ResultNotFound {
		return "", nil
	}
	if len(result.Items) > 0 {
		return "", errors.New(string(result.Items[0]))
	}
	return "", errors.New(result.Status)
}
func (s *ssdbClient)GetValue(key string, out interface{})error{
	if !use_ssdb{
		return nil
	}
	result := s.conn.Cmd("get", key)
	if result.Status == ResultOK {
		err := json.Unmarshal(result.Bytes(), &out)
		if err != nil {
			return err
		}
		return nil
	}
	if result.Status == ResultNotFound {
		return ErrNotFound
	}
	if len(result.Items) > 0 {
		return  errors.New(string(result.Items[0]))
	}
	return  errors.New(result.Status)
}

func (s *ssdbClient) Del(key string) error {
	if !use_ssdb{
		return nil
	}
	result := s.conn.Cmd("del", key)
	if result.Status == ResultOK {
		return nil
	}
	if result.Status == ResultNotFound {
		return nil
	}
	if len(result.Items) > 0 {
		return errors.New(string(result.Items[0]))
	}
	return errors.New(result.Status)
}
