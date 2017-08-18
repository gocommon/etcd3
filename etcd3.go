// Package etcd3 etcdv3
// 1、InitEtcdv3 in main()
// 2 、use
// 	s := Session()
// 	defer s.Close()
//
// 	l := s.NewLocker()
// 	l.Lock()
// 	l.Ulock()
package etcd3

import (
	"fmt"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

// Config Config
type Config struct {
	// Endpoints is a list of URLs.
	Endpoints []string `json:"endpoints"`

	// AutoSyncInterval is the interval to update endpoints with its latest members.
	// 0 disables auto-sync. By default auto-sync is disabled.
	AutoSyncInterval int `json:"auto-sync-interval"`

	// DialTimeout is the timeout for failing to establish a connection.
	DialTimeout int `json:"dial-timeout"`

	// DialKeepAliveTime is the time in seconds after which client pings the server to see if
	// transport is alive.
	DialKeepAliveTime int `json:"dial-keep-alive-time"`

	// DialKeepAliveTimeout is the time in seconds that the client waits for a response for the
	// keep-alive probe.  If the response is not received in this time, the connection is closed.
	DialKeepAliveTimeout int `json:"dial-keep-alive-timeout"`

	// Username is a username for authentication.
	Username string `json:"username"`

	// Password is a password for authentication.
	Password string `json:"password"`

	// RejectOldCluster when set will refuse to create a client against an outdated cluster.
	RejectOldCluster bool `json:"reject-old-cluster"`
}

// ToEtcdv3Config ToEtcdv3Config
func ToEtcdv3Config(conf Config) clientv3.Config {
	c := clientv3.Config{}
	c.Endpoints = conf.Endpoints
	c.AutoSyncInterval = time.Duration(conf.AutoSyncInterval) * time.Second
	c.DialTimeout = time.Duration(conf.DialTimeout) * time.Second
	c.DialKeepAliveTime = time.Duration(conf.DialKeepAliveTime) * time.Second
	c.DialKeepAliveTimeout = time.Duration(conf.DialKeepAliveTimeout) * time.Second
	c.Username = conf.Username
	c.Password = conf.Password
	c.RejectOldCluster = conf.RejectOldCluster
	return c
}

// EtcdClients EtcdClients
var EtcdClients = map[string]*clientv3.Client{}

// InitEtcdv3 InitEtcdv3
func InitEtcdv3(confs map[string]Config) error {

	for name := range confs {
		cli, err := clientv3.New(ToEtcdv3Config(confs[name]))
		if err != nil {
			return err
		}
		EtcdClients[name] = cli
	}

	return nil
}

// Etcd Etcd
func Etcd(name ...string) *clientv3.Client {
	cname := "default"
	if len(name) > 0 {
		cname = name[0]
	}

	c, has := EtcdClients[cname]
	if !has {
		panic(fmt.Errorf("etcd not found: %s", cname))
	}

	return c
}

// Session Session
func Session(name ...string) (*Sessiond, error) {
	s, err := concurrency.NewSession(Etcd(name...))
	if err != nil {
		return nil, err
	}

	return &Sessiond{s}, nil
}

// Sessiond Sessiond
type Sessiond struct {
	sess *concurrency.Session
}

// Sess Sess
func (s *Sessiond) Sess() *concurrency.Session {
	return s.sess
}

// NewLocker NewLocker
func (s *Sessiond) NewLocker(key string) sync.Locker {
	return concurrency.NewLocker(s.sess, key)
}

// NewMutex NewMutex
func (s *Sessiond) NewMutex(key string) *concurrency.Mutex {
	return concurrency.NewMutex(s.sess, key)
}

// NewElection NewElection
func (s *Sessiond) NewElection(key string) *concurrency.Election {
	return concurrency.NewElection(s.sess, key)
}

// ResumeElection initializes an election with a known leader.
func (s *Sessiond) ResumeElection(pfx string, leaderKey string, leaderRev int64) *concurrency.Election {
	return concurrency.ResumeElection(s.sess, pfx, leaderKey, leaderRev)
}
