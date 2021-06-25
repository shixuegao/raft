package config

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Config struct {
	MaxEventSize      int      `yaml:"maxEventSize"`
	MaxEntrySize      int      `yaml:"maxEntrySize"`
	MaxHandleSize     int      `yaml:"maxHandleSize"`
	LogAppendSize     int      `yaml:"logAppendSize"`
	ReadTimeout       int      `yaml:"readTimeout"`       //ms
	HeartbeatInterval int      `yaml:"heartbeatInterval"` //ms
	VoteInterval      int      `yaml:"voteInterval"`      //ms
	Local             Server   `yaml:"local"`
	Cluster           []Server `yaml:"cluster"`
}

type Server struct {
	ID   uint64 `yaml:"id"`
	Addr string `yaml:"addr"`
}

func (s Server) Validate() error {
	if s.ID <= 0 {
		return fmt.Errorf("非法的服务ID[%v]", s.ID)
	}
	if s.Addr == "" {
		return fmt.Errorf("服务[%v]的地址不能为空", s.ID)
	}
	return nil
}

func (s *Config) Validate() error {
	if s.MaxEventSize <= 0 {
		return fmt.Errorf("非法的事件队列上限值[%v]", s.MaxEventSize)
	}
	if s.MaxEntrySize <= 0 {
		return fmt.Errorf("非法的实体上限值[%v]", s.MaxEventSize)
	}
	if s.MaxHandleSize <= 0 {
		return fmt.Errorf("非法的日志处理上限值[%v]", s.MaxHandleSize)
	}
	if s.LogAppendSize <= 0 {
		return fmt.Errorf("非法的日志存储容量扩充值[%v]", s.LogAppendSize)
	}
	if s.ReadTimeout <= 0 {
		return fmt.Errorf("非法的读取超时时间[%v]", s.ReadTimeout)
	}
	if s.HeartbeatInterval <= 0 {
		return fmt.Errorf("非法的心跳周期[%v]", s.HeartbeatInterval)
	}
	if s.VoteInterval <= 0 {
		return fmt.Errorf("非法的投票周期[%v]", s.VoteInterval)
	}
	if err := s.Local.Validate(); err != nil {
		return err
	}
	ids := map[uint64]byte{s.Local.ID: 1}
	for _, v := range s.Cluster {
		if err := v.Validate(); err != nil {
			return err
		}
		if _, ok := ids[v.ID]; ok {
			return fmt.Errorf("服务ID[%v]重复", v.ID)
		}
		ids[v.ID] = 1
	}
	return nil
}

func NewConfig(filename string) (*Config, error) {
	path := []string{"./", "../", "conf/", "../conf/"}
	for _, v := range path {
		filepath := v + filename
		content, err := ioutil.ReadFile(filepath)
		if err != nil {
			continue
		}
		config := Config{}
		err = yaml.Unmarshal(content, &config)
		if err != nil {
			continue
		}
		return &config, nil
	}
	return nil, fmt.Errorf("%v中找不到配置文件%v", path, filename)
}
