package main

import (
	"log"
	"log/syslog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

var (
	conf   Conf
	logger *syslog.Writer
)

func init() {
	var (
		dir string
		err error
	)
	logger, err = syslog.New(syslog.LOG_DEBUG|syslog.LOG_LOCAL3, "logc")
	if err != nil {
		log.Fatalf("get logger fail %v\n", err)
	}
	dir, err = filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		logger.Crit(Format("get work dir fail %v\n", err))
	}
	os.Chdir(dir)
	conf, err = InitConf(filepath.Join(dir, "logc.yml"))
	if err != nil {
		logger.Crit(Format("parse config file %s fail %v\n", filepath.Join(dir, "logc.yml"), err))
	}
}

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)

	collector, err := NewCollector()
	if err != nil {
		logger.Crit(Format("init collector fail %v\n", err))
		os.Exit(1)
	}
	<-c
	collector.Stop()
	logger.Close()
	os.Exit(0)
}
