//package main
//
//import (
//	"encoding/json"
//	"net"
//	"time"
//	"math/rand"
//	"strings"
//)
//
//type AlarmMessage struct {
//	ID       string `json:"id"`
//	Key      string `json:"key"`
//	Title    string `json:"title"`
//	Msg      M `json:"message"`
//	Level    int `json:"level"`
//	App      string `json:"app"`
//	Hostname string `json:"hostname"`
//}
//
//type M struct {
//	App     string `json:"app"`
//	IP      string `json:"ip"`
//	Path    string `json:"path"`
//	Pos     int64  `json:"pos"`
//	Size    int64  `json:"size"`
//	Pending int64  `json:"pending"`
//}
//
//func Alert(path string, pos, size int64) {
//	var conn net.Conn
//	msg := M{
//		App: conf.App,
//		IP: conf.IP,
//		Path: path,
//		Pos: pos,
//		Size: size,
//		Pending: size - pos}
//	message := AlarmMessage{
//		ID: conf.AlarmConfig.ID,
//		Key:Format("logc::%s::%s::pending", conf.IP, path),
//		Title: conf.AlarmConfig.Title,
//		Msg: msg,
//		Level: conf.AlarmConfig.Level,
//		App: conf.App,
//		Hostname: conf.IP}
//
//	m, err := json.Marshal(message)
//	if (err != nil) {
//		logger.Warning(Format("send alarm %s fail: %v\n", m, err))
//		return
//	}
//	for i := 0; i < conf.AlarmConfig.Timeout.Retry; i++ {
//
//		if (len(m) >= 1400) {
//			server := conf.AlarmConfig.Servers.TCPServers[rand.Intn(len(conf.AlarmConfig.Servers.TCPServers))]
//			conn, err = net.DialTimeout("tcp", server, time.Duration(conf.AlarmConfig.Timeout.Conn)*time.Second)
//			if (err != nil) {
//				logger.Warning(Format("connect to alarm server<%s> fail: %v\n", server, err))
//				continue
//			}
//		}else {
//			server := conf.AlarmConfig.Servers.UDPServers[rand.Intn(len(conf.AlarmConfig.Servers.UDPServers))]
//			conn, err = net.DialTimeout("udp", server, time.Duration(conf.AlarmConfig.Timeout.Conn)*time.Second)
//			if (err != nil) {
//				logger.Warning(Format("connect to alarm server<%s> fail: %v\n", server, err))
//				continue
//			}
//		}
//
//		conn.SetWriteDeadline(time.Now().Add(time.Duration(conf.AlarmConfig.Timeout.Write) * time.Second))
//		conn.Write(m)
//		conn.SetReadDeadline(time.Now().Add(time.Duration(conf.AlarmConfig.Timeout.Read) * time.Second))
//		recv := make([]byte, 1024)
//		length, err := conn.Read(recv)
//		if (err != nil) {
//			logger.Warning(Format("send message fail: %v\n", err))
//			continue
//		}
//		if (strings.TrimSpace(string(recv[0:length])) == "ok") {
//			conn.Close()
//			return
//		}
//		conn.Close()
//		<-time.After(time.Duration(3) * time.Second)
//	}
//}
