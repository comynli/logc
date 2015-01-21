package main

import (
	"encoding/json"
	"github.com/ActiveState/tail"
	"github.com/spaolacci/murmur3"
	tomb "gopkg.in/tomb.v2"
	"io"
	"os"
	"path"
	"strconv"
	"time"
)

type LogItem struct {
	Path    string   `json:"path"`
	Tags    []string `json:"path"`
	Source  string   `json:"source"`
	App     string   `json:"app"`
	Content string   `json:"Content"`
}

type Tailer struct {
	path    string
	queue   chan []byte
	tags    []string
	tailer  *tail.Tail
	db      *os.File
	file    *os.File //use for check file size
	isStart bool
	tomb.Tomb
}

func NewTailer(logPath string, queue chan []byte, tags []string) (*Tailer, error) {
	f, err := os.OpenFile(path.Join(conf.StatusDB, Format("%x", murmur3.Sum32([]byte(logPath)))), os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil, err
	}
	t := &Tailer{
		path:    logPath,
		queue:   queue,
		tags:    tags,
		db:      f,
		isStart: false}
	t.Go(t.loop)
	return t, nil
}

func (t *Tailer) loop() error {
	defer t.cleanup()

	//test the file is can open
	file, err := os.Open(t.path)
	for err != nil {
		select {
		case <-time.After(time.Duration(3) * time.Second):
			file, err = os.Open(t.path)
		case <-t.Dying():
			return nil
		}
		logger.Warning(Format("open %s fail %v\n", t.path, err))
	}

	t.file = file
	//get last read offset
	offset, err := t.getPos()
	if err != nil {
		logger.Warning(Format("get seek info of %s fail %v, will use end of file\n", t.path, err))
	}
	stat, err := file.Stat()
	if err != nil {
		logger.Err(Format("get %s stat fail %v\n", t.path, err))
	}
	// current file length less than last read offset, it is means, this is a new file, read from begin
	if stat.Size() < offset {
		offset = 0
	}

	// wrong offset
	if offset < 0 {
		offset = 0
	}

	// init tailer object
	tailer, err := tail.TailFile(t.path,
		tail.Config{
			Follow:   true,
			Location: &tail.SeekInfo{Offset: offset},
			ReOpen:   true,
			Poll:     false})
	if err != nil {
		logger.Err(Format("init tailer fail %v\n", err))
		return err
	}

	t.isStart = true
	t.tailer = tailer

	// main loop
	for {
		pos, err := t.tailer.Tell()
		select {
		case <-t.Dying():
			if pos != offset {
				offset, err = t.setPos(pos)
			}
			return nil
		case <-time.After(time.Duration(3) * time.Second):
			//persistent offset info pre 3 second
			if pos != offset {
				offset, err = t.setPos(pos)
			}
		case line := <-t.tailer.Lines:
			t.fire(line.Text)
			if err != nil {
				//get read offset fail, skip
				logger.Warning(Format("get current pos of %s fail: %v\n", t.path, err))
			} else {
				// check pending size
				stat, err := t.file.Stat()
				if err != nil {
					//get file stat fail, skip
					logger.Warning(Format("get size of %s fail: %v\n", t.path, err))
					continue
				}

				if stat.Name() != t.tailer.Filename {
					// means tailer read a new file, but the stater is still old file, reset it
					file, err := os.Open(t.path)
					if err != nil {
						continue
					}
					t.file.Close()
					t.file = file
					continue
				}
				size := stat.Size()
				if (size - pos) >= conf.MaxPendingSize*1024*1024 {
					logger.Warning(Format("%s overhead limit pending size, current pos is %d, size is %d\n", t.path, pos, size))
					//go Alert(t.path, pos, size)
				}
			}
		}
	}
}

//func (t *Tailer) init() error {
//	file, err := os.Open(t.path)
//	for err != nil {
//		select {
//		case <-time.After(time.Duration(3) * time.Second):
//			file, err = tail.OpenFile(t.path)
//		case <-t.Dying():
//			return nil
//		}
//		logger.Warning(Format("open %s fail %v\n", t.path, err))
//	}
//	t.file = file
//	seek, err := t.getPos()
//	if err != nil {
//		logger.Warning(Format("get seek info of %s fail %v, will use end of file\n", t.path, err))
//	}
//
//	stat, err := file.Stat()
//	if err != nil {
//		logger.Err(Format("get %s stat fail %v\n", t.path, err))
//		return err
//	}
//
//	if stat.Size() < seek {
//		seek = 0
//	}
//
//	if seek < 0 {
//		seek = stat.Size()
//	}
//
//	tailer, err := tail.TailFile(t.path,
//		tail.Config{
//			Follow:   true,
//			Location: &tail.SeekInfo{Offset: seek},
//			ReOpen:   true,
//			Poll:     false})
//
//	if err != nil {
//		logger.Err(Format("init tailer fail %v\n", err))
//		return err
//	}
//	t.isStart = true
//	t.tailer = tailer
//	return nil
//}

func (t *Tailer) Stop() error {
	t.Kill(nil)
	return t.Wait()
}

//func (t *Tailer) Start() {
//	defer t.cleanup()
//
//	var seek int64
//	err := t.init()
//	if err != nil {
//		logger.Err(Format("init tailer %s fail %v\n", t.path, err))
//		return
//	}
//
//	for {
//		if !t.isStart {
//			select {
//			case <-t.Dying():
//				return
//			default:
//				<-time.After(time.Duration(1) * time.Second)
//				continue
//			}
//		}
//		pos, err := t.tailer.Tell()
//		select {
//		case <-t.Dying():
//			if pos != seek {
//				seek, err = t.setPos(pos)
//			}
//			return
//		case <-time.After(time.Duration(3) * time.Second):
//			if pos != seek {
//				seek, err = t.setPos(pos)
//			}
//		case line := <-t.tailer.Lines:
//			t.fire(line.Text)
//			if err != nil {
//				logger.Warning(Format("get current pos of %s fail: %v\n", t.path, err))
//			} else {
//				stat, err := t.file.Stat()
//
//				if err != nil {
//					logger.Warning(Format("get size of %s fail: %v\n", t.path, err))
//					continue
//				}
//
//				if stat.Name() != t.tailer.Filename {
//					file, err := os.Open(t.path)
//					if err != nil {
//						continue
//					}
//					t.file.Close()
//					t.file = file
//					continue
//				}
//				size := stat.Size()
//				if (size - pos) >= conf.MaxPendingSize*1024*1024 {
//					logger.Warning(Format("%s overhead limit pending size, current pos is %d, size is %d\n", t.path, pos, size))
//					go Alert(t.path, pos, size)
//				}
//			}
//		}
//	}
//}

func (t *Tailer) fire(line string) {
	logItem := LogItem{Path: t.path, Tags: t.tags, App: conf.App, Source: conf.Source, Content: line}
	encodedLog, err := json.Marshal(logItem)
	if err != nil {
		logger.Err(Format("encode log fail %s", err))
	}
	t.queue <- encodedLog
}

func (t *Tailer) getPos() (int64, error) {
	buf := make([]byte, 1024)
	t.db.Seek(0, 0)
	n, err := t.db.Read(buf)
	if err != nil && err != io.EOF {
		return -1, err
	}
	pos, err := strconv.ParseInt(string(buf[:n]), 0, 64)
	if err != nil {
		return -1, err
	}
	return pos, nil
}

func (t *Tailer) setPos(pos int64) (int64, error) {
	buf := Format("%d", pos)
	logger.Info(Format("set pos of %s to %d\n", t.path, pos))
	t.db.Truncate(0)
	t.db.Seek(0, 0)
	_, err := t.db.Write([]byte(buf))
	if err != nil {
		return -1, err
	}
	return pos, nil
}

func (t *Tailer) cleanup() {
	t.db.Close()
	t.file.Close()
	t.tailer.Stop()
}
