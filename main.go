package main

import (
	"fmt"
	"github.com/henrylee2cn/goutil/graceful"
	"net/http"
	_ "net/http/pprof"
	"nqs/broker"
	"nqs/common/nlog"
	_ "nqs/common/nlog"
	"time"
)

func main() {

	broker.Initialize()

	server := &http.Server{
		Addr:    ":8023",
		Handler: nil,
	}

	go func() {
		// http://localhost:8023/debug/pprof/goroutine?debug=1
		// index http://localhost:8023/debug/pprof/
		// 启动一个 http server，注意 pprof 相关的 handler 已经自动注册过了
		if err := server.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				return
			}

			fmt.Println(err)
		}
	}()

	// graceful shutdown
	graceful.SetLog(nlog.GetLogger())
	graceful.SetShutdown(5* time.Second, nil, func() error {
		broker.Shutdown()
		_ = server.Close()
		return nil
	})
	graceful.GraceSignal()

}
