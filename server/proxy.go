package server

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/grafov/bcast"
	"github.com/hashicorp/raft"
	"github.com/valyala/fasthttp"
	"hhttpp/node"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

var strResponseContinue = []byte("HTTP/1.1 102 Processing\r\n\r\n")

type ChnlMtxStrct struct {
	mess chan storageValueStruct
	counter int
}

var redux sync.Map

func newRequestRequired(key string) bool {
	reqElI, _ := redux.Load(key)
	reqEl, ok := reqElI.(ChnlMtxStrct)

	if ok {
		reqEl.counter++
		redux.Store(key, reqEl)
		return false
	}

	reqEl.counter = 0
	reqEl.mess = make(chan storageValueStruct)
	redux.Store(key, reqEl)
	return true
}

func notifyFollowers(key string, sValue storageValueStruct) {
	reqElI, _ := redux.Load(key)
	reqEl, _ := reqElI.(ChnlMtxStrct)

	for i := 1;  i<=reqEl.counter; i++ {
		reqEl.mess <- sValue
	}

	redux.Delete(key)
}

func waiter(ctx *fasthttp.RequestCtx, finish chan bool) {
	var bw *bufio.Writer
	var err error

	bw = fasthttp.AcquireWriter(ctx)

	for {
		select {
		case <-finish:
			return
		case <-time.After(100 * time.Millisecond):
			_, err = bw.Write(strResponseContinue)
			if err != nil {
				return
			}
			err = bw.Flush()
			if err != nil {
				return
			}
		}
	}
}

func logIncoming(guiLog *bcast.Group, ctx *fasthttp.RequestCtx, key string) {
	guiLog.Send(LogEntry{
		Ts:         time.Now().UTC().String(),
		Source:     getRequestSource(ctx),
		DestHost:   string(ctx.Request.Header.Peek("X-Upstream")),
		DestPath:   string(ctx.Request.RequestURI()),
		StorageKey: key,
	})
}

func logOutcoming(guiLog *bcast.Group, ctx *fasthttp.RequestCtx, key string, source string) {
	guiLog.Send(LogEntry{
		Ts:         time.Now().UTC().String(),
		Source:     source,
		DestHost:   string(ctx.Request.Header.Peek("X-Upstream")),
		DestPath:   string(ctx.Request.RequestURI()),
		StorageKey: key,
	})
}

func getRequestSource(ctx *fasthttp.RequestCtx) string {
	source := ctx.Request.Header.Peek("X-Source")
	if source == nil {
		return "unknown"
	}
	return string(source)
}

func CacheHandler(storage *node.RStorage, guiLog *bcast.Group) func(*fasthttp.RequestCtx) {
	view := func(ctx *fasthttp.RequestCtx) {
		if ctx.Request.Header.Peek("X-Upstream") == nil {
			ctx.Response.SetStatusCode(http.StatusBadRequest)
			return
		}

		if storage.RaftNode.State() != raft.Leader {
			ctx.Response.SetStatusCode(http.StatusFound)
			u, err := url.Parse(string(ctx.Request.RequestURI()))
			if err != nil {
				ctx.Response.SetStatusCode(http.StatusServiceUnavailable)
				return
			}
			u.Host = string(storage.RaftNode.Leader())
			ctx.Response.Header.Set("Location", fmt.Sprint(u))
			return
		}

		storageKey := toStorageKey(ctx)
		logIncoming(guiLog, ctx, storageKey)
		storageValue, presented := getFromStorage(storage, storageKey)
		if presented {
			logOutcoming(guiLog, ctx, storageKey, "storage")
			setResponse(ctx, storageValue)
			return
		}

		var finish = make(chan bool)
		go waiter(ctx, finish)

		if !newRequestRequired(storageKey) {
			println("wait for another thread")

			reqElI, _ := redux.Load(storageKey)
			reqEl, _ := reqElI.(ChnlMtxStrct)

			storageValue = <-reqEl.mess

			setResponse(ctx, storageValue)
			finish <- true
			logOutcoming(guiLog, ctx, storageKey, "storage")
			return
		}

		println("make new request")
		response, err := proxyRequest(ctx)
		if err != nil {
			println("new request failed")
			ctx.Response.SetStatusCode(http.StatusServiceUnavailable)

			finish <- true
			logOutcoming(guiLog, ctx, storageKey, "error")
			return
		}
		resBody, _ := ioutil.ReadAll(response.Body)

		//save to storage
		storageValue = createStorageValue(string(ctx.Path()), response, ctx.Request.Body(), resBody)

		if false && response.StatusCode >= 200 && response.StatusCode < 300 {
			b, _ := json.Marshal(storageValue)
			_ = storage.Set(storageKey, string(b))
		}

		finish <- true
		logOutcoming(guiLog, ctx, storageKey, "request")
		notifyFollowers(storageKey, storageValue)
		setResponse(ctx, storageValue)
	}
	return view
}

func getFromStorage(storage *node.RStorage, storageKey string) (storageValueStruct, bool) {
	savedValueRaw := storage.Get(storageKey)

	if savedValueRaw == "" {
		return storageValueStruct{}, false
	}

	var storageValue storageValueStruct
	_ = json.Unmarshal([]byte(savedValueRaw), &storageValue)

	t, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", storageValue.Ts)
	if t.Add(5 * time.Second).After(time.Now()) {
		return storageValue, true
	}

	return storageValueStruct{}, false
}

func toStorageKey(ctx *fasthttp.RequestCtx) string {
	var result = ctx.Request.Header.Peek("Upstream")[:]
	result = append(result, ctx.URI().String()[:]...)
	result = append(result, ctx.Request.Header.Peek("X-Request-Id")...)
	result = append(result, ctx.Request.Body()...)

	return base64.URLEncoding.EncodeToString(sha1.New().Sum(result))
}

func proxyRequest(ctx *fasthttp.RequestCtx) (*http.Response, error) {
	client := &http.Client{}

	host := getHost(string(ctx.Request.Header.Peek("X-Upstream")))
	var url = host + string(ctx.Request.RequestURI())

	newReq, _ := http.NewRequest(string(ctx.Method()), url, bytes.NewReader(ctx.Request.Body()))
	ctx.Request.Header.VisitAll(func(key, value []byte) {
		newReq.Header[string(key)] = []string{string(value)}
	})

	return client.Do(newReq)
}

func getHost(upstream string) string {
	if port, ok := Hosts[upstream]; ok {
		return "http://ts25.pyn.ru:" + strconv.Itoa(port)
	}

	if host, ok := ExternalHosts[upstream]; ok {
		return "http://" + host
	}

	return upstream
}

func setResponse(ctx *fasthttp.RequestCtx, value storageValueStruct) {
	for h, values := range value.ResHeaders {
		for _, val := range values {
			ctx.Response.Header.Set(h, val)
		}
	}
	ctx.Response.SetStatusCode(value.ResStatus)
	ctx.Response.SetBody([]byte(value.ResBody))
}
