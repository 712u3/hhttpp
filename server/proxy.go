package server

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"github.com/valyala/fasthttp"
	"hhttpp/node"
	"net/http"
	"time"
)

var strResponseContinue = []byte("HTTP/1.1 102 Processing\r\n\r\n")

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

func CacheHandler(storage *node.RStorage) func(*fasthttp.RequestCtx) {
	view := func(ctx *fasthttp.RequestCtx) {
		if ctx.Request.Header.Peek("X-Upstream") == nil {
			ctx.Response.SetStatusCode(http.StatusBadRequest)
			return
		}

		if storage.RaftNode.State() != 2 {  //not a master
			ctx.Response.SetStatusCode(http.StatusForbidden)
			return
		}

		storageKey := toStorageKey(ctx)
		savedValueRaw := storage.Get(storageKey)

		if savedValueRaw != "" {
			var storageValue storageValueStruct
			err := json.Unmarshal([]byte(savedValueRaw), &storageValue)
			if err != nil {
				println("can't parse value")
				ctx.Response.SetStatusCode(http.StatusServiceUnavailable)
				return
			}

			t, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", storageValue.Ts)
			if t.Add(5 * time.Second).After(time.Now()) {
				println("return from storage")
				setResponse(ctx, storageValue)
				return
			}
		}
		println("make new request2")

		var finish = make(chan bool)
		go waiter(ctx, finish)

		response, err := proxyRequest(ctx)
		if err != nil {
			println("new request failed")
			ctx.Response.SetStatusCode(http.StatusServiceUnavailable)

			finish <- true
			return
		}
		resBody := getBodyBytes(response.Body)

		//save to storage
		storageValue := createStorageValue(string(ctx.Path()), response, ctx.Request.Body(), resBody)

		if response.StatusCode >= 200 && response.StatusCode < 300 {
			b, _ := json.Marshal(storageValue)
			err = storage.Set(storageKey, string(b))
			if err != nil {
				println("set new value to storage failed")
				ctx.Response.SetStatusCode(http.StatusServiceUnavailable)

				finish <- true
				return
			}
		}

		finish <- true

		setResponse(ctx, storageValue)
	}
	return view
}

func toStorageKey(ctx *fasthttp.RequestCtx) string {
	var result = ctx.Request.Header.Peek("Upstream")[:]
	result = append(result, ctx.Path()[:]...)
	result = append(result, ctx.Request.Header.Peek("Authorization")[:]...)
	result = append(result, ctx.Request.Header.Peek("Hh-Proto-Session")...)
	result = append(result, ctx.Request.Header.Peek("X-Request-Id")...)

	return base64.URLEncoding.EncodeToString(sha1.New().Sum(result))
}

func proxyRequest(ctx *fasthttp.RequestCtx) (*http.Response, error) {
	client := &http.Client{}

	var newUrl = "http://" + string(ctx.Request.Header.Peek("X-Upstream")) + string(ctx.Path())

	newReq, _ := http.NewRequest(string(ctx.Method()), newUrl, bytes.NewReader(ctx.Request.Body()))
	ctx.Request.Header.VisitAll(func(key, value []byte) {
		newReq.Header[string(key)] = []string{string(value)}
	})

	//formParam? something else?
	return client.Do(newReq)
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
