package server

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/valyala/fasthttp"
	"hhttpp/node"
	"io/ioutil"
	"log"
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

func CacheHandler(storage *node.RStorage) func(*fasthttp.RequestCtx) {
	view := func(ctx *fasthttp.RequestCtx) {
		if ctx.Request.Header.Peek("X-Upstream") == nil {
			ctx.Response.SetStatusCode(http.StatusBadRequest)
			return
		}

		if storage.RaftNode.State() != raft.Leader {
			u, err := url.Parse(string(ctx.Request.RequestURI()))
			if err != nil {
				ctx.Response.SetStatusCode(http.StatusServiceUnavailable)
				return
			}

			leader := string(storage.RaftNode.Leader())
			if leader == "" {
				ctx.Response.SetStatusCode(http.StatusExpectationFailed)
				return
			}

			ctx.Response.SetStatusCode(http.StatusFound)
			u.Host = storage.Get(leader)
			ctx.Response.Header.Set("Location", fmt.Sprint(u))
			return
		}

		storageKey := toStorageKey(ctx)
		storageValue, presented := getFromStorage(storage, storageKey)
		if presented {
			setResponse(ctx, storageValue)
			return
		}

		var finish = make(chan bool)
		go waiter(ctx, finish)

		if !newRequestRequired(storageKey) {
			log.Printf("[INFO] waiting for current request to completem, %s", string(ctx.Path()))

			reqElI, _ := redux.Load(storageKey)
			reqEl, _ := reqElI.(ChnlMtxStrct)

			storageValue = <-reqEl.mess

			setResponse(ctx, storageValue)
			finish <- true
			return
		}

		log.Printf("[INFO] request to %s, %s", string(ctx.Request.Header.Peek("X-Upstream")), string(ctx.Path()))
		response, err := proxyRequest(ctx)
		if err != nil {
			log.Printf("[ERROR] request to %s, %s failed with %s", string(ctx.Request.Header.Peek("X-Upstream")), string(ctx.Path()), err)
			ctx.Response.SetStatusCode(http.StatusServiceUnavailable)

			finish <- true
			notifyFollowers(storageKey, storageValue)
			return
		}
		resBody, _ := ioutil.ReadAll(response.Body)

		//save to storage
		storageValue = createStorageValue(string(ctx.Path()), response, ctx.Request.Body(), resBody)

		if response.StatusCode >= 200 && response.StatusCode < 300 {
			b, err := json.Marshal(storageValue)
			if err != nil {
				log.Printf("[ERROR] response store failed, %s", err)
			} else {
				err = storage.Set(storageKey, string(b))
				time.Sleep(1 * time.Second)
				if err != nil {
					log.Printf("[ERROR] response store failed, %s", err)
				}
			}
		}

		finish <- true
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
	if t.Add(5 * time.Minute).After(time.Now()) {
		return storageValue, true
	}

	return storageValueStruct{}, false
}

func toStorageKey(ctx *fasthttp.RequestCtx) string {
	var result = ctx.Request.Header.Peek("Upstream")[:]
	result = append(result, ctx.URI().RequestURI()[:]...)
	result = append(result, ctx.Request.Header.Peek("X-Request-Id")...)
	result = append(result, ctx.Request.Body()...)

	return base64.URLEncoding.EncodeToString(sha1.New().Sum(result))
}

func proxyRequest(ctx *fasthttp.RequestCtx) (*http.Response, error) {
	client := &http.Client{}

	var uri = getHost(string(ctx.Request.Header.Peek("X-Upstream"))) + string(ctx.Request.RequestURI())

	newReq, _ := http.NewRequest(string(ctx.Method()), uri, bytes.NewReader(ctx.Request.Body()))
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
