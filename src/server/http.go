package server

import (
	"../node"
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"io"
	"log"
	"net/http"
	"time"
)

type joinData struct {
	Address string `json:"address"`
}

type storageValueStruct struct {
	Path string
	ReqHeaders map[string][]string
	ReqBody string
	ResStatus int
	ResBody string
	ResHeaders map[string][]string
	Ts string
}

// joinView handles 'join' request from another nodes
// if some node wants to join to the cluster it must be added by leader
// so this node sends a POST request to the leader with it's address and the leades adds it as a voter
// if this node os not a leader, it forwards request to current cluster leader
func joinView(storage *node.RStorage) func(*gin.Context) {
	view := func(c *gin.Context) {
		var data joinData
		err := c.BindJSON(&data)
		if err != nil {
			log.Printf("[ERROR] Reading POST data error: %+v", err)
			c.JSON(503, gin.H{})
			return
		}

		err = storage.AddVoter(data.Address)
		if err != nil {
			c.JSON(503, gin.H{})
		} else {
			c.JSON(200, gin.H{})
		}
	}
	return view
}

func cacheHandler(storage *node.RStorage) func(*gin.Context) {
	view := func(context *gin.Context) {
		if context.Request.Header["New-Host"] == nil {
			context.JSON(http.StatusBadRequest, gin.H{
				"error": "new-host is empty",
			})
			return
		}

		if storage.RaftNode.State() != 2 {  //not a master
			context.JSON(http.StatusForbidden, gin.H{
				"master": storage.RaftNode.Leader(),
			})
			return
		}

		storageKey := createStorageKey(context)
		savedValueRaw := storage.Get(storageKey)

		if savedValueRaw != "" {
			var storageValue storageValueStruct
			err := json.Unmarshal([]byte(savedValueRaw), &storageValue)
			if err != nil {
				println("can't parse value")
				context.Status(http.StatusServiceUnavailable)
				return
			}

			t, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", storageValue.Ts)
			if t.Add(5 * time.Second).After(time.Now()) {
				println("return from storage")
				setContextResponse(context, storageValue)
				return
			}
		}
		println("make new request")

		reqBody := getBodyBytes(context.Request.Body)

		response, err := doNewRequest(context, reqBody)
		if err != nil {
			println("new request failed")
			context.Status(http.StatusServiceUnavailable)
			return
		}
		resBody := getBodyBytes(response.Body)

		//save to storage
		storageValue := createStorageValue(context, response, reqBody, resBody)

		if response.StatusCode >= 200 && response.StatusCode < 300 {
			b, _ := json.Marshal(storageValue)
			err = storage.Set(storageKey, string(b))
			if err != nil {
				println("set new value to storage failed")
				context.Status(http.StatusServiceUnavailable)
				return
			}
		}

		setContextResponse(context, storageValue)
	}
	return view
}

func setContextResponse(context *gin.Context, value storageValueStruct) {
	for h, values := range value.ResHeaders {
		for _, val := range values {
			context.Header(h, val)
		}
	}
	context.Data(value.ResStatus, value.ResHeaders["Content-Type"][0], []byte(value.ResBody))
}

func doNewRequest(context *gin.Context, body []byte) (*http.Response, error) {
	client := &http.Client{}

	var newUrl = "http://" + context.Request.Header["New-Host"][0] + "/a" + context.Param("proxyPath")

	newReq, _ := http.NewRequest(context.Request.Method, newUrl, bytes.NewReader(body))
	for h, val := range context.Request.Header {
		if !stringInSlice(h, []string{"New-Host"}) {
			newReq.Header[h] = val
		}
	}
	//formParam? something else?
	return client.Do(newReq)
}

func createStorageKey(context *gin.Context) string {
	var result = getHeaderSafe(context, "New-Host") +
		context.Param("proxyPath") +
		getHeaderSafe(context, "Authorization") +
		getHeaderSafe(context, "Hh-Proto-Session") +
		getHeaderSafe(context, "X-Request-Id")

	return base64.URLEncoding.EncodeToString(sha1.New().Sum([]byte(result)))
}

func createStorageValue(context *gin.Context, response *http.Response, reqBody []byte, resBody []byte) storageValueStruct {
	return storageValueStruct{
		Path: context.Param("proxyPath"),
		ReqHeaders: context.Request.Header,
		ReqBody: string(reqBody),
		ResStatus: response.StatusCode,
		ResBody: string(resBody),
		ResHeaders: response.Header,
		Ts: time.Now().UTC().String(),
	}
}

func getHeaderSafe(context *gin.Context, key string) string {
	if context.Request.Header[key] == nil {
		return ""
	}
	return context.Request.Header[key][0]
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func getBodyBytes(reqBody io.ReadCloser) []byte {
	buf := make([]byte, 1024)
	num, _ := reqBody.Read(buf)
	return buf[0:num]
}

func semiEcho() func(*gin.Context) {
	view := func(context *gin.Context) {
		reqBody := string(getBodyBytes(context.Request.Body))

		context.Header("Qqq", "Www")

		context.JSON(http.StatusCreated, gin.H{
			"testbody": reqBody,
			"testheaders": context.Request.Header,
		})
	}
	return view
}

func setupRouter(raftNode *node.RStorage) *gin.Engine {
	router := gin.Default()

	router.POST("/cluster/join/", joinView(raftNode))
	router.Any("/a/*proxyPath", semiEcho())
	router.Any("/g/*proxyPath", cacheHandler(raftNode))

	return router
}

// RunHTTPServer starts HTTP server
func RunHTTPServer(raftNode *node.RStorage) {
	router := setupRouter(raftNode)
	_ = router.Run() // listen and serve on 0.0.0.0:8080
}
