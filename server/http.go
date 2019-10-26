package server

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/grafov/bcast"
	"hhttpp/node"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type joinData struct {
	Address string `json:"address"`
}

type LogEntry struct {
	Ts, Source, DestHost, DestPath, StorageKey, BirthTime string
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

var ChnlAsync sync.Map

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

func wsHandler(guiLog *bcast.Group) func(*gin.Context) {
	view := func(c *gin.Context) {
		httpConn, _ := upgrader.Upgrade(c.Writer, c.Request, nil)
		chnlConn := guiLog.Join()

		for {
			dataI := chnlConn.Recv()
			data, _ := dataI.(LogEntry)
			dataBytes, _ := json.Marshal(data)

			if err := httpConn.WriteMessage(1, dataBytes); err != nil {
				return
			}
		}
	}
	return view
}

func index(c *gin.Context) {
	dat, _ := ioutil.ReadFile("/Users/l.vinogradov/projects/go/src/hhttpp/static/index.html")
	c.Data(http.StatusOK, "text/html; charset=utf-8", dat)
}

func js(c *gin.Context) {
	dat, _ := ioutil.ReadFile("/Users/l.vinogradov/projects/go/src/hhttpp/static/main.js")
	c.Data(http.StatusOK, "text/javascript; charset=utf-8", dat)
}

func css(c *gin.Context) {
	dat, _ := ioutil.ReadFile("/Users/l.vinogradov/projects/go/src/hhttpp/static/main.css")
	c.Data(http.StatusOK, "text/css; charset=utf-8", dat)
}

func createStorageValue(path string, response *http.Response, reqBody []byte, resBody []byte) storageValueStruct {
	return storageValueStruct{
		Path: path,
		//ReqHeaders: context.Request.Header,
		ReqBody: string(reqBody),
		ResStatus: response.StatusCode,
		ResBody: string(resBody),
		ResHeaders: response.Header,
		Ts: time.Now().UTC().String(),
	}
}

func setupRouter(raftNode *node.RStorage, guiLog *bcast.Group) *gin.Engine {
	router := gin.Default()

	router.POST("/cluster/join/", joinView(raftNode))
	router.Any("/ws", wsHandler(guiLog))
	router.GET("/index", index)
	router.GET("/main.js", js)
	router.GET("/main.css", css)

	return router
}

// RunHTTPServer starts HTTP server
func RunHTTPServer(raftNode *node.RStorage, guiLog *bcast.Group) {
	router := setupRouter(raftNode, guiLog)
	_ = router.Run() // listen and serve on 0.0.0.0:8080
}
