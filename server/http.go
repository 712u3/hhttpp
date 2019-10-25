package server

import (
	"github.com/gin-gonic/gin"
	"hhttpp/node"
	"log"
	"net/http"
	"time"
)

type joinData struct {
	Address string `json:"address"`
	ProxyAddress string `json:"proxy"`
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
		err2 := storage.Set(data.Address, data.ProxyAddress)
		if err != nil && err2 != nil {
			c.JSON(503, gin.H{})
		} else {
			c.JSON(200, gin.H{})
		}
	}
	return view
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

func setupRouter(raftNode *node.RStorage) *gin.Engine {
	router := gin.Default()

	router.POST("/cluster/join/", joinView(raftNode))

	return router
}

// RunHTTPServer starts HTTP server
func RunHTTPServer(raftNode *node.RStorage) {
	router := setupRouter(raftNode)
	_ = router.Run() // listen and serve on 0.0.0.0:8080
}
