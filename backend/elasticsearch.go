package backend

import (
    "context"
    "fmt"

    "around/constants"
    "around/util"

    "github.com/olivere/elastic/v7"
)
//结构体
//
var (
    ESBackend *ElasticsearchBackend
)
// 实例对象 // in java is same as favoriteDao
// client is same as the sessionFactory
// 
type ElasticsearchBackend struct { // similar with dao class 
    client *elastic.Client

}

// ESBackend = new ElasticsearchBackend
// 为了实例话so pointer一个
// go canot write into a class

// client的对象
func InitElasticsearchBackend(config *util.ElasticsearchInfo) {
	//client := new Clinet()
	//esBackend := new ElasticsearchBackend(myClient)
	
	//ESBackend = &esBackend
	//new cilent return error to err if there is error ex name and 
	client, err := elastic.NewClient( // new 新的链接
		elastic.SetURL(config.Address),
		elastic.SetBasicAuth(config.Username, config.Password))
	if err != nil {
		panic(err)
	}
	// exists is declare index exist or not.     // post _index is the id, ...struct 
	exists, err := client.IndexExists(constants.POST_INDEX).Do(context.Background())
    if err != nil {
        panic(err)
    }
	//https://www.geeksforgeeks.org/sql-query-complexity/
	if !exists {
        mapping := `{
            "mappings": {
                "properties": {
                    "id":       { "type": "keyword", "index": true }, // keyword == string 
                    "user":     { "type": "keyword" },
                    "message":  { "type": "text" },     // text == string search by text, all the part of vincent that show up 
                    "url":      { "type": "keyword", "index": false },
                    "type":     { "type": "keyword", "index": false }
                }
            }
        }`
        _, err := client.CreateIndex(constants.POST_INDEX).Body(mapping).Do(context.Background())
        if err != nil {
            panic(err)
        }
    }

	exists, err = client.IndexExists(constants.USER_INDEX).Do(context.Background())
    if err != nil {
        panic(err)
    }

    if !exists { // 不存在后mapping
        mapping := `{
                        "mappings": {
                                "properties": {
                                        "username": {"type": "keyword"},
                                        "password": {"type": "keyword"},
                                        "age":      {"type": "long", "index": false},
                                        "gender":   {"type": "keyword", "index": false}
                                }
                        }
                }`
        _, err = client.CreateIndex(constants.USER_INDEX).Body(mapping).Do(context.Background())
        if err != nil {
            panic(err)
        }
    }
    fmt.Println("Indexes are created.")

	ESBackend = &ElasticsearchBackend{client: client}
}

// ESBacked.Read() 调用不用copy每一次的实例
// search return two results. err and searchresult
func (backend *ElasticsearchBackend) ReadFromES (query elastic.Query, index string) (*elastic.SearchResult, error) {
    searchResult, err := backend.client.Search().
        Index(index).
        Query(query).
        Pretty(true).
        Do(context.Background())
    if err != nil {
        return nil, err // handle --retry? print? set_response_status?
    }

    return searchResult, nil
}

// interface in the golang means: object 对象，类型不一定
func (backend ElasticsearchBackend) SaveToES (i interface{}, index string, id string) error {
    _, err := backend.client.Index().
		Index(index).
		Id(id).
		BodyJson(i).
		Do(context.Background())
	return err
}

// func (backend ElasticsearchBackend) Write () {

// }

// func (backend ElasticsearchBackend) Delete () {

// }

//var backend1 &ElasticsearcBackend  -> star in the 
//backend.Read()
// backend.Write()
// backend.Delete()

// class Myclass { 

    