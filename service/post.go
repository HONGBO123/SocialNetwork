package service

import (
	"mime/multipart"
    "reflect"

    "around/backend"
    "around/constants"
    "around/model"

    "github.com/olivere/elastic/v7"
)
// user= sun in the end of the link https;
// 
func SearchPostsByUser(user string) ([]model.Post,error) {
	termQuery := elastic.NewTermQuery("user", user)
	// backend.ReadFrom...... ReadFromES返回给上一个程序
	//属于backend里的变量-> var ()
	// constants in the constants.go 常量
	//
	searchResult, err := backend.ESBackend.ReadFromES(termQuery, constants.POST_INDEX) // backend is package 
    if err != nil {
        return nil, err
    }
	//if no error on the return.
    //return getPostFromSearchResult(searchResult), nil
    //
	//item is [interface{},..]
	var ptype model.Post
	var posts []model.Post
	// reflect 去判断这个数据类型，  cast Typeof 判断类型
	for _, item := range searchResult.Each(reflect.TypeOf(ptype)) {
		p := item.(model.Post) // 满足条件 再去cast， model.Post is like Javva the instanceof 
		posts = append(posts, p)
		//fmt.Printf("Tweet by %s: %s\n", t.User, t.Message)
	}
	return posts, nil
}

func SearchPostsByKeywords(keywords string) ([]model.Post, error) {
	query := elastic.NewMatchQuery("message", keywords) // match the message or and
	query.Operator("AND")
	if keywords == "" {
		query.ZeroTermsQuery("all") // 没有关键词
	}

	searchResult, err := backend.ESBackend.ReadFromES(query, constants.POST_INDEX) 
    if err != nil {
        return nil, err
    }

	var ptype model.Post
	var posts []model.Post
	
	for _, item := range searchResult.Each(reflect.TypeOf(ptype)) {
		p := item.(model.Post) 
		posts = append(posts, p)
		
	}
	return posts, nil
}
// func getPostFromSearchResult(searchResult *elastic.SearchResult) []model.Post 

func SavePost(post *model.Post, file multipart.File) error {
    medialink, err := backend.GCSBackend.SaveToGCS(file, post.Id)
    if err != nil {
        return err
    }
    post.Url = medialink

    return backend.ESBackend.SaveToES(post, constants.POST_INDEX, post.Id)
}


