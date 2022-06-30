package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {

	conn, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://10.117.1.102:27017/test?connect=direct"))
	if err != nil {
		panic(err)
	}
	// 检查连接
	err = conn.Ping(context.TODO(), nil)
	if err != nil {
		panic(err)
	}

	fmt.Println("Connected to MongoDB!")
	conn.Database("test").Collection("orders").Drop(context.Background())
	conn.Database("test").Collection("orders_detail").Drop(context.Background())
	conn.Database("test").Collection("books").Drop(context.Background())
	conn.Database("test").Collection("authors").Drop(context.Background())

	var docs []interface{}
	err = bson.UnmarshalExtJSON([]byte(`
 [
   	{ "_id": 1, "order": "1100" , "dataTime":"2021-05-30" },
	{ "_id": 2, "order": "1101", "dataTime":"2022-06-30" },
	{ "_id": 3, "order": "1102", "dataTime":"2019-04-30" },
	{ "_id": 4, "order": "1103", "dataTime":"2006-06-30" },
	{ "_id": 5, "order": "1104", "dataTime":"2021-03-30" },
	{ "_id": 6, "order": "1105", "dataTime":"2022-05-30" },
	{ "_id": 7, "order": "1106", "dataTime":"2005-06-30" },
	{ "_id": 8, "order": "1107", "dataTime":"2015-02-30" }
 ] `), true, &docs)
	if err != nil {
		panic(err)
	}
	_, err2 := conn.Database("test").Collection("orders").InsertMany(context.Background(), docs)
	if err2 != nil {
		panic(err2)
	}

	var docs2 []interface{}
	err = bson.UnmarshalExtJSON([]byte(`
 [
   	{ "_id": "1", "order": "1100" , "bookId": 1 , "bookName" : "力霸天", "type" : "机车", "count":2 , "money" : 20 },
	{ "_id": "2", "order": "1100" , "bookId": 2, "bookName" : "霹雳火", "type" : "机车", "count":5, "money" : 15 },
	{ "_id": "3", "order": "1103", "bookId": 3, "bookName" : "猛虎王", "type" : "猛兽", "count":7, "money" : 10 },
	{ "_id": "4", "order": "1103", "bookId": 4, "bookName" : "狂野猩", "type" : "猛兽", "count":2, "money" : 18 },
	{ "_id": "5", "order": "1103", "bookId": 5, "bookName" : "超音速", "type" : "机车", "count":12, "money" : 5 },
	{ "_id": "6", "order": "1103", "bookId": 6, "bookName" : "龙卷风", "type" : "机车", "count":5, "money" : 52 },
	{ "_id": "7", "order": "1103", "bookId": 7, "bookName" : "金铁兽", "type" : "猛兽", "count":7, "money" : 10 },
	{ "_id": "8", "order": "1108", "bookId": 8, "bookName" : "狂野猩历险记", "type" : "冒险", "count":8, "money" : 25 },
	{ "_id": "9", "order": "1103", "bookId": 9, "bookName" : "狂野猩穿扮", "type" : "打扮", "count":6, "money" : 48 },
	{ "_id": "10", "order": "1102", "bookId": 10, "bookName" : "暴龙神", "type" : "猛兽", "count":8, "money" : 35 },
	{ "_id": "11","order": "1104", "bookId": 11, "bookName" : "暴龙神", "type" : "猛兽", "count":8, "money" : 35 },
	{ "_id": "12", "order": "1107", "bookId": 12, "bookName" : "暴龙神", "type" : "猛兽", "count":8, "money" : 35 }

 ] `), true, &docs2)
	if err != nil {
		panic(err)
	}
	_, err2 = conn.Database("test").Collection("orders_detail").InsertMany(context.Background(), docs2)
	if err2 != nil {
		panic(err2)
	}

	var docs3 []interface{}
	err = bson.UnmarshalExtJSON([]byte(`
 [
   	{ "_id" : 1, "bookId": 1 , "bookName" : "力霸天", "type" : "机车", "money" : 20, "authorId":1, "author":"史蒂夫" },
	{ "_id" : 2, "bookId": 2, "bookName" : "霹雳火", "type" : "机车", "money" : 15, "authorId":2,"author":"戈萨" },
	{ "_id" : 3, "bookId": 3, "bookName" : "猛虎王", "type" : "猛兽", "money" : 10, "authorId":3, "author":"撒旦" },
	{ "_id" : 4, "bookId": 4, "bookName" : "狂野猩", "type" : "猛兽", "money" : 18,"authorId":4, "author":"电饭锅" },
	{ "_id" : 5, "bookId": 5, "bookName" : "超音速", "type" : "机车", "money" : 5, "authorId":5, "author":"阿斯顿发" },
	{ "_id" : 6, "bookId": 6, "bookName" : "龙卷风", "type" : "机车", "money" : 52, "authorId":6, "author":"豆腐干" },
	{ "_id" : 7, "bookId": 7, "bookName" : "金铁兽", "type" : "猛兽", "money" : 10, "authorId":7, "author":"风格化" },
	{ "_id" : 8, "bookId": 8, "bookName" : "狂野猩历险记", "type" : "冒险", "money" : 25, "authorId":8, "author":"大佛古寺" },
	{ "_id" : 9, "bookId": 9, "bookName" : "狂野猩穿扮", "type" : "打扮", "money" : 48, "authorId":9, "author":"维尔" },
	{ "_id" : 10, "bookId": 10, "bookName" : "暴龙神", "type" : "猛兽", "money" : 35, "authorId":10, "author":"官方回复" },
	{ "_id" : 11, "bookId": 11, "bookName" : "银铁兽", "type" : "猛兽", "money" : 78, "authorId":11, "author":"傻大个" },
	{ "_id" : 12, "bookId": 12, "bookName" : "银铁兽", "type" : "猛兽", "money" : 78, "authorId":11, "author":"傻大个" }
 ] `), true, &docs3)
	if err != nil {
		panic(err)
	}
	_, err2 = conn.Database("test").Collection("books").InsertMany(context.Background(), docs3)
	if err2 != nil {
		panic(err2)
	}

	var docs4 []interface{}
	err = bson.UnmarshalExtJSON([]byte(`
 [
   	{ "authorId":1, "author":"史蒂夫" },
	{ "authorId":2,"author":"戈萨" },
	{ "authorId":3, "author":"撒旦" },
	{ "authorId":4, "author":"电饭锅" },
	{ "authorId":5, "author":"阿斯顿发" },
	{ "authorId":6, "author":"豆腐干" },
	{ "authorId":7, "author":"风格化" },
	{ "authorId":8, "author":"大佛古寺" },
	{ "authorId":9, "author":"维尔" },
	{ "authorId":10, "author":"官方回复" },
	{ "authorId":11, "author":"傻大个" }
 ] `), true, &docs4)
	if err != nil {
		panic(err)
	}
	_, err2 = conn.Database("test").Collection("authors").InsertMany(context.Background(), docs4)
	if err2 != nil {
		panic(err2)
	}

	cus, err := conn.Database("test").Collection("orders").
		Aggregate(context.Background(), Pipeline().
			Lookup(
				Lookup().From("orders_detail").LocalField("order").ForeignField("order").As("od_docs").
					Pipeline(
						Pipeline().Lookup(
							Lookup().From("books").LocalField("bookName").ForeignField("bookName").As("bo_docs").
								Pipeline(Pipeline().Lookup(
									Lookup().From("authors").LocalField("authorId").ForeignField("authorId").As("au_docs"),
								),
								),
						).UnwindSimple("$au_docs").ProjectAny("ddd2", "$au_docs.authorId"),
					),
			).
			//ProjectAny("ddd", "$bo_docs.ddd2").

			//Group(Group().FieldSimple("_id", "$(od_docs.bo_docs.au_docs.authorId)").Field("c", bson.M{"$sum": 1})).
			DS())
	if err != nil {
		panic(err)
	}
	var res []map[string]interface{}
	err = cus.All(context.Background(), &res)
	if err != nil {
		panic(err)
	}
	d, _ := json.Marshal(res)
	fmt.Println(string(d))

}
