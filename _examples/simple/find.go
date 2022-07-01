package simple

import (
	"context"
	"errors"
	"fmt"
	"github.com/drtechco/mgqb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func Findmain() {

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

	conn.Database("test").Collection("ratings").Drop(context.Background())
	var docs []interface{}
	err = bson.UnmarshalExtJSON([]byte(`
 [
  { "_id": 1, "name": "apples", "qty": 5, "rating": 3 },
  { "_id": 2, "name": "bananas", "qty": 7, "rating": 1, "microsieverts": 0.1 },
  { "_id": 3, "name": "oranges", "qty": 6, "rating": 2 },
  { "_id": 4, "name": "avocados", "qty": 3, "rating": 5 }
 ] `), true, &docs)
	if err != nil {
		panic(err)
	}
	_, err2 := conn.Database("test").Collection("ratings").InsertMany(context.Background(), docs)
	if err2 != nil {
		panic(err2)
	}
	cus, err := conn.Database("test").Collection("ratings").Find(context.Background(), mgqb.Match("qty", mgqb.WhereOperators.EQ, 5).D())
	if err != nil {
		panic(err)
	}
	var res bson.A
	err = cus.All(context.Background(), &res)
	if err != nil {
		panic(err)
	}
	if len(res) != 1 {
		panic(errors.New("res length not 1"))
	}
	fmt.Println(res)
}
