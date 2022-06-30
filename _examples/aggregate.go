package _examples

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type Sales struct {
	id       int
	item     string
	price    float64
	quantity int32
	data     time.Time
}

func main() {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://10.117.1.102:27017"))
	if err != nil {
		panic(err)
	}
	// 检查连接
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		panic(err)
	}

	client.Database("test").Collection("sales").Drop(context.Background())
	fmt.Println("Connected to MongoDB!")

	//  如果你的应用程序不再需要一个连接， 该连接可以使用client.Disconnect()被关闭
	//err = client.Disconnect(context.TODO())
	//if err != nil {
	//	log.Fatal(err)
	//}
	fmt.Println("Connection to MongoDB closed.")
}
