package mgqb

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
)

func Test_pipeline1(t *testing.T) {
	t.Run("Test_pipeline1", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("orders").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
   { "_id": 0, "name": "Pepperoni", "size": "small", "price": 19, "quantity": 10, "date": "2021-03-13T08:14:30Z" },
   { "_id": 1, "name": "Pepperoni", "size": "medium", "price": 20, "quantity": 20, "date" : "2021-03-13T09:13:24Z" },
   { "_id": 2, "name": "Pepperoni", "size": "large", "price": 21, "quantity": 30, "date" : "2021-03-17T09:22:12Z" },
   { "_id": 3, "name": "Cheese", "size": "small", "price": 12, "quantity": 15, "date" : "2021-03-13T11:21:39.736Z" },
   { "_id": 4, "name": "Cheese", "size": "medium", "price": 13, "quantity":50, "date" : "2022-01-12T21:23:13.331Z" },
   { "_id": 5, "name": "Cheese", "size": "large", "price": 14, "quantity": 10, "date" : "2022-01-12T05:08:13Z" },
   { "_id": 6, "name": "Vegan", "size": "small", "price": 17, "quantity": 10, "date" :"2021-01-13T05:08:13Z" },
   { "_id": 7, "name": "Vegan", "size": "medium", "price": 18, "quantity": 10, "date" : "2021-01-13T05:10:13Z"}
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 := conn.Database("test").Collection("orders").InsertMany(context.Background(), docs)
		if err2 != nil {
			t.Fatal(err2)
		}

		cus, err := conn.Database("test").Collection("orders").
			Aggregate(context.Background(), Pipeline().
				SetMatchRaw(bson.D{{"size", "medium"}}).
				Group(Group().FieldSimple("_id", "$name").
					Field("totalQuantity", bson.M{"$sum": "$quantity"})).DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 2 {
			t.Fatal(errors.New("res length not 2"))
		}
		fmt.Println(res)
	})

}
