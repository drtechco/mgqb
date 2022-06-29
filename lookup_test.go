package mgqb

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
)

func Test_lookup1(t *testing.T) {
	t.Run("Test_lookup1", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("orders").Drop(context.Background())
		conn.Database("test").Collection("inventory").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
   { "_id" : 1, "item" : "almonds", "price" : 12, "quantity" : 2 },
   { "_id" : 2, "item" : "pecans", "price" : 20, "quantity" : 1 },
   { "_id" : 3  }
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 := conn.Database("test").Collection("orders").InsertMany(context.Background(), docs)
		if err2 != nil {
			t.Fatal(err2)
		}
		var docs2 []interface{}
		err = bson.UnmarshalExtJSON([]byte(`
 [
   { "_id" : 1, "sku" : "almonds", "description": "product 1", "instock" : 120 },
   { "_id" : 2, "sku" : "bread", "description": "product 2", "instock" : 80 },
   { "_id" : 3, "sku" : "cashews", "description": "product 3", "instock" : 60 },
   { "_id" : 4, "sku" : "pecans", "description": "product 4", "instock" : 70 },
   { "_id" : 5, "sku": null, "description": "Incomplete" },
   { "_id" : 6 }
 ] `), true, &docs2)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 = conn.Database("test").Collection("inventory").InsertMany(context.Background(), docs2)
		if err2 != nil {
			t.Fatal(err2)
		}
		cus, err := conn.Database("test").Collection("orders").
			Aggregate(context.Background(), Pipeline().
				Lookup(Lookup().
					From("inventory").
					LocalField("item").
					ForeignField("sku").
					As("inventory_docs")).DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		//if len(res) != 3 {
		//	t.Fatal(errors.New("res length not 3"))
		//}
		fmt.Println(res)
	})

}

func Test_lookup2(t *testing.T) {
	t.Run("Test_lookup2", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("classes").Drop(context.Background())
		conn.Database("test").Collection("members").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
   { "_id": 1, "title": "Reading is ...", "enrollmentlist": [ "giraffe2", "pandabear", "artie" ], "days": ["M", "W", "F"] },
   { "_id": 2, "title": "But Writing ...", "enrollmentlist": [ "giraffe1", "artie" ], "days": ["T", "F"] }
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 := conn.Database("test").Collection("classes").InsertMany(context.Background(), docs)
		if err2 != nil {
			t.Fatal(err2)
		}
		var docs2 []interface{}
		err = bson.UnmarshalExtJSON([]byte(`
 [
   { "_id": 1, "name": "artie", "joined": "2016-05-01", "status": "A" },
   { "_id": 2, "name": "giraffe", "joined": "2017-05-01", "status": "D" },
   { "_id": 3, "name": "giraffe1", "joined": "2017-10-01", "status": "A" },
   { "_id": 4, "name": "panda", "joined": "2018-10-11", "status": "A" },
   { "_id": 5, "name": "pandabear", "joined": "2018-12-01", "status": "A" },
   { "_id": 6, "name": "giraffe2", "joined": "2018-12-01", "status": "D" }
 ] `), true, &docs2)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 = conn.Database("test").Collection("members").InsertMany(context.Background(), docs2)
		if err2 != nil {
			t.Fatal(err2)
		}
		cus, err := conn.Database("test").Collection("classes").
			Aggregate(context.Background(), Pipeline().
				Lookup(Lookup().
					From("members").
					LocalField("enrollmentlist").
					ForeignField("name").
					As("enrollee_info")).DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		//if len(res) != 3 {
		//	t.Fatal(errors.New("res length not 3"))
		//}
		fmt.Println(res)
	})

}
