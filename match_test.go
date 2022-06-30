package main

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
)

func Test_match1(t *testing.T) {
	t.Run("Test_match1", func(t *testing.T) {
		conn := initTestConn()
		//conn.Database("test").Collection("articles").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
  { "_id" : "512bc95fe835e68f199c8686", "author" : "dave", "score" : 80, "views" : 100 },
{ "_id" : "512bc962e835e68f199c8687", "author" : "dave", "score" : 85, "views" : 521 },
{ "_id" : "55f5a192d4bede9ac365b257", "author" : "ahn", "score" : 60, "views" : 1000 },
{ "_id" : "55f5a192d4bede9ac365b258", "author" : "li", "score" : 55, "views" : 5000 },
{ "_id" : "55f5a1d3d4bede9ac365b259", "author" : "annT", "score" : 60, "views" : 50 },
{ "_id" : "55f5a1d3d4bede9ac365b25a", "author" : "li", "score" : 94, "views" : 999 },
{ "_id" : "55f5a1d3d4bede9ac365b25b", "author" : "ty", "score" : 95, "views" : 1000 }
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		//_, err2 := conn.Database("test").Collection("articles").InsertMany(context.Background(), docs)
		//if err2 != nil {
		//	t.Fatal(err2)
		//}

		cus, err := conn.Database("test").Collection("articles").
			Aggregate(context.Background(), Pipeline().SetMatchRaw(bson.D{{"author", "dave"}}).DS())
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

func Test_match2(t *testing.T) {
	t.Run("Test_match2", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("articles").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
  { "_id" : "512bc95fe835e68f199c8686", "author" : "dave", "score" : 80, "views" : 100 },
{ "_id" : "512bc962e835e68f199c8687", "author" : "dave", "score" : 85, "views" : 521 },
{ "_id" : "55f5a192d4bede9ac365b257", "author" : "ahn", "score" : 60, "views" : 1000 },
{ "_id" : "55f5a192d4bede9ac365b258", "author" : "li", "score" : 55, "views" : 5000 },
{ "_id" : "55f5a1d3d4bede9ac365b259", "author" : "annT", "score" : 60, "views" : 50 },
{ "_id" : "55f5a1d3d4bede9ac365b25a", "author" : "li", "score" : 94, "views" : 999 },
{ "_id" : "55f5a1d3d4bede9ac365b25b", "author" : "ty", "score" : 95, "views" : 1000 }
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 := conn.Database("test").Collection("articles").InsertMany(context.Background(), docs)
		if err2 != nil {
			t.Fatal(err2)
		}
		cus, err := conn.Database("test").Collection("articles").
			Aggregate(context.Background(), Pipeline().
				SetMatchRaw(bson.D{{"$or",
					[]bson.D{{{"score",
						bson.M{"$gt": 70, "$lt": 90}}},
						{{"views",
							bson.M{"$gte": 1000}}}}}}).
				Group(Group().Field("_id", nil).Field("count", bson.M{"$sum": 1})).DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 1 {
			t.Fatal(errors.New("res length not 1"))
		}
		fmt.Println(res)
	})

}
