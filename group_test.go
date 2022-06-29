package mgqb

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"testing"
	"time"
)

func Test_group1(t *testing.T) {
	t.Run("Test_group1", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("sales").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
 { "_id" : 1, "item" : "abc", "price" :  10, "quantity" : 2, "date" : "2014-03-01T08:00:00Z"},
   { "_id" : 2, "item" : "jkl", "price" :20, "quantity" : 1, "date" : "2014-03-01T09:00:00Z"},
   { "_id" : 3, "item" : "xyz", "price" : 5, "quantity" : 10, "date" : "2014-03-15T09:00:00Z"},
   { "_id" : 4, "item" : "xyz", "price" : 5, "quantity" :  20, "date" : "2014-04-04T11:21:39.736Z"},
   { "_id" : 5, "item" : "abc", "price" : 10, "quantity" : 10, "date" : "2014-04-04T21:23:13.331Z"},
   { "_id" : 6, "item" : "def", "price" : 7.5, "quantity": 5, "date" : "2015-06-04T05:08:13Z"},
   { "_id" : 7, "item" : "def", "price" : 7.5, "quantity": 10, "date" : "2015-09-10T08:43:00Z"},
   { "_id" : 8, "item" : "abc", "price" : 10, "quantity" : 5, "date" : "2016-02-06T20:20:13Z"}
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 := conn.Database("test").Collection("sales").InsertMany(context.Background(), docs)
		if err2 != nil {
			t.Fatal(err2)
		}
		cus, err := conn.Database("test").Collection("sales").
			Aggregate(context.Background(), Pipeline().
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

		cus, err = conn.Database("test").Collection("sales").
			Aggregate(context.Background(), Pipeline().
				Group(Group().FieldSimple("_id", "$item")).DS())
		if err != nil {
			t.Fatal(err)
		}
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 4 {
			t.Fatal(errors.New("res length not 4"))
		}
		fmt.Println(res)

	})

}

func Test_group2(t *testing.T) {
	t.Run("Test_group2", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("sales").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
 { "_id" : 1, "item" : "abc", "price" :  10, "quantity" : 2, "date" : "2014-03-01T08:00:00Z"},
   { "_id" : 2, "item" : "jkl", "price" :20, "quantity" : 1, "date" : "2014-03-01T09:00:00Z"},
   { "_id" : 3, "item" : "xyz", "price" : 5, "quantity" : 10, "date" : "2014-03-15T09:00:00Z"},
   { "_id" : 4, "item" : "xyz", "price" : 5, "quantity" :  20, "date" : "2014-04-04T11:21:39.736Z"},
   { "_id" : 5, "item" : "abc", "price" : 10, "quantity" : 10, "date" : "2014-04-04T21:23:13.331Z"},
   { "_id" : 6, "item" : "def", "price" : 7.5, "quantity": 5, "date" : "2015-06-04T05:08:13Z"},
   { "_id" : 7, "item" : "def", "price" : 7.5, "quantity": 10, "date" : "2015-09-10T08:43:00Z"},
   { "_id" : 8, "item" : "abc", "price" : 10, "quantity" : 5, "date" : "2016-02-06T20:20:13Z"}
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 := conn.Database("test").Collection("sales").InsertMany(context.Background(), docs)
		if err2 != nil {
			t.Fatal(err2)
		}
		Pipeline().SetMatch(Match("totalSaleAmount", WhereOperators.GTE, 100))
		cus, err := conn.Database("test").Collection("sales").
			Aggregate(context.Background(), Pipeline().SetMatch(Match("totalSaleAmount", WhereOperators.GTE, 100)).
				Group(Group().FieldSimple("_id", "$item").Field("totalSaleAmount", bson.M{"$sum": bson.M{"$multiply": []string{"$price", "$quantity"}}})).DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 3 {
			t.Fatal(errors.New("res length not 3"))
		}
		fmt.Println(res)
	})

}

func Test_group3(t *testing.T) {
	t.Run("Test_group3", func(t *testing.T) {
		conn := initTestConn()
		//conn.Database("test").Collection("sales").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
   { "_id" : 1, "item" : "abc", "price" : 10, "quantity" : 2, "date" : "2014-03-01T08:00:00Z"},
  { "_id" : 2, "item" : "jkl", "price" : 20, "quantity" : 1, "date" : "2014-03-01T09:00:00Z"},
  { "_id" : 3, "item" : "xyz", "price" : 5, "quantity" : 10, "date" : "2014-03-15T09:00:00Z"},
  { "_id" : 4, "item" : "xyz", "price" : 5, "quantity" :  20, "date" : "2014-04-04T11:21:39.736Z"},
  { "_id" : 5, "item" : "abc", "price" : 10, "quantity" : 10, "date" : "2014-04-04T21:23:13.331Z"},
  { "_id" : 6, "item" : "def", "price" : 7.5, "quantity": 5, "date" : "2015-06-04T05:08:13Z"},
  { "_id" : 7, "item" : "def", "price" : 7.5, "quantity": 10, "date" : "2015-09-10T08:43:00Z"},
  { "_id" : 8, "item" : "abc", "price" : 10, "quantity" : 5, "date" : "2016-02-06T20:20:13Z"}
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		//_, err2 := conn.Database("test").Collection("sales").InsertMany(context.Background(), docs)
		//if err2 != nil {
		//	t.Fatal(err2)
		//}

		tt, err := time.Parse("2006-01-02", "2014-01-01")
		if err != nil {
			t.Fatal(err)
		}
		ttt, err := time.Parse("2006-01-02", "2015-01-01")
		if err != nil {
			t.Fatal(err)
		}

		cus, err := conn.Database("test").Collection("sales2").
			Aggregate(context.Background(), Pipeline().
				SetMatch(
					MatchWo(
						"date",
						WO(WhereOperators.GTE, primitive.NewDateTimeFromTime(tt)),
						WO(WhereOperators.LT, primitive.NewDateTimeFromTime(ttt)),
					),
				).
				Group(
					Group().
						Field("_id",
							bson.M{"$dateToString": bson.D{{"format", "%Y-%m-%d"}, {"date", "$date"}}}).
						Field("totalSaleAmount",
							bson.M{"$sum": bson.M{"$multiply": []string{"$price", "$quantity"}}}).
						Field("averageQuantity", bson.M{"$avg": "$quantity"}).
						Field("count", bson.M{"$sum": 1}),
				).
				SortDesc("totalSaleAmount").
				DS())
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 3 {
			t.Fatal(errors.New("res length not 3"))
		}
		fmt.Println(res)
	})

}

func Test_group4(t *testing.T) {
	t.Run("Test_group4", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("sales").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
  { "_id" : 1, "item" : "abc", "price" : 10, "quantity" : 2, "date" : "2014-03-01T08:00:00Z" },
  { "_id" : 2, "item" : "jkl", "price" : 20, "quantity" : 1, "date" : "2014-03-01T09:00:00Z" },
  { "_id" : 3, "item" : "xyz", "price" : 5, "quantity" : 10, "date" : "2014-03-15T09:00:00Z" },
  { "_id" : 4, "item" : "xyz", "price" : 5, "quantity" :  20, "date" : "2014-04-04T11:21:39.736Z"},
  { "_id" : 5, "item" : "abc", "price" : 10, "quantity" : 10, "date" : "2014-04-04T21:23:13.331Z" },
  { "_id" : 6, "item" : "def", "price" : 7.5, "quantity": 5, "date" : "2015-06-04T05:08:13Z" },
  { "_id" : 7, "item" : "def", "price" : 7.5, "quantity": 10, "date" : "2015-09-10T08:43:00Z" },
  { "_id" : 8, "item" : "abc", "price" : 10, "quantity" : 5, "date" : "2016-02-06T20:20:13Z" }
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 := conn.Database("test").Collection("sales").InsertMany(context.Background(), docs)
		if err2 != nil {
			t.Fatal(err2)
		}
		cus, err := conn.Database("test").Collection("sales").
			Aggregate(context.Background(), Pipeline().
				Group(Group().
					Field("_id", nil).Field("totalSaleAmount", bson.M{"$sum": bson.M{"$multiply": []string{"$price", "$quantity"}}}).
					Field("averageQuantity", bson.M{"$avg": "$quantity"}).
					Field("count", bson.M{"$sum": 1})).DS())
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

func Test_group5(t *testing.T) {
	t.Run("Test_group5", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("books").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
  { "_id" : 8751, "title" : "The Banquet", "author" : "Dante", "copies" : 2 },
  { "_id" : 8752, "title" : "Divine Comedy", "author" : "Dante", "copies" : 1 },
  { "_id" : 8645, "title" : "Eclogues", "author" : "Dante", "copies" : 2 },
  { "_id" : 7000, "title" : "The Odyssey", "author" : "Homer", "copies" : 10 },
  { "_id" : 7020, "title" : "Iliad", "author" : "Homer", "copies" : 10 }
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 := conn.Database("test").Collection("books").InsertMany(context.Background(), docs)
		if err2 != nil {
			t.Fatal(err2)
		}
		cus, err := conn.Database("test").Collection("books").
			Aggregate(context.Background(), Pipeline().
				Group(Group().
					FieldSimple("_id", "$author").
					Field("books", bson.M{"$push": "$title"})).DS())
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

func Test_group6(t *testing.T) {
	t.Run("Test_group6", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("books").Drop(context.Background())
		var docs []interface{}
		err := bson.UnmarshalExtJSON([]byte(`
 [
  { "_id" : 8751, "title" : "The Banquet", "author" : "Dante", "copies" : 2 },
  { "_id" : 8752, "title" : "Divine Comedy", "author" : "Dante", "copies" : 1 },
  { "_id" : 8645, "title" : "Eclogues", "author" : "Dante", "copies" : 2 },
  { "_id" : 7000, "title" : "The Odyssey", "author" : "Homer", "copies" : 10 },
  { "_id" : 7020, "title" : "Iliad", "author" : "Homer", "copies" : 10 }
 ] `), true, &docs)
		if err != nil {
			t.Fatal(err)
		}
		_, err2 := conn.Database("test").Collection("books").InsertMany(context.Background(), docs)
		if err2 != nil {
			t.Fatal(err2)
		}
		_addFields := AddFields()
		_addFields.Sum("totalCopies", "$books.copies")
		cus, err := conn.Database("test").Collection("books").
			Aggregate(context.Background(), Pipeline().AddFields(_addFields).
				Group(Group().
					FieldSimple("_id", "$author").
					Field("books", bson.M{"$push": "$$ROOT"})).DS())
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
