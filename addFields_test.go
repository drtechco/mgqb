package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
)

func Test_addFields1(t *testing.T) {
	t.Run("Test_addFields1", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("scores").Drop(context.Background())
		var docs []interface{}
		json.Unmarshal([]byte(`
[
  {
  "_id": 1,
  "student": "Maya",
  "homework": [ 10, 5, 10 ],
  "quiz": [ 10, 8 ],
  "extraCredit": 0
},
{
  "_id": 2,
  "student": "Ryan",
  "homework": [ 5, 6, 5 ],
  "quiz": [ 8, 8 ],
  "extraCredit": 8
}
]`), &docs)
		conn.Database("test").Collection("scores").InsertMany(context.Background(), docs)
		_addFields := AddFields()
		_addFields.
			Sum("totalHomework", "$homework").
			Sum("totalQuiz", "$quiz")
		_addFields2 := AddFields()
		_addFields2.Add("totalScore", []string{"$totalHomework", "$totalQuiz", "$extraCredit"})
		cus, err := conn.Database("test").Collection("scores").
			Aggregate(context.Background(), []bson.D{{{"$addFields", _addFields.ES()}}, {{"$addFields", _addFields2.ES()}}})
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

func Test_addFields2(t *testing.T) {
	t.Run("Test_addFields2", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("vehicles").Drop(context.Background())
		var docs []interface{}
		json.Unmarshal([]byte(`
[
{ "_id": 1, "type": "car", "specs": { "doors": 4, "wheels": 4 } },
{ "_id": 2, "type": "motorcycle", "specs": { "doors": 0, "wheels": 2 } },
{ "_id": 3, "type": "jet ski" }
]`), &docs)
		m := conn.Database("test").Collection("vehicles")
		m.InsertMany(context.Background(), docs)
		_addFields3 := AddFields()
		_addFields3.Const("specs.fuel_type", "unleaded")
		cus, err := m.Aggregate(context.Background(), []bson.M{{"$addFields": _addFields3.ES()}})

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

func Test_addFields3(t *testing.T) {
	t.Run("Test_addFields3", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("animals").Drop(context.Background())
		var docs []interface{}
		json.Unmarshal([]byte(`
[
{ "_id": 1, "dogs": 10, "cats": 15 }
]`), &docs)
		m := conn.Database("test").Collection("animals")
		m.InsertMany(context.Background(), docs)
		_addFields4 := AddFields()
		_addFields4.Const("cats", "20")
		cus, err := m.Aggregate(context.Background(), []bson.M{{"$addFields": _addFields4.ES()}})

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

func Test_addFields4(t *testing.T) {
	t.Run("Test_addFields4", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("fruit").Drop(context.Background())
		var docs []interface{}
		json.Unmarshal([]byte(`
[
{ "_id" : 1, "item" : "tangerine", "type" : "citrus" },
{ "_id" : 2, "item" : "lemon", "type" : "citrus" },
{ "_id" : 3, "item" : "grapefruit", "type" : "citrus" }
]`), &docs)
		m := conn.Database("test").Collection("fruit")
		m.InsertMany(context.Background(), docs)
		_addFields4 := AddFields()
		_addFields4.Replace("_id", "item")
		_addFields4.Const("item", "fruit")
		cus, err := m.Aggregate(context.Background(), []bson.M{{"$addFields": _addFields4.ES()}})
		fmt.Println(err)
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

func Test_addFields5(t *testing.T) {
	t.Run("Test_addFields5", func(t *testing.T) {
		conn := initTestConn()
		conn.Database("test").Collection("scores").Drop(context.Background())
		var docs []interface{}
		json.Unmarshal([]byte(`
[
{ "_id": 1, "student": "Maya", "homework": [ 10, 5, 10 ], "quiz": [ 10, 8 ], "extraCredit": 0 },
   { "_id": 2, "student": "Ryan", "homework": [ 5, 6, 5 ], "quiz": [ 8, 8 ], "extraCredit": 8 }
]`), &docs)
		m := conn.Database("test").Collection("scores")
		m.InsertMany(context.Background(), docs)
		_addFields5 := AddFields()
		_addFields5.ConcatArrays("homework", "$homework", []interface{}{7})
		Pipeline().SetMatch(Match("_id", WhereOperators.EQ, 1))
		cus, err := m.Aggregate(context.Background(), Pipeline().SetMatch(Match("_id", WhereOperators.EQ, 1)).AddFields(_addFields5).DS())
		fmt.Println(err)
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
