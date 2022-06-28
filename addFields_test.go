package mgqb

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
		conn.Database("test1").Collection("scores").Drop(context.Background())
		var docs []interface{}
		json.Unmarshal([]byte(`
		[
		{
		  _id: 1,
		  student: "Maya",
		  homework: [ 10, 5, 10 ],
		  quiz: [ 10, 8 ],
		  extraCredit: 0
		},
		{
		  _id: 2,
		  student: "Ryan",
		  homework: [ 5, 6, 5 ],
		  quiz: [ 8, 8 ],
		  extraCredit: 8
		}
		]`), &docs)
		conn.Database("test1").Collection("scores").InsertMany(context.Background(), docs)
		_addFields := AddFields()
		_addFields.
			Sum("totalHomework", "$homework").
			Sum("totalQuiz", "$quiz")

		_addFields2 := AddFields()
		_addFields2.Add("totalScore", []string{"$totalHomework", "$totalQuiz", "$extraCredit"})
		cus, err := conn.Database("test1").Collection("scores").
			Aggregate(context.Background(), []bson.D{bson.D{{"$addFields", _addFields.ES()}}, {{"$addFields", _addFields2.ES()}}})
		if err != nil {
			t.Fatal(err)
		}
		var res bson.A
		err = cus.All(context.Background(), &res)
		fmt.Println(res)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 2 {
			t.Fatal(errors.New("res length not 2"))
		}
		fmt.Println(res)
	})

}
