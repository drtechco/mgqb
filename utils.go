package mgqb

import "go.mongodb.org/mongo-driver/bson"

func findDWithE(d *bson.D, field string) (*bson.E, bool) {
	for i := 0; i < len(*d); i++ {
		e := (*d)[i]
		if e.Key == field {
			return &(*d)[i], true
		}
	}
	return &bson.E{}, false
}

func copyOf(y pipeline) pipeline {
	return y
}
