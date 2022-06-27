package mgqb

import "go.mongodb.org/mongo-driver/bson"

func findDWithE(d bson.D, field string) (bson.E, bool) {
	for _, e := range d {
		if e.Key == field {
			return e, true
		}
	}
	return bson.E{}, false
}
