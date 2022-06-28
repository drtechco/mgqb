package mgqb

import (
	"errors"
	"go.mongodb.org/mongo-driver/bson"
)

type addFields struct {
	fields map[string]interface{}
}

func AddFields() *addFields {
	return &addFields{
		fields: make(map[string]interface{}),
	}
}

func (s *addFields) Set(field string, expression bson.D) *addFields {
	s.fields[field] = expression
	return s
}

func (s *addFields) Sum(field string, collField string, documents ...interface{}) *addFields {
	s.fields[field] = bson.D{{Key: "$sum", Value: collField}}
	s.setDoc(field, documents)
	return s
}

func (s *addFields) Add(field string, collFields []string, documents ...interface{}) *addFields {
	v := make(bson.A, 0)
	for _, collField := range collFields {
		v = append(v, collField)
	}
	s.fields[field] = bson.D{{Key: "$add", Value: v}}
	s.setDoc(field, documents)
	return s
}

func (s *addFields) Avg(field string, collField string, documents ...interface{}) *addFields {
	s.fields[field] = bson.D{{Key: "$avg", Value: collField}}
	s.setDoc(field, documents)
	return s
}

func (s *addFields) Const(field string, value interface{}, documents ...interface{}) *addFields {
	s.fields[field] = value
	s.setDoc(field, documents)
	return s
}

func (s *addFields) Replace(field string, collField string, documents ...interface{}) *addFields {
	s.fields[field] = "$" + collField
	s.setDoc(field, documents)
	return s
}

func (s *addFields) WindowOperator(field string, operator windowOperator, operatorPars string) *addFields {
	if d, ok := s.fields[field].(bson.D); ok {
		if e, ok := findDWithE(d, string(operator)); ok {
			e.Value = operatorPars
		} else {
			s.fields[field] = append(s.fields[field].(bson.D), bson.E{
				Key:   string(operator),
				Value: operatorPars,
			})
		}
	}
	return s
}

func (s *addFields) Rang(field string, rangeVal ...interface{}) *addFields {
	s.checkField(field, "window", bson.D{{"range", rangeVal}}, func(w, a bson.D) interface{} {
		if a, ok := findDWithE(w, "range"); ok {
			if a1, ok := a.Value.([]interface{}); ok {
				return append(a1, rangeVal...)
			} else {
				panic(errors.New("range value is not slice"))
			}
		} else {
			return a
		}
	})
	return s
}

func (s *addFields) Unit(field string, unit interface{}) *addFields {
	s.checkField(field, "window", bson.D{{"unit", unit}}, func(e, a bson.D) interface{} {
		return unit
	})
	return s
}

func (s addFields) checkField(field string, subField string, addVal bson.D, setFun func(exists, a bson.D) interface{}) {
	if f, ok := s.fields[field]; ok {
		if d, ok := f.(bson.D); ok {
			if wi, ok := findDWithE(d, subField); ok {
				if w, ok := wi.Value.(bson.D); ok {
					wi.Value = setFun(w, addVal)
				} else {
					panic(errors.New("documents is not bson.E"))
				}
			} else {
				s.fields[field] = append(d, bson.E{
					Key:   subField,
					Value: addVal,
				})
			}
		} else {
			panic(errors.New("field is not bson.D"))
		}
	} else {
		panic(errors.New("field is nil"))
	}
}

func (s *addFields) setDoc(field string, documents []interface{}) {
	if documents != nil && len(documents) > 0 {
		s.checkField(field, "window", bson.D{{"documents", documents}}, func(w, a bson.D) interface{} {
			if a, ok := findDWithE(w, "range"); ok {
				if a1, ok := a.Value.([]interface{}); ok {
					return append(a1, documents...)
				} else {
					panic(errors.New("documents value is not slice"))
				}
			} else {
				return a
			}
		})
	}
}

func (s *addFields) ES() []bson.E {
	res := make([]bson.E, 0)
	for key, f := range s.fields {
		res = append(res, bson.E{
			Key:   key,
			Value: f,
		})
	}
	return res
}

func (s *addFields) D() bson.D {
	return s.ES()
}
