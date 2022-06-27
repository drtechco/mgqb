package mgqb

import "go.mongodb.org/mongo-driver/bson"

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

func (s *addFields) WindowOperator(field string, operator operator, operatorPars string) {
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
}

func (s *addFields) setDoc(field string, documents ...interface{}) {
	if len(documents) > 0 {
		s.fields[field] = append(s.fields[field].(bson.D), bson.E{
			Key:   "window",
			Value: bson.E{Key: "documents", Value: documents},
		})
	}
}
