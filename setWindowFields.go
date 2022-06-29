package mgqb

import "go.mongodb.org/mongo-driver/bson"

type setWindowFields struct {
	partitionBy interface{}
	sortBy      bson.D
	output      []*addFields
	outputRaw   bson.D
}

func SetWindowFields() *setWindowFields {
	return &setWindowFields{
		sortBy:    make(bson.D, 0),
		output:    make([]*addFields, 0),
		outputRaw: make(bson.D, 0),
	}
}

func (s *setWindowFields) PartitionByField(field string) *setWindowFields {
	s.partitionBy = field
	return s
}

func (s *setWindowFields) PartitionBySimple(field string, collField string) *setWindowFields {
	s.partitionBy = bson.D{{field, collField}}
	return s
}

func (s *setWindowFields) PartitionBy(expression bson.D) *setWindowFields {
	s.partitionBy = expression
	return s
}

func (s *setWindowFields) SortDesc(field string) *setWindowFields {
	if ex, ok := findDWithE(&s.sortBy, field); ok {
		ex.Value = -1
	} else {
		s.sortBy = append(s.sortBy, bson.E{Key: field, Value: -1})
	}
	return s
}

func (s *setWindowFields) SortAsc(field string) *setWindowFields {
	if ex, ok := findDWithE(&s.sortBy, field); ok {
		ex.Value = 1
	} else {
		s.sortBy = append(s.sortBy, bson.E{Key: field, Value: 1})
	}
	return s
}

func (s *setWindowFields) AddOutput(field *addFields) *setWindowFields {
	s.output = append(s.output, field)
	return s
}

func (s *setWindowFields) D() bson.D {
	res := make(bson.D, 0)
	if s.partitionBy != nil {
		res = append(res, bson.E{Key: "partitionBy", Value: s.partitionBy})
	}
	if len(s.sortBy) > 0 {
		res = append(res, bson.E{Key: "sortBy", Value: s.sortBy})
	}
	if len(s.output) > 0 {
		for _, o := range s.output {
			for _, e := range o.ES() {
				s.outputRaw = append(s.outputRaw, e)
			}
		}
	}
	if len(s.outputRaw) > 0 {
		res = append(res, bson.E{Key: "output", Value: s.outputRaw})
	}
	return res
}
