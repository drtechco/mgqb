package mgqb

import "go.mongodb.org/mongo-driver/bson"
type setWindowFields struct {
	partitionBy string
	sortBy      bson.M
}

func SetWindowFields() *setWindowFields {
	return &setWindowFields{
		sortBy: make(bson.M),
	}
}

func (s *setWindowFields) PartitionBy(expression string) *setWindowFields {
	s.partitionBy = expression
	return s
}

func (s *setWindowFields) SortDesc(field string) *setWindowFields {
	s.sortBy[field] = -1
	return s
}

func (s *setWindowFields) SortAsc(field string) *setWindowFields {
	s.sortBy[field] = 1
	return s
}

func (s *setWindowFields) Output() {
	
}
