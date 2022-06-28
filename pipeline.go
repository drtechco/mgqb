package mgqb

import (
	"go.mongodb.org/mongo-driver/bson"
)

type pipeline struct {
	matchRaw        bson.D
	match           *match
	skip            bson.D
	limit           bson.D
	count           bson.D
	sort            bson.D
	unwind          bson.D
	project         bson.D
	lookupRaw       bson.M
	lookup          *lookup
	setWindowFields bson.D
	group           *group
	groupRaw        bson.D
}

func Pipeline() *pipeline {
	return &pipeline{
		sort:     make(bson.D, 0),
		project:  make(bson.D, 0),
		groupRaw: make(bson.D, 0),
	}
}

func (r *pipeline) DS() []bson.D {
	if r.group != nil {
		for _, d := range r.group.DS() {
			r.groupRaw = append(r.groupRaw, d)
		}
	}
	res := make([]bson.D, 0)
	if len(r.groupRaw) > 0 {
		res = append(res, bson.D{{"$group", r.groupRaw}})
	}
	if BSON_LOGGER {
		d, e := bson.MarshalExtJSON(bson.D{{"pipeline", res}}, true, false)
		if e != nil {
			Error_Log(e)
		} else {
			Trace_Log(string(d))
		}

	}
	return res
}

func (r *pipeline) M() []bson.M {
	return nil
}

//func (r *pipeline) GetMatch() *match {
//	if r.match == nil {
//		r.match = Ma()
//	}
//	return r.match
//}

func (r *pipeline) SetMatch(b *match) *pipeline {
	r.match = b
	return r
}

func (r *pipeline) Skip(skip int64) *pipeline {
	r.skip = bson.D{{"$skip", skip}}
	return r
}

func (r *pipeline) Limit(limit int64) *pipeline {
	r.limit = bson.D{{"$limit", limit}}
	return r
}

func (r *pipeline) Count(field string) *pipeline {
	if field == "" {
		field = "totalDocuments"
	}
	r.count = bson.D{{"$count", field}}
	return r
}

func (r *pipeline) SortDesc(field string) *pipeline {

	r.sort = append(r.sort, bson.E{Key: field, Value: -1})
	return r
}

func (r *pipeline) SortAsc(field string) *pipeline {
	r.sort = append(r.sort, bson.E{Key: field, Value: 1})
	return r
}

func (r *pipeline) Unwind(path string, includeArrayIndex string, preserveNullAndEmptyArrays bool) *pipeline {
	r.unwind = bson.D{{"$unwind", bson.D{
		{"path", path},
		{"includeArrayIndex", includeArrayIndex},
		{"preserveNullAndEmptyArrays", preserveNullAndEmptyArrays},
	}}}
	return r
}

func (r *pipeline) Project1(field string) *pipeline {
	r.project = append(r.project, bson.E{Key: field, Value: 1})
	return r
}

func (r *pipeline) Project0(field string) *pipeline {
	r.project = append(r.project, bson.E{Key: field, Value: 0})
	return r
}

func (r *pipeline) ProjectSub(field string, e bson.E) *pipeline {
	r.project = append(r.project, bson.E{Key: field, Value: e})
	return r
}

func (r *pipeline) LookupRaw(l bson.M) *pipeline {
	r.lookupRaw = l
	return r
}
func (r *pipeline) Lookup(l *lookup) *pipeline {
	r.lookup = l
	return r
}

func (r *pipeline) GroupRaw(g bson.D) *pipeline {
	r.groupRaw = g
	return r
}

func (r *pipeline) Group(g *group) *pipeline {
	r.group = g
	return r
}
