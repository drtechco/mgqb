package mgqb

import (
	"go.mongodb.org/mongo-driver/bson"
)

type pipeline struct {
	addFields          []*addFields
	addFieldsRaw       []bson.D
	matchRaw           bson.D
	match              *match
	skip               bson.D
	limit              bson.D
	count              bson.D
	sort               bson.D
	unwind             bson.D
	project            bson.D
	lookupRaw          bson.D
	lookup             *lookup
	setWindowFields    *setWindowFields
	setWindowFieldsRaw bson.D
	group              *group
	groupRaw           bson.D
	replaceRoot        interface{}
	unset              []string
	sortByCount        string
}

func Pipeline() *pipeline {
	return &pipeline{
		sort:         make(bson.D, 0),
		project:      make(bson.D, 0),
		groupRaw:     make(bson.D, 0),
		addFields:    make([]*addFields, 0),
		addFieldsRaw: make([]bson.D, 0),
	}
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

func (r *pipeline) LookupRaw(l bson.D) *pipeline {
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

func (r *pipeline) AddFields(addField *addFields) *pipeline {
	r.addFields = append(r.addFields, addField)
	return r
}

func (r *pipeline) AddFieldsRaw(addField bson.D) *pipeline {
	r.addFieldsRaw = append(r.addFieldsRaw, addField)
	return r
}

func (r *pipeline) ReplaceRoot(v interface{}) *pipeline {
	r.replaceRoot = v
	return r
}

func (r *pipeline) AddUnset(fields ...string) *pipeline {
	r.unset = append(r.unset, fields...)
	return r
}

func (r *pipeline) SetWindowFields(swf *setWindowFields) *pipeline {
	r.setWindowFields = swf
	return r
}

func (r *pipeline) SetWindowFieldsRaw(swf bson.D) *pipeline {
	r.setWindowFieldsRaw = swf
	return r
}

func (r *pipeline) SortByCount(field string) {
	r.sortByCount = field
}

func (r *pipeline) DS() []bson.D {
	res := make([]bson.D, 0)

	if r.match != nil {
		r.matchRaw = r.match.D()
	}

	if r.matchRaw != nil {
		res = append(res, bson.D{{"$match", r.matchRaw}})
	}

	if r.count != nil {
		res = append(res, r.count)
	}

	if r.group != nil {
		for _, d := range r.group.DS() {
			r.groupRaw = append(r.groupRaw, d)
		}
	}
	if len(r.groupRaw) > 0 {
		res = append(res, bson.D{{"$group", r.groupRaw}})
	}

	if r.limit != nil {
		res = append(res, r.limit)
	}

	if r.lookup != nil {
		r.lookupRaw = r.lookup.D()
	}

	if r.lookupRaw != nil {
		res = append(res, bson.D{{"$lookup", r.lookupRaw}})
	}

	if len(r.project) > 0 {
		res = append(res, bson.D{{"$project", r.project}})
	}

	if r.replaceRoot != nil {
		res = append(res, bson.D{{"$replaceRoot", bson.D{{"newRoot", r.replaceRoot}}}})
	}

	if r.setWindowFields != nil {
		r.setWindowFieldsRaw = r.setWindowFields.D()
	}

	if r.setWindowFieldsRaw != nil {
		res = append(res, bson.D{{"$setWindowFields", r.setWindowFieldsRaw}})
	}

	if r.skip != nil {
		res = append(res, r.skip)
	}

	if len(r.sort) > 0 {
		res = append(res, bson.D{{"$sort", r.sort}})
	}

	if r.sortByCount != "" {
		res = append(res, bson.D{{"$sortByCount", r.sortByCount}})
	}

	if len(r.unset) > 0 {
		res = append(res, bson.D{{"$unset", r.unset}})
	}

	if r.unwind != nil {
		res = append(res, r.unwind)
	}

	if len(r.addFields) > 0 {
		for _, field := range r.addFields {
			r.addFieldsRaw = append(r.addFieldsRaw, field.D())
		}
	}
	if len(r.addFieldsRaw) > 0 {
		for _, d := range r.addFieldsRaw {
			res = append(res, bson.D{{"$addFields", d}})
		}
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
