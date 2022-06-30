package main

import "go.mongodb.org/mongo-driver/bson"

type lookup struct {
	from         string
	localField   string
	foreignField string
	let          bson.M
	pipelineRaw  []bson.D
	pipeline     *pipeline
	as           string
}

func Lookup() *lookup {
	return &lookup{
		let:         make(map[string]interface{}),
		pipelineRaw: make([]bson.D, 0),
	}
}

func (r *lookup) From(from string) *lookup {
	r.from = from
	return r
}

func (r *lookup) LocalField(field string) *lookup {
	r.localField = field
	return r
}

func (r *lookup) ForeignField(field string) *lookup {
	r.foreignField = field
	return r
}

func (r *lookup) Let(field string, as interface{}) *lookup {
	r.let[field] = as
	return r
}

func (r *lookup) PipelineRaw(pl []bson.D) *lookup {
	r.pipelineRaw = pl
	return r
}

func (r *lookup) Pipeline(pl *pipeline) *lookup {
	r.pipeline = pl
	return r
}

func (r *lookup) As(collection string) *lookup {
	r.as = collection
	return r
}

func (r *lookup) D() bson.D {
	d := make(bson.D, 0)
	if r.from != "" {
		d = append(d, bson.E{Key: "from", Value: r.from})
	}
	if r.localField != "" {
		d = append(d, bson.E{Key: "localField", Value: r.localField})
	}
	if r.foreignField != "" {
		d = append(d, bson.E{Key: "foreignField", Value: r.foreignField})
	}

	if len(r.let) > 0 {
		d = append(d, bson.E{Key: "let", Value: r.let})
	}
	if r.pipeline != nil {
		for _, v := range r.pipeline.DS() {
			r.pipelineRaw = append(r.pipelineRaw, v)
		}
	}
	if len(r.pipelineRaw) > 0 {
		d = append(d, bson.E{Key: "pipeline", Value: r.pipelineRaw})
	}
	d = append(d, bson.E{Key: "as", Value: r.as})
	return d
}
