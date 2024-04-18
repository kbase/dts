package huma

import (
	"bytes"
	"path"
	"reflect"
)

type schemaField struct {
	Schema string `json:"$schema"`
}

// SchemaLinkTransformer is a transform that adds a `$schema` field to the
// response (if it is a struct) and a Link header pointing to the JSON
// Schema that describes the response structure. This is useful for clients
// to understand the structure of the response and enables things like
// as-you-type validation & completion of HTTP resources in editors like
// VSCode.
type SchemaLinkTransformer struct {
	prefix      string
	schemasPath string
	types       map[any]struct {
		t      reflect.Type
		fields []int
		ref    string
		header string
	}
}

// NewSchemaLinkTransformer creates a new transformer that will add a `$schema`
// field to the response (if it is a struct) and a Link header pointing to the
// JSON Schema that describes the response structure. This is useful for clients
// to understand the structure of the response and enables things like
// as-you-type validation & completion of HTTP resources in editors like
// VSCode.
func NewSchemaLinkTransformer(prefix, schemasPath string) *SchemaLinkTransformer {
	return &SchemaLinkTransformer{
		prefix:      prefix,
		schemasPath: schemasPath,
		types: map[any]struct {
			t      reflect.Type
			fields []int
			ref    string
			header string
		}{},
	}
}

func (t *SchemaLinkTransformer) addSchemaField(oapi *OpenAPI, content *MediaType) bool {
	if content == nil || content.Schema == nil || content.Schema.Ref == "" {
		return true
	}

	schema := oapi.Components.Schemas.SchemaFromRef(content.Schema.Ref)
	if schema.Type != TypeObject || (schema.Properties != nil && schema.Properties["$schema"] != nil) {
		return true
	}

	schema.Properties["$schema"] = &Schema{
		Type:        TypeString,
		Format:      "uri",
		Description: "A URL to the JSON Schema for this object.",
		ReadOnly:    true,
	}
	return false
}

// OnAddOperation is triggered whenever a new operation is added to the API,
// enabling this transformer to precompute & cache information about the
// response and schema.
func (t *SchemaLinkTransformer) OnAddOperation(oapi *OpenAPI, op *Operation) {
	// Update registry to be able to get the type from a schema ref.
	// Register the type in t.types with the generated ref
	if op.RequestBody != nil && op.RequestBody.Content != nil {
		for _, content := range op.RequestBody.Content {
			t.addSchemaField(oapi, content)
		}
	}

	registry := oapi.Components.Schemas
	for _, resp := range op.Responses {
		for _, content := range resp.Content {
			if t.addSchemaField(oapi, content) {
				continue
			}

			// Then, create the wrapper Go type that has the $schema field.
			typ := deref(registry.TypeFromRef(content.Schema.Ref))

			extra := schemaField{
				Schema: t.schemasPath + "/" + path.Base(content.Schema.Ref) + ".json",
			}

			fieldIndexes := []int{}
			fields := []reflect.StructField{
				reflect.TypeOf(extra).Field(0),
			}
			for i := 0; i < typ.NumField(); i++ {
				f := typ.Field(i)
				if f.IsExported() {
					fields = append(fields, f)

					// Track which fields are exported, so we can copy them over.
					// It's preferred to track/compute this here to avoid allocations in
					// the transform function from looking up what is exported.
					fieldIndexes = append(fieldIndexes, i)
				}
			}

			newType := reflect.StructOf(fields)
			info := t.types[typ]
			info.t = newType
			info.fields = fieldIndexes
			info.ref = extra.Schema
			info.header = "<" + extra.Schema + ">; rel=\"describedBy\""
			t.types[typ] = info
		}
	}
}

// Transform is called for every response to add the `$schema` field and/or
// the Link header pointing to the JSON Schema.
func (t *SchemaLinkTransformer) Transform(ctx Context, status string, v any) (any, error) {
	if v == nil {
		return v, nil
	}

	typ := deref(reflect.TypeOf(v))

	if typ.Kind() != reflect.Struct {
		return v, nil
	}

	info := t.types[typ]
	if info.t == nil {
		return v, nil
	}

	host := ctx.Host()
	ctx.AppendHeader("Link", info.header)

	tmp := reflect.New(info.t).Elem()

	// Set the `$schema` field.
	buf := bufPool.Get().(*bytes.Buffer)
	if len(host) >= 9 && host[:9] == "localhost" {
		buf.WriteString("http://")
	} else {
		buf.WriteString("https://")
	}
	buf.WriteString(host)
	buf.WriteString(info.ref)
	tmp.Field(0).SetString(buf.String())
	buf.Reset()
	bufPool.Put(buf)

	// Copy over all the exported fields.
	vv := reflect.Indirect(reflect.ValueOf(v))
	for i, j := range info.fields {
		// Field 0 is the $schema field, so we need to offset the index by one.
		// There might have been unexported fields in the struct declared in the schema,
		// but these have been filtered out when creating the new type.
		// Therefore, the field with index i on the new type maps to the field with index j
		// in the original struct.
		tmp.Field(i + 1).Set(vv.Field(j))
	}

	return tmp.Addr().Interface(), nil
}
