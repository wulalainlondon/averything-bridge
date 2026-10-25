package coordination

import (
	"reflect"
	"strings"
)

// StructSchema is the single source for strict tool schemas and generated
// cross-language collaboration contracts. Embedded Go structs are flattened.
func StructSchema(value any) map[string]any { return schemaType(reflect.TypeOf(value)) }
func schemaType(t reflect.Type) map[string]any {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		properties := map[string]any{}
		required := []string{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.PkgPath != "" {
				continue
			}
			if f.Anonymous {
				embedded := schemaType(f.Type)
				for name, spec := range embedded["properties"].(map[string]any) {
					properties[name] = spec
				}
				required = append(required, embedded["required"].([]string)...)
				continue
			}
			tag := f.Tag.Get("json")
			parts := strings.Split(tag, ",")
			name := parts[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = f.Name
			}
			properties[name] = schemaType(f.Type)
			if values := f.Tag.Get("enum"); values != "" {
				properties[name].(map[string]any)["enum"] = strings.Split(values, ",")
			}
			if !strings.Contains(tag, ",omitempty") {
				required = append(required, name)
			}
		}
		return map[string]any{"type": "object", "properties": properties, "required": required, "additionalProperties": false}
	case reflect.Slice, reflect.Array:
		return map[string]any{"type": "array", "items": schemaType(t.Elem())}
	case reflect.Map:
		return map[string]any{"type": "object", "additionalProperties": schemaType(t.Elem())}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Interface:
		return map[string]any{}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	default:
		return map[string]any{"type": "string"}
	}
}
