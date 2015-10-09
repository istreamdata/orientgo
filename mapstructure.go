package orient

import (
	"github.com/mitchellh/mapstructure"
	"reflect"
	"time"
)

// TagName is a name for a struct tag used for types conversion using reflect
var TagName = "mapstructure"

var mapDecoderHooks = []mapstructure.DecodeHookFunc{
	stringToTimeHookFunc,
	stringToByteSliceHookFunc,
	documentToMapHookFunc,
}

// RegisterMapDecoderHook allows to register additional hook for map decoder
func RegisterMapDecoderHook(hook mapstructure.DecodeHookFunc) {
	mapDecoderHooks = append(mapDecoderHooks, hook)
}

// NewMapDecoder returns decoder configured for decoding data into result with all registered hooks.
func newMapDecoder(result interface{}) (*mapstructure.Decoder, error) {
	return mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook: mapstructure.ComposeDecodeHookFunc(mapDecoderHooks...),
		Metadata:   nil,
		Result:     result,
		TagName:    TagName,
	})
}

var reflTimeType = reflect.TypeOf((*time.Time)(nil)).Elem()

// StringToTimeHookFunc returns a DecodeHookFunc that converts
// strings to time.Time using RFC3339Nano format.
func stringToTimeHookFunc(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
	if f.Kind() != reflect.String || t != reflTimeType {
		return data, nil
	}
	return time.Parse(time.RFC3339Nano, data.(string))
}

var reflByteSliceType = reflect.TypeOf(([]byte)(nil))

// StringToByteSliceHookFunc returns a DecodeHookFunc that converts strings to []byte.
func stringToByteSliceHookFunc(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
	if f.Kind() != reflect.String || t != reflByteSliceType {
		return data, nil
	}
	return []byte(data.(string)), nil
}

var reflDocumentType = reflect.TypeOf((*Document)(nil))

func documentToMapHookFunc(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
	if f != reflDocumentType {
		return data, nil
	}
	return data.(*Document).ToMap()
}
