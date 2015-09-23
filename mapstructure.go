package orient

import (
	"github.com/mitchellh/mapstructure"
	"reflect"
	"time"
)

var mapDecoderHooks = []mapstructure.DecodeHookFunc{
	stringToTimeHookFunc,
	stringToByteSliceHookFunc,
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
	})
}

// StringToTimeHookFunc returns a DecodeHookFunc that converts
// strings to time.Time using RFC3339Nano format.
func stringToTimeHookFunc(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
	if f.Kind() != reflect.String {
		return data, nil
	}
	if t != reflect.TypeOf(time.Time{}) {
		return data, nil
	}
	return time.Parse(time.RFC3339Nano, data.(string))
}

// StringToByteSliceHookFunc returns a DecodeHookFunc that converts strings to []byte.
func stringToByteSliceHookFunc(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
	if f.Kind() != reflect.String {
		return data, nil
	}
	if t != reflect.TypeOf([]byte{}) {
		return data, nil
	}
	return []byte(data.(string)), nil
}
