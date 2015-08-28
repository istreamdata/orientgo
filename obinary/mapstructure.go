package obinary

import (
	"github.com/mitchellh/mapstructure"
	"reflect"
	"time"
	//    "fmt"
)

func (dbc *Client) NewMapDecoder(result interface{}) (*mapstructure.Decoder, error) {
	return dbc.newMapDecoder(result, dbc.opts.MapDecoderHooks...)
}

func (dbc *Client) newMapDecoder(result interface{}, customHooks ...mapstructure.DecodeHookFunc) (*mapstructure.Decoder, error) {
	hooks := mapstructure.ComposeDecodeHookFunc(StringToTimeHookFunc(), StringToByteSliceHookFunc())
	for _, hook := range customHooks {
		hooks = mapstructure.ComposeDecodeHookFunc(hooks, hook)
	}
	config := &mapstructure.DecoderConfig{
		DecodeHook: hooks,
		Metadata:   nil,
		Result:     result,
	}
	return mapstructure.NewDecoder(config)
}

// StringToTimeHookFunc returns a DecodeHookFunc that converts
// strings to time.Time using RFC3339Nano format.
func StringToTimeHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}
		if t != reflect.TypeOf(time.Now()) {
			return data, nil
		}
		return time.Parse(time.RFC3339Nano, data.(string))
	}
}

// StringToByteSliceHookFunc returns a DecodeHookFunc that converts
// strings to []byte.
func StringToByteSliceHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}
		if t != reflect.TypeOf([]byte{}) {
			return data, nil
		}
		return []byte(data.(string)), nil
	}
}
