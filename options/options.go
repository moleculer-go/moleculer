package options

import "github.com/moleculer-go/moleculer"

func Wrap(opts []moleculer.OptionsFunc) moleculer.OptionsFunc {
	return func(key string) interface{} {
		return Get(key, opts)
	}
}

func String(key string, opts []moleculer.OptionsFunc) string {
	result := Get(key, opts)
	if result != nil {
		return result.(string)
	}
	return ""
}

func Get(key string, opts []moleculer.OptionsFunc) interface{} {
	for _, opt := range opts {
		result := opt(key)
		if result != nil {
			return result
		}
	}
	return nil
}
