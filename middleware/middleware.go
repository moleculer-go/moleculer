package middleware

import (
	"github.com/moleculer-go/moleculer"
	log "github.com/sirupsen/logrus"
)

type Dispatch struct {
	handlers map[string][]moleculer.MiddlewareHandler
	logger   *log.Entry
}

func Dispatcher(logger *log.Entry) *Dispatch {
	handlers := make(map[string][]moleculer.MiddlewareHandler)
	return &Dispatch{handlers, logger}
}

var validHandlers = []string{"brokerConfig", "brokerStoping", "brokerStoped", "brokerStarting", "brokerStarted", "serviceStoping", "serviceStoped", "serviceStarting", "serviceStarted", "beforeLocalAction", "afterLocalAction", "beforeRemoteAction", "afterRemoteAction"}

// validHandler check if the name of handlers midlewares are tryignt o register exists!
func (dispatch *Dispatch) validHandler(name string) bool {
	for _, item := range validHandlers {
		if name == item {
			return true
		}
	}
	return false
}

func (dispatch *Dispatch) Add(mwares moleculer.Middlewares) {
	for name, handler := range mwares {
		if dispatch.validHandler(name) {
			dispatch.handlers[name] = append(dispatch.handlers[name], handler)
		}
	}
}

func (dispatch *Dispatch) Has(name string) bool {
	items, exists := dispatch.handlers[name]
	return exists && len(items) > 0
}

// nextHandler return a next function that invoke next midlewares until the end of the stack.
func nextHandler(handlers *[]moleculer.MiddlewareHandler, index *int, params interface{}, resultChanel chan interface{}) func(result ...interface{}) {
	return func(newResult ...interface{}) {
		newIndex := (*index) + 1
		index = &newIndex
		if newIndex < len((*handlers)) {
			var value interface{}
			if newResult != nil && len(newResult) > 0 {
				value = newResult[0]
			} else {
				value = params
			}
			(*handlers)[newIndex](value, nextHandler(handlers, index, value, resultChanel))
		} else {
			if newResult != nil && len(newResult) > 0 {
				resultChanel <- newResult[0]
			} else {
				resultChanel <- params
			}
		}
	}
}

// CallHandlers invoke handlers that subscribe to this topic.
func (dispatch *Dispatch) CallHandlers(name string, params interface{}) interface{} {
	handlers := dispatch.handlers[name]
	if len(handlers) > 0 {
		dispatch.logger.Info("hanve Handlers len(handlers) ", len(handlers))
		result := make(chan interface{})
		index := 0
		go func() {
			//starts the chain reaction ...
			handlers[0](params, nextHandler(&handlers, &index, params, result))
		}()
		return <-result
	} else {
		dispatch.logger.Debug("No Handlers found for -> ", name)
	}
	return params
}
