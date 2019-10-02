package service

import (
	"errors"
	"fmt"
	"os"
	"reflect"

	"github.com/moleculer-go/cupaloy/v2"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var logger = log.WithField("Unit Test", true)
var snap = cupaloy.New(cupaloy.FailOnUpdate(os.Getenv("UPDATE_SNAPSHOTS") == "true"))

var _ = Describe("Service", func() {

	It("isInternalEvent() should reconized internal events", func() {

		Expect(isInternalEvent(Event{
			name:        "$services.changed",
			serviceName: "source service",
		})).Should(BeTrue())

		Expect(isInternalEvent(Event{
			name:        "someservice.act$on",
			serviceName: "source service",
		})).Should(BeFalse())

	})

	rotateFunc := func(ctx moleculer.Context, params moleculer.Payload) interface{} {
		return "Hellow Leleu ;) I'm rotating ..."
	}

	rotatesEventFunc := func(ctx moleculer.Context, params moleculer.Payload) {
		fmt.Println("spining spining spining")
	}

	mixinTideFunc := func(ctx moleculer.Context, params moleculer.Payload) interface{} {
		return "tide influence in the oceans"
	}

	mixinRotatesFunc := func(ctx moleculer.Context, params moleculer.Payload) {
		fmt.Println("update tide in relation to the moon")
	}

	mixinMoonIsCloseFunc := func(ctx moleculer.Context, params moleculer.Payload) {
		fmt.Println("rise the tide !")
	}

	moonMixIn := moleculer.Mixin{
		Name: "moon",
		Settings: map[string]interface{}{
			"craters": true,
		},
		Metadata: map[string]interface{}{
			"resolution": "high",
		},
		Hooks: map[string]interface{}{
			"earth": "true",
		},
		Actions: []moleculer.Action{
			moleculer.Action{
				Name:    "tide",
				Handler: mixinTideFunc,
			},
		},
		Events: []moleculer.Event{
			moleculer.Event{
				Name:    "earth.rotates",
				Handler: mixinRotatesFunc,
			},
			moleculer.Event{
				Name:    "moon.isClose",
				Handler: mixinMoonIsCloseFunc,
			},
		},
	}

	serviceSchema := moleculer.ServiceSchema{
		Name:    "earth",
		Version: "0.2",
		Settings: map[string]interface{}{
			"dinosauros": true,
		},
		Metadata: map[string]interface{}{
			"star-system": "sun",
		},
		Hooks: map[string]interface{}{
			"solar-system": "true",
		},
		Mixins: []moleculer.Mixin{moonMixIn},
		Actions: []moleculer.Action{
			moleculer.Action{
				Name:    "rotate",
				Handler: rotateFunc,
			},
		},
		Events: []moleculer.Event{
			moleculer.Event{
				Name:    "earth.rotates",
				Handler: rotatesEventFunc,
			},
		},
	}

	It("Should merge and overwrite existing actions", func() {
		merged := extendActions(serviceSchema, &moonMixIn)
		actions := merged.Actions
		Expect(actions).Should(HaveLen(2))
		Expect(snap.Snapshot(actions)).Should(Succeed())
	})

	It("Should merge and overwrite existing events", func() {
		merged := concatenateEvents(serviceSchema, &moonMixIn)
		Expect(merged.Events).Should(HaveLen(2))
		Expect(snap.Snapshot(merged.Events)).Should(Succeed())
	})

	It("Should merge and overwrite existing settings", func() {

		mergedServiceSettings := extendSettings(serviceSchema, &moonMixIn)
		Expect(mergedServiceSettings.Settings).Should(Equal(map[string]interface{}{
			"dinosauros": true,
			"craters":    true,
		},
		))
	})

	It("Should merge and overwrite existing metadata", func() {

		mergedServiceMetadata := extendMetadata(serviceSchema, &moonMixIn)
		Expect(mergedServiceMetadata.Metadata).Should(Equal(map[string]interface{}{
			"star-system": "sun",
			"resolution":  "high",
		},
		))
	})

	It("Should merge and overwrite existing hooks", func() {
		mergedServiceHooks := extendHooks(serviceSchema, &moonMixIn)
		Expect(mergedServiceHooks.Hooks).Should(Equal(map[string]interface{}{
			"solar-system": "true",
			"earth":        "true",
		},
		))
	})

	It("Should apply mixins collectively", func() {
		merged := applyMixins(serviceSchema)
		Expect(merged.Actions).Should(HaveLen(2))
		Expect(merged.Events).Should(HaveLen(2))
		Expect(snap.Snapshot(merged)).Should(Succeed())
	})

	It("actionName() should return the method name in camel case", func() {
		Expect(actionName("SetLogRate")).Should(Equal("setLogRate"))
		Expect(actionName("someName")).Should(Equal("someName"))
		Expect(actionName("Name")).Should(Equal("name"))
	})

	It("handlerTemplate() should find a template for a complete action", func() {
		v := reflect.ValueOf(&ServiceTestObj{})
		m := v.MethodByName("CompleteAction")
		Expect(m.IsValid()).Should(BeTrue())
		ah := handlerTemplate(m)
		Expect(ah).ShouldNot(BeNil())
		Expect(ah(nil, nil)).Should(Equal("CompleteAction Invoked!"))

		m = v.MethodByName("NonPointerCompleteAction")
		Expect(m.IsValid()).Should(BeTrue())
		ah = handlerTemplate(m)
		Expect(ah).ShouldNot(BeNil())
		Expect(ah(nil, nil)).Should(Equal("NonPointerCompleteAction Invoked!"))
	})

	It("handlerTemplate() should find a template for a complete action no return", func() {
		v := reflect.ValueOf(&ServiceTestObj{})
		m := v.MethodByName("CompleteActionNoReturn")
		Expect(m.IsValid()).Should(BeTrue())
		ah := handlerTemplate(m)
		Expect(ah).ShouldNot(BeNil())
		Expect(ah(nil, nil)).Should(BeNil())
	})

	It("handlerTemplate() should find a template for an action with just context args", func() {
		v := reflect.ValueOf(&ServiceTestObj{})
		m := v.MethodByName("JustContext")
		Expect(m.IsValid()).Should(BeTrue())
		ah := handlerTemplate(m)
		Expect(ah).ShouldNot(BeNil())
		Expect(ah(nil, nil)).Should(Equal("JustContext Invoked!"))
	})

	It("handlerTemplate() should find a template for an action with just context args and no return", func() {
		v := reflect.ValueOf(&ServiceTestObj{})
		m := v.MethodByName("JustContextNoReturn")
		Expect(m.IsValid()).Should(BeTrue())
		ah := handlerTemplate(m)
		Expect(ah).ShouldNot(BeNil())
		Expect(ah(nil, nil)).Should(BeNil())
	})

	It("handlerTemplate() should find a template for an action with just params args", func() {
		v := reflect.ValueOf(&ServiceTestObj{})
		m := v.MethodByName("JustParams")
		Expect(m.IsValid()).Should(BeTrue())
		ah := handlerTemplate(m)
		Expect(ah).ShouldNot(BeNil())
		Expect(ah(nil, nil)).Should(Equal("JustParams Invoked!"))
	})

	It("handlerTemplate() should find a template for an action with just params args and no return", func() {
		v := reflect.ValueOf(&ServiceTestObj{})
		m := v.MethodByName("JustParamsNoReturn")
		Expect(m.IsValid()).Should(BeTrue())
		ah := handlerTemplate(m)
		Expect(ah).ShouldNot(BeNil())
		Expect(ah(nil, nil)).Should(BeNil())
	})

	It("handlerTemplate() should find a template for an action with no args", func() {
		v := reflect.ValueOf(&ServiceTestObj{})
		m := v.MethodByName("NoArgs")
		Expect(m.IsValid()).Should(BeTrue())
		ah := handlerTemplate(m)
		Expect(ah).ShouldNot(BeNil())
		Expect(ah(nil, nil)).Should(Equal("NoArgs Invoked!"))
	})

	It("handlerTemplate() should find a template for an action with just params args and no return", func() {
		v := reflect.ValueOf(&ServiceTestObj{})
		m := v.MethodByName("NoArgsNoReturn")
		Expect(m.IsValid()).Should(BeTrue())
		ah := handlerTemplate(m)
		Expect(ah).ShouldNot(BeNil())
		Expect(ah(nil, nil)).Should(BeNil())
	})

	It("getName() should invoke the Name() method on pointer and non pointer methods", func() {
		name, err := getName(NoPointers{})
		Expect(err).Should(BeNil())
		Expect(name).Should(Equal("My Name"))

		name, err = getName(&NoPointers{})
		Expect(err).Should(BeNil())
		Expect(name).Should(Equal("My Name"))

		name, err = getName(&ServiceTestObj{})
		Expect(err).Should(BeNil())
		Expect(name).Should(Equal("My Name"))

	})

	It("getParamTypes() should return a list of ordered parameter names", func() {
		v := reflect.ValueOf(&ServiceTestObj{})
		m := v.MethodByName("Add")
		Expect(m.IsValid()).Should(BeTrue())
		ptypes := getParamTypes(m)
		Expect(ptypes).ShouldNot(BeNil())
		Expect(len(ptypes)).Should(Equal(2))
		Expect(ptypes).Should(Equal([]string{"int", "int"}))
	})

	It("buildArgs() should return a list of refelct.Value arguments", func() {
		ptypes := []string{"int", "string", "int"}
		list := []moleculer.Payload{
			payload.New(1),
			payload.New("hi"),
			payload.New(42),
		}
		r := buildArgs(ptypes, payload.New(list))
		Expect(len(r)).Should(Equal(3))
		Expect(r[0].Interface()).Should(Equal(1))
		Expect(r[1].Interface()).Should(Equal("hi"))
		Expect(r[2].Interface()).Should(Equal(42))
	})

	It("validateArgs() should return error when param is invalid", func() {
		ptypes := []string{"int", "string", "int"}
		matchError1 := Equal("This action requires arguments to be sent in an array. #3 arguments - types: [int string int]")
		matchError2 := Equal("This action requires #3 arguments - types: [int string int]")

		Expect(validateArgs(ptypes, payload.New(1)).Error()).Should(matchError1)
		Expect(validateArgs(ptypes, payload.New("a")).Error()).Should(matchError1)
		Expect(validateArgs(ptypes, payload.New(nil)).Error()).Should(matchError1)
		Expect(validateArgs(ptypes, payload.New([]interface{}{})).Error()).Should(matchError2)
		Expect(validateArgs(ptypes, payload.New([]interface{}{1})).Error()).Should(matchError2)
		Expect(validateArgs(ptypes, payload.New([]interface{}{1, 2})).Error()).Should(matchError2)
	})

	It("validateArgs() should return nil when param is valid", func() {
		ptypes := []string{"int", "string", "int"}
		Expect(validateArgs(ptypes, payload.New([]interface{}{1, "a", 2}))).Should(BeNil())
	})

	It("checkReturn() should return nil when its args is nil", func() {
		var in []reflect.Value = nil
		Expect(checkReturn(in)).Should(BeNil())
	})

	It("checkReturn() should return nil when its args is empty", func() {
		in := []reflect.Value{}
		Expect(checkReturn(in)).Should(BeNil())
	})

	It("checkReturn() should return the only value in the args", func() {
		in := []reflect.Value{reflect.ValueOf("hellow")}
		Expect(checkReturn(in)).Should(Equal("hellow"))
	})

	It("checkReturn() should return the error, when the last params is an error not nil", func() {
		err := errors.New("some error...")
		//second is error
		Expect(checkReturn([]reflect.Value{reflect.ValueOf(nil), reflect.ValueOf(err)})).Should(Equal(err))
		//third is error
		Expect(checkReturn([]reflect.Value{reflect.ValueOf(nil), reflect.ValueOf(nil), reflect.ValueOf(err)})).Should(Equal(err))
		//forth is error
		Expect(checkReturn([]reflect.Value{reflect.ValueOf(nil), reflect.ValueOf(nil), reflect.ValueOf(nil), reflect.ValueOf(err)})).Should(Equal(err))
	})

	It("checkReturn() should return convert multiple items returned into an array", func() {
		in := []reflect.Value{reflect.ValueOf("hellow"), reflect.ValueOf("worderfull"), reflect.ValueOf("world")}
		r := checkReturn(in)
		Expect(r).ShouldNot(BeNil())
		Expect(r.(moleculer.Payload).StringArray()).Should(Equal([]string{"hellow", "worderfull", "world"}))
	})

	It("variableArgsHandler() should create an action handler that can deal with variable arguments.", func() {
		svc := &ServiceTestObj{}
		v := reflect.ValueOf(svc)
		m := v.MethodByName("Add")
		ah := variableArgsHandler(m)
		p := payload.New([]int{10, 23})
		r := ah(nil, p)
		Expect(r).Should(Equal(33))

		m = v.MethodByName("NoArgsNoReturn")
		ah = variableArgsHandler(m)
		r = ah(nil, payload.New(nil))
		Expect(r).Should(BeNil())
		Expect(svc.NoArgsNoReturnCalled).Should(BeTrue())

		m = v.MethodByName("NoArgs")
		ah = variableArgsHandler(m)
		r = ah(nil, payload.New(nil))
		Expect(r).Should(Equal("NoArgs Invoked!"))

		m = v.MethodByName("JustParamsNoReturn")
		ah = variableArgsHandler(m)
		r = ah(nil, payload.Empty())
		Expect(r).Should(BeNil())
		Expect(svc.JustParamsNoReturnCalled).Should(BeTrue())
	})

	It("wrapAction() should create an action with the given method.", func() {
		svc := &ServiceTestObj{}
		v := reflect.ValueOf(svc)
		mv := v.MethodByName("Add")
		m, _ := v.Type().MethodByName("Add")

		a := wrapAction(m, mv)
		Expect(a.Name).Should(Equal("add"))
		p := payload.New([]int{10, 23})
		r := a.Handler(nil, p)
		Expect(r).Should(Equal(33))

		mv = v.MethodByName("JustParamsNoReturn")
		m, _ = v.Type().MethodByName("JustParamsNoReturn")
		a = wrapAction(m, mv)
		Expect(a.Name).Should(Equal("justParamsNoReturn"))
	})

	It("extractActions() should extract all actions for an object.", func() {
		acts := extractActions(&ServiceTestObj{})
		Expect(len(acts)).Should(Equal(10))
		Expect(snap.Snapshot(acts)).Should(Succeed())
	})

	It("validActionName() should check if a method name is valid for Action.", func() {
		Expect(validActionName("Name")).Should(BeFalse())
		Expect(validActionName("Version")).Should(BeFalse())
		Expect(validActionName("Dependencies")).Should(BeFalse())
		Expect(validActionName("Settings")).Should(BeFalse())
		Expect(validActionName("Metadata")).Should(BeFalse())
		Expect(validActionName("Mixins")).Should(BeFalse())
		Expect(validActionName("Events")).Should(BeFalse())

		Expect(validActionName("ChangeName")).Should(BeTrue())
		Expect(validActionName("JustContext")).Should(BeTrue())
		Expect(validActionName("SomeName")).Should(BeTrue())
		Expect(validActionName("Ahah")).Should(BeTrue())

	})

	It("extractCreated() should return func with params and wrap func with no params.", func() {
		svc := &ServiceTestObj{}
		fn := extractCreated(svc)
		Expect(fn).ShouldNot(BeNil())
		fn(moleculer.ServiceSchema{}, nil)
		Expect(svc.CreatedCalled).Should(BeTrue())

		svc2 := &ServiceTestNoParams{}
		fn = extractCreated(svc2)
		Expect(fn).ShouldNot(BeNil())
		fn(moleculer.ServiceSchema{}, nil)
		Expect(svc2.CreatedCalled).Should(BeTrue())

		svc3 := &EmptyObj{}
		fn = extractCreated(svc3)
		Expect(fn).Should(BeNil())
	})

	It("extractStarted() should return func with params and wrap func with no params.", func() {
		svc := &ServiceTestObj{}
		fn := extractStarted(svc)
		Expect(fn).ShouldNot(BeNil())
		fn(nil, moleculer.ServiceSchema{})
		Expect(svc.StartedCalled).Should(BeTrue())

		svc2 := &ServiceTestNoParams{}
		fn = extractStarted(svc2)
		Expect(fn).ShouldNot(BeNil())
		fn(nil, moleculer.ServiceSchema{})
		Expect(svc2.StartedCalled).Should(BeTrue())

		svc3 := &EmptyObj{}
		fn = extractStarted(svc3)
		Expect(fn).Should(BeNil())
	})

	It("extractStopped() should return func with params and wrap func with no params.", func() {
		svc := &ServiceTestObj{}
		fn := extractStopped(svc)
		Expect(fn).ShouldNot(BeNil())
		fn(nil, moleculer.ServiceSchema{})
		Expect(svc.StoppedCalled).Should(BeTrue())

		svc2 := &ServiceTestNoParams{}
		fn = extractStopped(svc2)
		Expect(fn).ShouldNot(BeNil())
		fn(nil, moleculer.ServiceSchema{})
		Expect(svc2.StoppedCalled).Should(BeTrue())

		svc3 := &EmptyObj{}
		fn = extractStopped(svc3)
		Expect(fn).Should(BeNil())
	})

	It("ParseVersion()", func() {
		intVer := 1
		Expect(ParseVersion(intVer)).Should(Equal("1"))

		floatVer := 1.1
		Expect(ParseVersion(floatVer)).Should(Equal("1.1"))

		strVer := "v0.1.1"
		Expect(ParseVersion(strVer)).Should(Equal("v0.1.1"))
	})

})

type NoPointers struct {
}

func (svc NoPointers) Name() string {
	return "My Name"
}

type ServiceTestObj struct {
	NoArgsNoReturnCalled     bool
	JustParamsNoReturnCalled bool
	CreatedCalled            bool
	StartedCalled            bool
	StoppedCalled            bool
}

func (svc *ServiceTestObj) Created(moleculer.ServiceSchema, *log.Entry) {
	svc.CreatedCalled = true
}

func (svc *ServiceTestObj) Started(moleculer.BrokerContext, moleculer.ServiceSchema) {
	svc.StartedCalled = true
}

func (svc *ServiceTestObj) Stopped(moleculer.BrokerContext, moleculer.ServiceSchema) {
	svc.StoppedCalled = true
}

func (svc *ServiceTestObj) Name() string {
	return "My Name"
}

func (svc *ServiceTestObj) Add(firstValue int, secondValue int) int {
	return firstValue + secondValue
}

func (svc *ServiceTestObj) NoArgsNoReturn() {
	svc.NoArgsNoReturnCalled = true
}

func (svc *ServiceTestObj) NoArgs() interface{} {
	return "NoArgs Invoked!"
}

func (svc *ServiceTestObj) JustParamsNoReturn(p moleculer.Payload) {
	svc.JustParamsNoReturnCalled = true
}

func (svc *ServiceTestObj) JustParams(p moleculer.Payload) interface{} {
	return "JustParams Invoked!"
}

func (svc *ServiceTestObj) JustContextNoReturn(context moleculer.Context) {

}

func (svc *ServiceTestObj) JustContext(context moleculer.Context) interface{} {
	return "JustContext Invoked!"
}

func (svc *ServiceTestObj) CompleteActionNoReturn(ctx moleculer.Context, params moleculer.Payload) {

}

func (svc *ServiceTestObj) CompleteAction(context moleculer.Context, params moleculer.Payload) interface{} {
	return "CompleteAction Invoked!"
}

func (svc ServiceTestObj) NonPointerCompleteAction(context moleculer.Context, params moleculer.Payload) interface{} {
	return "NonPointerCompleteAction Invoked!"
}

type ServiceTestNoParams struct {
	CreatedCalled bool
	StartedCalled bool
	StoppedCalled bool
}

func (svc *ServiceTestNoParams) Created(moleculer.ServiceSchema, *log.Entry) {
	svc.CreatedCalled = true
}

func (svc *ServiceTestNoParams) Started(moleculer.BrokerContext, moleculer.ServiceSchema) {
	svc.StartedCalled = true
}

func (svc *ServiceTestNoParams) Stopped(moleculer.BrokerContext, moleculer.ServiceSchema) {
	svc.StoppedCalled = true
}

type EmptyObj struct {
}
