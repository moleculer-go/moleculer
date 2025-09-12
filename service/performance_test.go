package service

import (
	"fmt"

	"github.com/moleculer-go/moleculer"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Service Performance Functions - Current Implementation", func() {

	Describe("findAction", func() {

		Context("when actions list is empty", func() {
			It("should return false", func() {
				actions := []moleculer.Action{}
				result := findAction("test-action", actions)
				Expect(result).To(BeFalse())
			})
		})

		Context("when action doesn't exist", func() {
			It("should return false", func() {
				actions := []moleculer.Action{
					{Name: "action1"},
					{Name: "action2"},
					{Name: "action3"},
				}
				result := findAction("non-existent", actions)
				Expect(result).To(BeFalse())
			})
		})

		Context("when action exists", func() {
			It("should return true for first action", func() {
				actions := []moleculer.Action{
					{Name: "action1"},
					{Name: "action2"},
					{Name: "action3"},
				}
				result := findAction("action1", actions)
				Expect(result).To(BeTrue())
			})

			It("should return true for middle action", func() {
				actions := []moleculer.Action{
					{Name: "action1"},
					{Name: "action2"},
					{Name: "action3"},
				}
				result := findAction("action2", actions)
				Expect(result).To(BeTrue())
			})

			It("should return true for last action", func() {
				actions := []moleculer.Action{
					{Name: "action1"},
					{Name: "action2"},
					{Name: "action3"},
				}
				result := findAction("action3", actions)
				Expect(result).To(BeTrue())
			})
		})

		Context("when actions have duplicate names", func() {
			It("should return true for first occurrence", func() {
				actions := []moleculer.Action{
					{Name: "duplicate"},
					{Name: "action2"},
					{Name: "duplicate"},
				}
				result := findAction("duplicate", actions)
				Expect(result).To(BeTrue())
			})
		})

		Context("with large action list", func() {
			It("should find action efficiently", func() {
				// Create a large list of actions
				actions := make([]moleculer.Action, 1000)
				for i := 0; i < 1000; i++ {
					actions[i] = moleculer.Action{Name: fmt.Sprintf("action-%d", i)}
				}

				// Test finding first action
				result := findAction("action-0", actions)
				Expect(result).To(BeTrue())

				// Test finding middle action
				result = findAction("action-500", actions)
				Expect(result).To(BeTrue())

				// Test finding last action
				result = findAction("action-999", actions)
				Expect(result).To(BeTrue())

				// Test finding non-existent action
				result = findAction("non-existent", actions)
				Expect(result).To(BeFalse())
			})
		})
	})

	Describe("concatenateEvents - CURRENT BUGGY IMPLEMENTATION", func() {

		Context("when service has no events", func() {
			It("should add all mixin events (correct behavior)", func() {
				service := moleculer.ServiceSchema{
					Events: []moleculer.Event{},
				}
				mixin := &moleculer.Mixin{
					Events: []moleculer.Event{
						{Name: "event1"},
						{Name: "event2"},
					},
				}

				result := concatenateEvents(service, mixin)
				// Correct behavior: all mixin events should be added when service has no events
				Expect(result.Events).To(HaveLen(2))
				Expect(result.Events[0].Name).To(Equal("event1"))
				Expect(result.Events[1].Name).To(Equal("event2"))
			})
		})

		Context("when mixin has no events", func() {
			It("should keep existing service events", func() {
				service := moleculer.ServiceSchema{
					Events: []moleculer.Event{
						{Name: "service-event1"},
						{Name: "service-event2"},
					},
				}
				mixin := &moleculer.Mixin{
					Events: []moleculer.Event{},
				}

				result := concatenateEvents(service, mixin)
				Expect(result.Events).To(HaveLen(2))
				Expect(result.Events[0].Name).To(Equal("service-event1"))
				Expect(result.Events[1].Name).To(Equal("service-event2"))
			})
		})

		Context("when events don't overlap", func() {
			It("should add all mixin events once (correct behavior)", func() {
				service := moleculer.ServiceSchema{
					Events: []moleculer.Event{
						{Name: "service-event1"},
						{Name: "service-event2"},
					},
				}
				mixin := &moleculer.Mixin{
					Events: []moleculer.Event{
						{Name: "mixin-event1"},
						{Name: "mixin-event2"},
					},
				}

				result := concatenateEvents(service, mixin)
				// Correct behavior: all events should be added exactly once
				Expect(result.Events).To(HaveLen(4)) // 2 original + 2 new
				Expect(result.Events[0].Name).To(Equal("service-event1"))
				Expect(result.Events[1].Name).To(Equal("service-event2"))
				Expect(result.Events[2].Name).To(Equal("mixin-event1"))
				Expect(result.Events[3].Name).To(Equal("mixin-event2"))
			})
		})

		Context("when events have duplicates", func() {
			It("should not add duplicate events (correct behavior)", func() {
				service := moleculer.ServiceSchema{
					Events: []moleculer.Event{
						{Name: "duplicate-event"},
						{Name: "service-event"},
					},
				}
				mixin := &moleculer.Mixin{
					Events: []moleculer.Event{
						{Name: "duplicate-event"},
						{Name: "mixin-event"},
					},
				}

				result := concatenateEvents(service, mixin)
				// Correct behavior: no duplicates, only new events added
				Expect(result.Events).To(HaveLen(3)) // 2 original + 1 new
				Expect(result.Events[0].Name).To(Equal("duplicate-event"))
				Expect(result.Events[1].Name).To(Equal("service-event"))
				Expect(result.Events[2].Name).To(Equal("mixin-event"))
			})
		})

		Context("when mixin has multiple duplicate events", func() {
			It("should not add any duplicate events (correct behavior)", func() {
				service := moleculer.ServiceSchema{
					Events: []moleculer.Event{
						{Name: "duplicate1"},
						{Name: "duplicate2"},
					},
				}
				mixin := &moleculer.Mixin{
					Events: []moleculer.Event{
						{Name: "duplicate1"},
						{Name: "duplicate2"},
						{Name: "duplicate1"}, // Another duplicate
					},
				}

				result := concatenateEvents(service, mixin)
				// Correct behavior: no duplicates added, only original events remain
				Expect(result.Events).To(HaveLen(2)) // 2 original, no new events
				Expect(result.Events[0].Name).To(Equal("duplicate1"))
				Expect(result.Events[1].Name).To(Equal("duplicate2"))
			})
		})

		Context("with large event lists", func() {
			It("should handle large lists efficiently (correct behavior)", func() {
				// Create service with many events
				serviceEvents := make([]moleculer.Event, 1000)
				for i := 0; i < 1000; i++ {
					serviceEvents[i] = moleculer.Event{Name: fmt.Sprintf("service-event-%d", i)}
				}
				service := moleculer.ServiceSchema{
					Events: serviceEvents,
				}

				// Create mixin with many events (some duplicates)
				mixinEvents := make([]moleculer.Event, 500)
				for i := 0; i < 500; i++ {
					if i < 250 {
						// First half are duplicates
						mixinEvents[i] = moleculer.Event{Name: fmt.Sprintf("service-event-%d", i)}
					} else {
						// Second half are new events
						mixinEvents[i] = moleculer.Event{Name: fmt.Sprintf("mixin-event-%d", i)}
					}
				}
				mixin := &moleculer.Mixin{
					Events: mixinEvents,
				}

				result := concatenateEvents(service, mixin)
				// Correct behavior: 1000 service events + 250 new mixin events = 1250 total
				// O(n+m) complexity instead of O(n*m)
				Expect(result.Events).To(HaveLen(1250))
			})
		})

		Context("edge cases", func() {
			It("should handle empty event names", func() {
				service := moleculer.ServiceSchema{
					Events: []moleculer.Event{
						{Name: ""},
						{Name: "valid-event"},
					},
				}
				mixin := &moleculer.Mixin{
					Events: []moleculer.Event{
						{Name: ""},
						{Name: "mixin-event"},
					},
				}

				result := concatenateEvents(service, mixin)
				// Correct behavior: no duplicates, only new events added
				Expect(result.Events).To(HaveLen(3)) // 2 original + 1 new
				Expect(result.Events[0].Name).To(Equal(""))
				Expect(result.Events[1].Name).To(Equal("valid-event"))
				Expect(result.Events[2].Name).To(Equal("mixin-event"))
			})

			It("should handle case-sensitive event names", func() {
				service := moleculer.ServiceSchema{
					Events: []moleculer.Event{
						{Name: "Event1"},
						{Name: "event1"},
					},
				}
				mixin := &moleculer.Mixin{
					Events: []moleculer.Event{
						{Name: "EVENT1"},
						{Name: "event1"},
					},
				}

				result := concatenateEvents(service, mixin)
				// Correct behavior: case-sensitive comparison, no duplicates
				Expect(result.Events).To(HaveLen(3)) // 2 original + 1 new
				Expect(result.Events[0].Name).To(Equal("Event1"))
				Expect(result.Events[1].Name).To(Equal("event1"))
				Expect(result.Events[2].Name).To(Equal("EVENT1"))
			})
		})
	})

	Describe("extendActions - CURRENT IMPLEMENTATION", func() {

		Context("when service has no actions", func() {
			It("should add all mixin actions", func() {
				service := moleculer.ServiceSchema{
					Actions: []moleculer.Action{},
				}
				mixin := &moleculer.Mixin{
					Actions: []moleculer.Action{
						{Name: "action1"},
						{Name: "action2"},
					},
				}

				result := extendActions(service, mixin)
				Expect(result.Actions).To(HaveLen(2))
				Expect(result.Actions[0].Name).To(Equal("action1"))
				Expect(result.Actions[1].Name).To(Equal("action2"))
			})
		})

		Context("when mixin has no actions", func() {
			It("should keep existing service actions", func() {
				service := moleculer.ServiceSchema{
					Actions: []moleculer.Action{
						{Name: "service-action1"},
						{Name: "service-action2"},
					},
				}
				mixin := &moleculer.Mixin{
					Actions: []moleculer.Action{},
				}

				result := extendActions(service, mixin)
				Expect(result.Actions).To(HaveLen(2))
				Expect(result.Actions[0].Name).To(Equal("service-action1"))
				Expect(result.Actions[1].Name).To(Equal("service-action2"))
			})
		})

		Context("when actions don't overlap", func() {
			It("should add all mixin actions", func() {
				service := moleculer.ServiceSchema{
					Actions: []moleculer.Action{
						{Name: "service-action1"},
						{Name: "service-action2"},
					},
				}
				mixin := &moleculer.Mixin{
					Actions: []moleculer.Action{
						{Name: "mixin-action1"},
						{Name: "mixin-action2"},
					},
				}

				result := extendActions(service, mixin)
				Expect(result.Actions).To(HaveLen(4))
				Expect(result.Actions[0].Name).To(Equal("service-action1"))
				Expect(result.Actions[1].Name).To(Equal("service-action2"))
				Expect(result.Actions[2].Name).To(Equal("mixin-action1"))
				Expect(result.Actions[3].Name).To(Equal("mixin-action2"))
			})
		})

		Context("when actions have duplicates", func() {
			It("should not add duplicate actions", func() {
				service := moleculer.ServiceSchema{
					Actions: []moleculer.Action{
						{Name: "duplicate-action"},
						{Name: "service-action"},
					},
				}
				mixin := &moleculer.Mixin{
					Actions: []moleculer.Action{
						{Name: "duplicate-action"},
						{Name: "mixin-action"},
					},
				}

				result := extendActions(service, mixin)
				Expect(result.Actions).To(HaveLen(3))
				Expect(result.Actions[0].Name).To(Equal("duplicate-action"))
				Expect(result.Actions[1].Name).To(Equal("service-action"))
				Expect(result.Actions[2].Name).To(Equal("mixin-action"))
			})
		})

		Context("with large action lists", func() {
			It("should handle large lists efficiently", func() {
				// Create service with many actions
				serviceActions := make([]moleculer.Action, 1000)
				for i := 0; i < 1000; i++ {
					serviceActions[i] = moleculer.Action{Name: fmt.Sprintf("service-action-%d", i)}
				}
				service := moleculer.ServiceSchema{
					Actions: serviceActions,
				}

				// Create mixin with many actions (some duplicates)
				mixinActions := make([]moleculer.Action, 500)
				for i := 0; i < 500; i++ {
					if i < 250 {
						// First half are duplicates
						mixinActions[i] = moleculer.Action{Name: fmt.Sprintf("service-action-%d", i)}
					} else {
						// Second half are new actions
						mixinActions[i] = moleculer.Action{Name: fmt.Sprintf("mixin-action-%d", i)}
					}
				}
				mixin := &moleculer.Mixin{
					Actions: mixinActions,
				}

				result := extendActions(service, mixin)
				// Should have 1000 service actions + 250 new mixin actions = 1250 total
				Expect(result.Actions).To(HaveLen(1250))
			})
		})
	})
})
