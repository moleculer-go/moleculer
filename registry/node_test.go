package registry

import (
	"reflect"
	"testing"
)

func TestFilterServices(t *testing.T) {
	currentServices := []map[string]interface{}{
		{"name": "service1"},
		{"name": "service2"},
		{"name": "service3"},
	}
	info := map[string]interface{}{
		"services": []interface{}{
			map[string]interface{}{"name": "service1"},
			map[string]interface{}{"name": "$service4"},
			map[string]interface{}{"name": "service5"},
		},
	}

	services, removedServices := FilterServices(currentServices, info)

	expectedServices := []map[string]interface{}{
		{"name": "service1"},
		{"name": "service5"},
	}
	if !reflect.DeepEqual(services, expectedServices) {
		t.Errorf("Expected services %v, but got %v", expectedServices, services)
	}

	expectedRemovedServices := []map[string]interface{}{
		{"name": "service2"},
		{"name": "service3"},
	}
	if !reflect.DeepEqual(removedServices, expectedRemovedServices) {
		t.Errorf("Expected removed services %v, but got %v", expectedRemovedServices, removedServices)
	}
}
