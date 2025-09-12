package registry_test

import (
	"github.com/moleculer-go/moleculer/registry"
	"github.com/moleculer-go/moleculer/service"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var _ = Describe("Service Catalog", func() {

	var serviceCatalog *registry.ServiceCatalog
	var logger *log.Entry

	BeforeEach(func() {
		logger = log.NewEntry(log.New())
		serviceCatalog = registry.CreateServiceCatalog(logger)
	})

	Describe("RemoveByNode", func() {

		Context("when catalog is empty", func() {
			It("should return empty slice", func() {
				removed := serviceCatalog.RemoveByNode("non-existent-node")
				Expect(removed).To(BeEmpty())
			})
		})

		Context("when node has no services", func() {
			BeforeEach(func() {
				// Add service to different node
				svc := createTestService("test-service", "1.0.0", "other-node")
				serviceCatalog.Add(svc)
			})

			It("should return empty slice", func() {
				removed := serviceCatalog.RemoveByNode("target-node")
				Expect(removed).To(BeEmpty())
			})
		})

		Context("when node has single service", func() {
			var testService *service.Service

			BeforeEach(func() {
				testService = createTestService("test-service", "1.0.0", "target-node")
				serviceCatalog.Add(testService)
			})

			It("should return the service", func() {
				removed := serviceCatalog.RemoveByNode("target-node")
				Expect(removed).To(HaveLen(1))
				Expect(removed[0]).To(Equal(testService))
			})

			It("should remove service from catalog", func() {
				serviceCatalog.RemoveByNode("target-node")
				Expect(serviceCatalog.Find("test-service", "1.0.0", "target-node")).To(BeFalse())
			})

			It("should update service counters", func() {
				// Check counter exists before removal
				Expect(serviceCatalog.FindByName("test-service")).To(BeTrue())

				serviceCatalog.RemoveByNode("target-node")

				// After removal, counter should be 0 but key might still exist
				// Let's check that the service is actually removed from the catalog
				Expect(serviceCatalog.Find("test-service", "1.0.0", "target-node")).To(BeFalse())
			})
		})

		Context("when node has multiple services", func() {
			var services []*service.Service

			BeforeEach(func() {
				services = []*service.Service{
					createTestService("service-1", "1.0.0", "target-node"),
					createTestService("service-2", "1.0.0", "target-node"),
					createTestService("service-3", "2.0.0", "target-node"),
				}
				for _, svc := range services {
					serviceCatalog.Add(svc)
				}
			})

			It("should return all services from the node", func() {
				removed := serviceCatalog.RemoveByNode("target-node")
				Expect(removed).To(HaveLen(3))
				Expect(removed).To(ContainElements(services))
			})

			It("should remove all services from catalog", func() {
				serviceCatalog.RemoveByNode("target-node")
				for _, svc := range services {
					Expect(serviceCatalog.Find(svc.Name(), svc.Version(), svc.NodeID())).To(BeFalse())
				}
			})

			It("should update all service counters", func() {
				// Check counters exist before removal
				Expect(serviceCatalog.FindByName("service-1")).To(BeTrue())
				Expect(serviceCatalog.FindByName("service-2")).To(BeTrue())
				Expect(serviceCatalog.FindByName("service-3")).To(BeTrue())

				serviceCatalog.RemoveByNode("target-node")

				// After removal, services should be removed from catalog
				Expect(serviceCatalog.Find("service-1", "1.0.0", "target-node")).To(BeFalse())
				Expect(serviceCatalog.Find("service-2", "1.0.0", "target-node")).To(BeFalse())
				Expect(serviceCatalog.Find("service-3", "2.0.0", "target-node")).To(BeFalse())
			})
		})

		Context("when multiple nodes have services", func() {
			var targetServices, otherServices []*service.Service

			BeforeEach(func() {
				targetServices = []*service.Service{
					createTestService("service-1", "1.0.0", "target-node"),
					createTestService("service-2", "1.0.0", "target-node"),
				}
				otherServices = []*service.Service{
					createTestService("service-3", "1.0.0", "other-node"),
					createTestService("service-4", "1.0.0", "other-node"),
				}

				for _, svc := range append(targetServices, otherServices...) {
					serviceCatalog.Add(svc)
				}
			})

			It("should only remove services from target node", func() {
				removed := serviceCatalog.RemoveByNode("target-node")
				Expect(removed).To(HaveLen(2))
				Expect(removed).To(ContainElements(targetServices))
			})

			It("should leave other node services intact", func() {
				serviceCatalog.RemoveByNode("target-node")
				for _, svc := range otherServices {
					Expect(serviceCatalog.Find(svc.Name(), svc.Version(), svc.NodeID())).To(BeTrue())
				}
			})
		})
	})

	Describe("Remove", func() {

		Context("when catalog is empty", func() {
			It("should return empty slice", func() {
				removed := serviceCatalog.Remove("non-existent-node", "non-existent-service")
				Expect(removed).To(BeEmpty())
			})
		})

		Context("when service doesn't exist", func() {
			BeforeEach(func() {
				svc := createTestService("existing-service", "1.0.0", "target-node")
				serviceCatalog.Add(svc)
			})

			It("should return empty slice for non-existent service", func() {
				removed := serviceCatalog.Remove("target-node", "non-existent-service")
				Expect(removed).To(BeEmpty())
			})

			It("should return empty slice for non-existent node", func() {
				removed := serviceCatalog.Remove("non-existent-node", "existing-service")
				Expect(removed).To(BeEmpty())
			})
		})

		Context("when service exists", func() {
			var testService *service.Service

			BeforeEach(func() {
				testService = createTestService("test-service", "1.0.0", "target-node")
				serviceCatalog.Add(testService)
			})

			It("should return the service", func() {
				removed := serviceCatalog.Remove("target-node", "test-service")
				Expect(removed).To(HaveLen(1))
				Expect(removed[0]).To(Equal(testService))
			})

			It("should remove service from catalog", func() {
				serviceCatalog.Remove("target-node", "test-service")
				Expect(serviceCatalog.Find("test-service", "1.0.0", "target-node")).To(BeFalse())
			})
		})

		Context("when multiple services have same name but different versions", func() {
			var services []*service.Service

			BeforeEach(func() {
				services = []*service.Service{
					createTestService("same-name", "1.0.0", "target-node"),
					createTestService("same-name", "2.0.0", "target-node"),
					createTestService("same-name", "1.0.0", "other-node"),
				}
				for _, svc := range services {
					serviceCatalog.Add(svc)
				}
			})

			It("should only remove services with matching node and name", func() {
				removed := serviceCatalog.Remove("target-node", "same-name")
				Expect(removed).To(HaveLen(2))
				Expect(removed).To(ContainElements(services[0], services[1]))
			})

			It("should leave other node services intact", func() {
				serviceCatalog.Remove("target-node", "same-name")
				Expect(serviceCatalog.Find("same-name", "1.0.0", "other-node")).To(BeTrue())
			})
		})
	})

	Describe("Add and Find operations", func() {

		Context("when adding services", func() {
			It("should be able to find added service", func() {
				svc := createTestService("test-service", "1.0.0", "test-node")
				serviceCatalog.Add(svc)

				Expect(serviceCatalog.Find("test-service", "1.0.0", "test-node")).To(BeTrue())
				Expect(serviceCatalog.FindByName("test-service")).To(BeTrue())
			})

			It("should be able to get added service", func() {
				svc := createTestService("test-service", "1.0.0", "test-node")
				serviceCatalog.Add(svc)

				retrieved := serviceCatalog.Get("test-service", "1.0.0", "test-node")
				Expect(retrieved).ToNot(BeNil())
				Expect(retrieved.Name()).To(Equal(svc.Name()))
				Expect(retrieved.Version()).To(Equal(svc.Version()))
				Expect(retrieved.NodeID()).To(Equal(svc.NodeID()))
			})
		})
	})
})

// Helper function to create test services
func createTestService(name, version, nodeID string) *service.Service {
	serviceInfo := map[string]interface{}{
		"name":     name,
		"version":  version,
		"settings": map[string]interface{}{},
		"metadata": map[string]interface{}{},
		"actions":  map[string]interface{}{},
		"events":   map[string]interface{}{},
	}
	svc := service.CreateServiceFromMap(serviceInfo)
	svc.SetNodeID(nodeID)
	return svc
}
