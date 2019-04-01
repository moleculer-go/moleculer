package cli

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
)

// argsToConfig read args sent ot the CLI and populate a molecule config.
func argsToConfig() *moleculer.Config {
	//TODO
	return &moleculer.Config{}
}

// Start parse the config from the cli args. created a service broker and pass down to the startHandler.
// handles SIGTERM and stop the broker in case of the process being shutdown.
func Start(userConfig *moleculer.Config, startHandler func(*broker.ServiceBroker)) {
	argsConfig := argsToConfig()
	broker := broker.New(userConfig, argsConfig)

	signalC := make(chan os.Signal)
	signal.Notify(signalC, os.Interrupt, syscall.SIGTERM)

	startHandler(broker)
	<-signalC
	broker.Stop()
}
