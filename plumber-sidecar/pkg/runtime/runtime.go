package runtime

import (
	"github.com/VerstraeteBert/plumber-sidecar/pkg/config"
	"github.com/VerstraeteBert/plumber-sidecar/pkg/kafka"
	"go.uber.org/zap"
)

func InitRuntime(config config.Config) error {
	kfk, err := kafka.InitKafka(&config)
	if err != nil {
		return err
	}
	zap.L().Debug("Kafka Initialized")

	handler := NewMessageHandler(kfk.Producer.PublishDownstream)
	zap.L().Debug("Message Handler Setup")

	err = kfk.StartConsumeLoop(handler.handle)
	if err != nil {
		return err
	}

	return nil
}
