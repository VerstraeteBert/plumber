package garbage_collector

import (
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/VerstraeteBert/plumber-operator/controllers/shared"
)

type KafkaClients struct {
	client sarama.Client
	admin  sarama.ClusterAdmin
}

func BuildKafkaClients() (KafkaClients, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_7_0_0

	client, err := sarama.NewClient(strings.Split(shared.InternalBootstrap, ","), config)
	if err != nil {
		return KafkaClients{}, fmt.Errorf("error creating kafka client: %s", err)
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		if !client.Closed() {
			client.Close()
		}
		return KafkaClients{}, fmt.Errorf("error creating kafka admin: %s", err)
	}

	return KafkaClients{
		client: client,
		admin:  admin,
	}, nil
}
