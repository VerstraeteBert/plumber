package runtime

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"io/ioutil"
	nethttp "net/http"
	"time"
)

type MessageHandler struct {
	forwarder  func(context.Context, binding.Message) error
	httpClient *nethttp.Client
}

func NewMessageHandler(forwarder func(context.Context, binding.Message) error) *MessageHandler {
	return &MessageHandler{
		forwarder:  forwarder,
		httpClient: nethttp.DefaultClient,
	}
}

func (mh *MessageHandler) convertToCloudEventRequest(ctx context.Context, cm *sarama.ConsumerMessage, req *nethttp.Request) error {
	msg := kafka_sarama.NewMessageFromConsumerMessage(cm)
	if msg.ReadEncoding() == binding.EncodingUnknown {
		// not a cloud event
		// TODO deeper look at cloudevents extensions
		event := cloudevents.NewEvent()
		event.SetID(uuid.New().String())
		event.SetTime(time.Now())
		// should be set to the type of source -> kafka:topic
		// or topic:partition:offset
		event.SetType("todo.todo")
		event.SetSource("todo")

		// TODO copy additional headers passed with the message
		err := event.SetData("application/json", msg.Value)
		if err != nil {
			return err
		}
		return http.WriteRequest(ctx, binding.ToMessage(&event), req)
	}
	// Message is already a cloudevent :-)
	return http.WriteRequest(cloudevents.WithEncodingBinary(ctx), msg, req)
}

func (mh *MessageHandler) newBindingmessageFromProcessorResponse(response *nethttp.Response) (binding.Message, error) {
	event := cloudevents.NewEvent()
	event.SetID(uuid.New().String())
	event.SetTime(time.Now())
	bbytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	err = event.SetData("application/json", bbytes)
	if err != nil {
		return nil, err
	}
	event.SetType("todo.todo")
	event.SetSource("todo")
	return binding.ToMessage(&event), nil
}

func (mh *MessageHandler) handle(ctx context.Context, cm *sarama.ConsumerMessage) (bool, error) {
	// create request for processor
	// safe to ignore this error (it checks for validity of passed method & url & ctx)
	req, _ := nethttp.NewRequestWithContext(ctx, "POST", "http://127.0.0.1:8080", nil)

	// convert sarama consumerMessage to cloudevent
	err := mh.convertToCloudEventRequest(ctx, cm, req)
	if err != nil {
		return true, err
	}

	resp, err := mh.httpClient.Do(req)
	if err != nil {
		zap.L().Error("failed to forward message to processor", zap.Error(err))
		return false, err
	}
	// always close the body -> connection can be reused
	defer resp.Body.Close()

	// BadRequest signifies that the message could not be processed by the processor, drop it
	// Any other statuscodes imply that something went wrong in the SDK code -> drop it aswell
	// TODO dlq
	zap.L().Info(fmt.Sprintf("Found statuscode: %d", resp.StatusCode))
	if resp.StatusCode == nethttp.StatusBadRequest {
		return true, fmt.Errorf("processor returned BadRequest, the message could not be processed")
	} else if resp.StatusCode != 200 {
		return true, fmt.Errorf("processor responded with an unrecognized status-code: %d", resp.StatusCode)
	}
	respMessage, err := mh.newBindingmessageFromProcessorResponse(resp)
	if err != nil {
		return true, err
	}

	// 200 response code -> OK
	// send message to downstream processor topics / sink topics
	err = mh.forwarder(ctx, respMessage)
	if err != nil {
		return false, err
	}

	return true, nil
}
