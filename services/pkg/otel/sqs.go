package otel

import (
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel/propagation"
)

// SQSCarrier adapts SQS MessageAttributes to OTel TextMapCarrier so that
// W3C trace context (`traceparent`, `tracestate`) can be injected on send
// and extracted on receive.
//
// 役割:
//   propagation.TextMapCarrier は本来 HTTP ヘッダのような Get/Set/Keys を
//   持つ map を抽象化したもの。OTel propagator はこれを介してしか trace
//   context を読み書きしない。SQS の MessageAttributes は HTTP ヘッダと
//   構造が異なる (DataType + StringValue を持つ struct の map) ため、
//   そのままでは propagator に渡せない。SQSCarrier はこの差異を吸収する。
//
// 使い方:
//
//	// 送信側 (api):
//	attrs := map[string]types.MessageAttributeValue{}
//	otel.GetTextMapPropagator().Inject(ctx, otel_pkg.SQSCarrier(attrs))
//	sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
//	    MessageAttributes: attrs, // ← traceparent などが書き込まれている
//	    ...
//	})
//
//	// 受信側 (manager):
//	ctx := otel.GetTextMapPropagator().Extract(ctx,
//	    otel_pkg.SQSCarrier(msg.MessageAttributes))
//	// この ctx を tracer.Start に渡せば、CONSUMER span が送信側の
//	// PRODUCER span の子として連結される。
//
// 注意:
//   受信側は ReceiveMessage で MessageAttributeNames=["All"] (または
//   "traceparent","tracestate") を明示しないと SQS が attrs を返さない。
type SQSCarrier map[string]types.MessageAttributeValue

var _ propagation.TextMapCarrier = SQSCarrier(nil)

func (c SQSCarrier) Get(key string) string {
	v, ok := c[key]
	if !ok || v.StringValue == nil {
		return ""
	}
	return *v.StringValue
}

func (c SQSCarrier) Set(key, value string) {
	dt := "String"
	c[key] = types.MessageAttributeValue{
		DataType:    &dt,
		StringValue: &value,
	}
}

func (c SQSCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
