# ebunny_pool
This is a simple library that allows you to implement [rabbitmq](http://www.rabbitmq.com/)
workers. It will automatically declare the exchange and queue needed, and create
 a [DLX](https://www.rabbitmq.com/dlx.html) for it too, so failed tasks can be
 retried after the specified interval.

# How to use
Start the top level supervisor named **ebunny_pool_sup** from your own
supervisor tree. Create a module that implements the behavior **ebunny_pool_worker**:

```erlang
-module(sample_worker).

-behavior(ebunny_pool_worker).

-export([init/1, handle/3, terminate/2]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {}).

init(Options) ->
  {ok, #state{}}. % You can return {ok, term()} | {error, term()}.

handle(
  DeliverInfo = #'basic.deliver'{
    consumer_tag = _ConsumerTag,
    delivery_tag = DeliveryTag,
    redelivered = _Redelivered,
    exchange = _Exchange,
    routing_key = _RoutingKey
  },
  Message = #amqp_msg{
    props = _Props,
    payload = Payload
  },
  State
) ->
  lager:debug("Got message: ~p", [Payload]),
  ok. % Return ok to ACK the message, or error to reject it and send it to the DLX.

terminate(_Reason, _State) ->
  ok.
```
