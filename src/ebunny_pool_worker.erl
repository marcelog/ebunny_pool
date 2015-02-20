-module(ebunny_pool_worker).
-github("https://github.com/marcelog").
-homepage("http://marcelog.github.com/").
-license("Apache License 2.0").

-behavior(gen_server).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Required Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-include_lib("amqp_client/include/amqp_client.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(state, {
  options = undefined:: undefined|[term()],
  channel = undefined:: undefined|pid(),
  channel_ref = undefined:: undefined|reference(),
  available = false:: boolean(),
  callback = undefined:: undefined|module(),
  callback_state = undefined:: undefined|term()
}).
-type state():: #state{}.
-type callback_state():: term().

-type option()::
  {user, string()}
  | {pass, string()}
  | {vhost, string()}
  | {port, pos_integer()}
  | {host, string()}
  | {reconnect_timeout, pos_integer()}
  | {exchange, string()}
  | {queue, string()}
  | {retry_timeout, pos_integer()}
  | {concurrency, pos_integer()}
  | {callback, module()}
  | {callback_options, term()}.

-type init_result():: {ok, callback_state()}|{error, term()}.
-type result():: ok|error.

-export_type([callback_state/0]).
-export_type([option/0]).
-export_type([result/0]).
-export_type([init_result/0]).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Callback definitions.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-callback init(term()) -> init_result().
-callback handle(
  #'basic.deliver'{}, #amqp_msg{}, callback_state()
) -> result().
-callback terminate(term(), callback_state()) -> ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Exports.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Public API.
-export([start_link/1]).

%%% gen_server API.
-export([
  init/1, handle_info/2, handle_cast/2, handle_call/3,
  code_change/3, terminate/2
]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Public API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Starts a new consumer.
-spec start_link([option()]) -> {ok, pid()}.
start_link(Options) ->
  gen_server:start_link(?MODULE, Options, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_server API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init(proplists:proplist()) -> {ok, state()}.
init(Options) ->
  erlang:send_after(0, self(), {connect}),
  CallbackModule = proplists:get_value(callback, Options),
  CallbackOptions = proplists:get_value(callback_options, Options),
  lager:info("Initializing worker: ~p", [CallbackModule]),
  try
    case CallbackModule:init(CallbackOptions) of
      {ok, CallbackState} ->
        {ok, #state{
          options = Options,
          callback_state = CallbackState,
          callback = CallbackModule
        }};
      {error, Error} ->
        lager:error("Could not initialize mq worker: ~p", [Error]),
        {stop, Error}
    end
  catch
    _:HardError ->
      lager:error(
        "Exception: Could not initialize mq worker: ~p: ~p",
        [HardError, erlang:get_stacktrace()]
      ),
      {stop, HardError}
  end.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Msg, State) ->
  lager:error("Invalid cast: ~p", [Msg]),
  {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({connect}, State) ->
  Connect = fun() ->
    case connect(State#state.options) of
      {ok, Pid} -> Pid;
      Error -> lager:alert("MQ NOT available: ~p", [Error]), not_available
    end
  end,
  Channel = case State#state.channel of
    Pid when is_pid(Pid) -> case is_process_alive(Pid) of
      true -> Pid;
      false -> Connect()
    end;
    _ -> Connect()
  end,
  ChannelRef = case Channel of
    _ when is_pid(Channel) -> erlang:monitor(process, Channel);
    _ -> erlang:send_after(
      proplists:get_value(reconnect_timeout, State#state.options),
      self(), {connect}
    ),
    undefined
  end,
  Available = is_pid(Channel) andalso ChannelRef =/= undefined,
  if
    Available ->
      Exchange = case proplists:get_value(exchange, State#state.options) of
        Exchange_ = {_, _} -> Exchange_;
        ExchangeName -> {ExchangeName, [{withretry, true}]}
      end,
      Queue = case proplists:get_value(queue, State#state.options) of
        Queue_ = {_, _} -> Queue_;
        QueueName -> {QueueName, [{withretry, true}]}
      end,
      TaskRetry = proplists:get_value(retry_timeout, State#state.options),
      Concurrency = proplists:get_value(concurrency, State#state.options),
      ok = create_exchange(Channel, Exchange),
      ok = create_queue(Channel, Exchange, Queue, TaskRetry, Concurrency);
    true -> ok
  end,
  {noreply, State#state{
    channel = Channel,
    channel_ref = ChannelRef,
    available = Available
  }};

handle_info({
  DeliverInfo = #'basic.deliver'{
    consumer_tag = _ConsumerTag,
    delivery_tag = DeliveryTag,
    redelivered = _Redelivered,
    exchange = _Exchange,
    routing_key = _RoutingKey
  },
  Message = #amqp_msg{
    props = _Props,
    payload = _Payload
  }
}, State=#state{channel = Channel}) ->
  spawn(fun() ->
    CallbackModule = State#state.callback,
    Result = try
      CallbackModule:handle(DeliverInfo, Message, State#state.callback_state)
    catch
      _:Error ->
        lager:error(
          "Error handling message: ~p: ~p", [Error, erlang:get_stacktrace()]
        ),
        error
    end,
    case Result of
      ok -> amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = DeliveryTag});
      error -> amqp_channel:cast(
        Channel, #'basic.reject'{requeue = false, delivery_tag = DeliveryTag}
      )
    end
  end),
  {noreply, State};

handle_info({'basic.consume_ok', Tag}, State) ->
  lager:info("Consuming with tag: ~p", [Tag]),
  {noreply, State};

handle_info(
  {'DOWN', MQRef, process, MQPid, Reason},
  State=#state{channel = MQPid, channel_ref = MQRef}
) ->
  lager:warning("MQ channel is down: ~p", [Reason]),
  erlang:send_after(0, self(), {connect}),
  {noreply, State#state{
    channel = undefined,
    channel_ref = undefined,
    available = false
  }};

handle_info(Msg, State) ->
  lager:error("Invalid msg: ~p", [Msg]),
  {noreply, State}.

-spec handle_call(
  term(), {pid(), reference()}, state()
) -> {reply, term(), state()}.
handle_call(Req, _From, State) ->
  lager:error("Invalid request: ~p", [Req]),
  {reply, invalid_request, State}.

-spec terminate(atom(), state()) -> ok.
terminate(Reason, State) ->
  TerminateChannel =
    State#state.channel =/= undefined
    andalso is_process_alive(State#state.channel),
  if
    TerminateChannel -> amqp_channel:close(State#state.channel);
    true -> ok
  end,
  lager:debug(
    "Queue consumer terminated with ~p: ~p",
    [Reason, lager:pr(State, ?MODULE)]
  ),
  CallbackModule = State#state.callback,
  CallbackModule:terminate(Reason, State#state.callback_state),
  ok.

-spec code_change(string(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Private API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec connect(proplists:proplist()) -> {ok, pid()}|term().
connect(Config) ->
  Get = fun
    ({s, X}) ->
      list_to_binary(proplists:get_value(X, Config));
    (X) ->
      proplists:get_value(X, Config) end,
  GetWithDefault = fun(X, Default) ->
    case Get(X) of
      undefined -> Default;
      Value -> Value
    end
  end,
  new_channel(amqp_connection:start(#amqp_params_network{
    username = Get({s, user}),
    password = Get({s, pass}),
    virtual_host = Get({s, vhost}),
    port = Get(port),
    host = Get(host),
    ssl_options = GetWithDefault(ssl_options, none)
  })).

-spec new_channel({ok, pid()}|term()) -> {ok, pid()}|term().
new_channel({ok, Connection}) ->
  amqp_connection:open_channel(Connection);

new_channel(Error) ->
  Error.

-spec create_exchange(pid(), {string(), proplists:proplist()}) -> ok.
create_exchange(Channel, {Name, Options}) ->
  #'exchange.declare_ok'{} = amqp_channel:call(Channel, exchange_options(Name, Options)),
  case proplists:get_value(withretry, Options) of
    true ->
      #'exchange.declare_ok'{} = amqp_channel:call(
        Channel, exchange_options(Name ++ ".retry")
      );
    _ -> ok
  end,
  ok.

-spec exchange_options(string()|proplists:proplist()) -> #'exchange.declare'{}.
exchange_options(Name, Options) ->
  #'exchange.declare'{
    exchange = list_to_binary(Name),
    type        = proplists:get_value(type, Options, <<"direct">>),
    passive     = proplists:get_value(passive, Options, false),
    durable     = proplists:get_value(durable, Options, true),
    auto_delete = proplists:get_value(auto_delete, Options, false),
    arguments   = proplists:get_value(arguments, Options, [])
  }.

exchange_options(Name) ->
  exchange_options(Name, []).

-spec create_queue(
  pid(), {string(), proplists:proplist()},
  {string(), proplists:proplist()}, pos_integer(), non_neg_integer()
) -> ok.
create_queue(Channel, {ExchangeName, Options}, {Name, QOptions}, RetryTimeout, Concurrency) ->
  QArgs = case proplists:get_value(withretry, Options) of
    true -> [
      {<<"x-dead-letter-exchange">>, longstr, list_to_binary(ExchangeName ++ ".retry")},
      {<<"x-dead-letter-routing-key">>, longstr, list_to_binary(Name ++ ".retry")}
    ];
    _ -> []
  end,
  QueueOptions = queue_options(Name, QOptions, QArgs),
  #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel, QueueOptions),

  case proplists:get_value(withretry, Options) of
    true ->
      QRArgs = [
        {<<"x-dead-letter-exchange">>, longstr, list_to_binary(ExchangeName)},
        {<<"x-dead-letter-routing-key">>, longstr, list_to_binary(Name)},
        {<<"x-message-ttl">>, long, RetryTimeout}
      ],
      RetryQueueOptions = queue_options(Name ++ ".retry", QOptions, QRArgs),
      #'queue.declare_ok'{queue = RetryQueue} = amqp_channel:call(
        Channel, RetryQueueOptions
      ),
      BindingRetry = #'queue.bind'{
        queue = RetryQueue,
        exchange = list_to_binary(ExchangeName ++ ".retry"),
        routing_key = list_to_binary(Name ++ ".retry")
      },
      #'queue.bind_ok'{} = amqp_channel:call(Channel, BindingRetry);
    _ -> ok
  end,

  Binding = #'queue.bind'{
    queue = Queue,
    exchange = list_to_binary(ExchangeName),
    routing_key = list_to_binary(proplists:get_value(routing_key, QOptions, Name))
  },
  #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),

  #'basic.qos_ok'{} = amqp_channel:call(
    Channel, #'basic.qos'{prefetch_count = Concurrency}
  ),
  Sub = #'basic.consume'{
    queue = Queue,
    no_ack = false
  },
  #'basic.consume_ok'{} = amqp_channel:call(Channel, Sub),
  ok.

-spec queue_options(
  string(), proplists:proplist(), [term()]
) -> #'queue.declare'{}.
queue_options(Name, Options, Arguments) ->
  #'queue.declare'{
    queue = list_to_binary(Name),
    durable = proplists:get_value(durable, Options, true),
    exclusive = proplists:get_value(exclusive, Options, false),
    auto_delete = proplists:get_value(auto_delete, Options, false),
    arguments = Arguments
  }.
