-module(amqpfs_provider).
-behaviour(gen_server).

-export([behaviour_info/1]).

-export([start/1, start/2]).

-export([init/1, handle_info/2, handle_call/3, handle_cast/2, code_change/3, terminate/2]).

-export([call_module/3]).

-export([announce/3]).

-include_lib("amqpfs/include/amqpfs.hrl").

behaviour_info(callbacks) ->
    [ 
     ].

start(Module) ->
    start(Module, []).

start(Module, Args) ->
    gen_server:start(?MODULE, [Module, Args], []).

init([Module, Args]) ->
    State0 = #amqpfs_provider_state{ module = Module, args = Args },
    State1 = setup(State0),
    State2 = call_module(init, [State1], State1),
    {ok, State2}.


handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(Msg, State) ->
    spawn(fun () -> handle_info_async(Msg, State) end),
    {noreply, State}.

handle_info_async({#'basic.deliver'{consumer_tag=_ConsumerTag, delivery_tag=_DeliveryTag, redelivered=_Redelivered, exchange = <<"amqpfs">>, routing_key=_RoutingKey}, Content}, State) ->
    #amqp_msg{payload = Payload } = Content,
    #'P_basic'{content_type = ContentType, headers = Headers, message_id = MessageId} = Content#amqp_msg.props,
    Command = amqpfs_util:decode_payload(ContentType, Payload),
    ReqState = State#amqpfs_provider_state{ request_headers = Headers, request_command = Command },
    case call_module(allow_request,[ReqState], ReqState) of
        false ->
            skip;
        _ ->
            case Command of
                {list_dir, Path} ->
                    spawn(fun () -> send_response(MessageId, ?CONTENT_TYPE_BERT, [ttl(Path, ReqState)], term_to_binary(call_module(list_dir,[Path, ReqState], ReqState)), ReqState) end);
                {open, Path, Fi} ->
                    spawn(fun () -> send_response(MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(open, [Path, Fi, ReqState], ReqState)), ReqState) end);
                {read, Path, Size, Offset} ->
                    spawn(fun () -> 
                                  {ResultContentType, Result} = 
                                      case call_module(read, [Path, Size, Offset, ReqState], ReqState) of
                                          Datum when is_binary(Datum) ->
                                              {?CONTENT_TYPE_BIN, Datum};
                                          Datum ->
                                              {?CONTENT_TYPE_BERT, term_to_binary(Datum)}
                                      end,
                                  send_response(MessageId, ResultContentType, [ttl(Path, ReqState)], Result, ReqState)
                          end);
                {write, Path, Data, Offset} ->
                    spawn(fun () -> send_response(MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(output, [Path, Data, Offset, ReqState], ReqState)), ReqState) end);
                {getattr, Path} ->
                    spawn(fun () -> send_response(MessageId, ?CONTENT_TYPE_BERT, [ttl(Path, ReqState)], term_to_binary(call_module(getattr, [Path, ReqState], ReqState)), ReqState) end);
                {setattr, Path, Stat, Attr, ToSet} ->
                    spawn(fun () -> send_response(MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(setattr, [Path, Stat, Attr, ToSet, ReqState], ReqState)), ReqState) end);
                {release, Path, Fi} ->
                    spawn(fun () -> send_response(MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(release, [Path, Fi, ReqState], ReqState)), ReqState) end); 
                _ ->
                    ignore
            end
    end;

handle_info_async(Msg, State) ->
    spawn(fun () -> call_module(handle_info, [Msg, State], State) end).

code_change(_OldVsn, State, _Extra) ->
    State.

terminate(_Reason, _State) ->
    ok.
%%%%


send_response(ReplyTo, ContentType, Headers, Content, #amqpfs_provider_state{channel = Channel}) ->
    amqp_channel:call(Channel, #'basic.publish'{exchange = <<"amqpfs.response">>},
                      {amqp_msg, #'P_basic'{reply_to = ReplyTo,
                                            content_type = ContentType,
                                            headers = Headers
                                            },
                       Content}).

ttl(Path, State) ->
    case call_module(ttl, [Path, State], State) of
        forever ->
            {"ttl", long, -1};
        Val ->
            {"ttl", long, Val}
    end.

%%%%

announce(directory, Name, #amqpfs_provider_state{ channel = Channel } = State) ->
    setup_listener(Name, State),
    amqpfs_announce:directory(Channel, Name).

%%%% 

setup(#amqpfs_provider_state{ module = Module }=State) ->
    Credentials = call_module(amqp_credentials, [], State),
    {ok, Connection} = erabbitmq_connections:start(#amqp_params{username = list_to_binary(proplists:get_value(username, Credentials, "guest")),
                                                                password = list_to_binary(proplists:get_value(password, Credentials, "guest")),
                                                                virtual_host = list_to_binary(proplists:get_value(virtual_host, Credentials, "/")),
                                                                host = proplists:get_value(host, Credentials, "localhost"),
                                                                port = proplists:get_value(port, Credentials, ?PROTOCOL_PORT),
                                                                ssl_options = proplists:get_value(ssl_options, Credentials, none)
                                                               }),
    {ok, Channel} = erabbitmq_channels:open(Connection),
    amqpfs_util:setup(Channel),
    amqpfs_util:setup_provider_queue(Channel, Module),
    State#amqpfs_provider_state { connection = Connection, channel = Channel }.
    


setup_listener(Name, #amqpfs_provider_state{ module = Module, channel = Channel}) ->
    Queue = amqpfs_util:provider_queue_name(Module),
    #'queue.bind_ok'{} = amqp_channel:call(Channel, #'queue.bind'{
                                                                  queue = Queue, exchange = <<"amqpfs">>,
                                                                  routing_key = amqpfs_util:path_to_matching_routing_key(Name),
                                                                  nowait = false, arguments = []}),
    #'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:subscribe(Channel, #'basic.consume'{
                                                                                                       queue = Queue,
                                                                                                       consumer_tag = <<"">>,
                                                                                                       no_local = false,
                                                                                                       no_ack = true,
                                                                                                       exclusive = false,
                                                                                                       nowait = false}, self()),
    receive
          #'basic.consume_ok'{consumer_tag = ConsumerTag} -> ok
    end.


%%%

call_module(F, A, #amqpfs_provider_state{ module = Module }) ->
    call_module(F, A, Module);

call_module(F, A, Module) when is_atom(Module) ->
    case (catch apply(Module, F, A))  of
        {'EXIT', {Reason, [{Module, F, A}|_]}} when Reason == badarg; Reason == undef; Reason == function_clause ->
            call_module(F, A, amqpfs_provider_base);
        {'EXIT', {Reason, Trace}} ->
            error_logger:format("An error occured in AMQPFS provider ~p:~n  Reason: ~p~n  Trace: ~p~n",[Module, Reason, Trace]);
        Result ->
            Result
    end.
