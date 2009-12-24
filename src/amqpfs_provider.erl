-module(amqpfs_provider).
-behaviour(gen_server).

-export([behaviour_info/1]).

-export([start/1, start/2, start_link/1, start_link/2]).

-export([init/1, handle_info/2, handle_call/3, handle_cast/2, code_change/3, terminate/2]).

-export([call_module/3]).

-export([announce/3]).

-include_lib("amqpfs/include/amqpfs.hrl").

behaviour_info(callbacks) ->
    [ 
     ].

start_link(Module) ->
    start_link(Module, []).

start_link(Module, Args) ->
    gen_server:start_link(?MODULE, [Module, Args], []).

start(Module) ->
    start(Module, []).

start(Module, Args) ->
    gen_server:start(?MODULE, [Module, Args], []).

init([Module, Args]) ->
    State0 = #amqpfs_provider_state{ module = Module, args = Args},
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

handle_info_async({#'basic.deliver'{consumer_tag=_ConsumerTag, delivery_tag=_DeliveryTag, redelivered=_Redelivered, exchange = <<"amqpfs.provider">>, routing_key=_RoutingKey}, Content}, State) ->
    #amqp_msg{payload = Payload } = Content,
    #'P_basic'{content_type = ContentType, headers = _Headers, message_id = MessageId, reply_to = ReplyTo} = Content#amqp_msg.props,
    Command = amqpfs_util:decode_payload(ContentType, Payload),
    case Command of
        ping ->
            send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(pong), State);
        _ ->
            ignore
    end;

handle_info_async({#'basic.deliver'{consumer_tag=_ConsumerTag, delivery_tag=_DeliveryTag, redelivered=_Redelivered, exchange = <<"amqpfs">>, routing_key=_RoutingKey}, Content}, State) ->
    #amqp_msg{payload = Payload } = Content,
    #'P_basic'{content_type = ContentType, headers = Headers, message_id = MessageId, reply_to = ReplyTo} = Content#amqp_msg.props,
    Command = amqpfs_util:decode_payload(ContentType, Payload),
    ReqState = State#amqpfs_provider_state{ request_headers = Headers, request_command = Command },
    case call_module(allow_request,[ReqState], ReqState) of
        false ->
            skip;
        _ ->
            case Command of
                {list_dir, Path} ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [ttl(Path, ReqState)], term_to_binary(call_module(list_dir,[tokenize_path(Path),ReqState], ReqState)), ReqState);
                {create, Path, Name, Mode} when (Mode band ?S_IFMT) =:= ?S_IFREG ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(create, [tokenize_path(Path),Name, Mode, ReqState], ReqState)), ReqState);
                {create, Path, Name, Mode} when (Mode band ?S_IFMT) =:= ?S_IFDIR ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(create_dir, [tokenize_path(Path),Name, Mode, ReqState], ReqState)), ReqState);                
                {rmdir, Path} ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(rmdir, [tokenize_path(Path),ReqState], ReqState)), ReqState);
                {remove, Path} ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(remove, [tokenize_path(Path),ReqState], ReqState)), ReqState);
                {rename, Path, NewPath} ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(rename, [tokenize_path(Path),NewPath, ReqState], ReqState)), ReqState);
                {link, Path, NewPath} ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(link, [tokenize_path(Path),NewPath, ReqState], ReqState)), ReqState);
                {symlink, Path, Contents} ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(symlink, [tokenize_path(Path),Contents, ReqState], ReqState)), ReqState);
                {readlink, Path} ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [ttl(Path, ReqState)], term_to_binary(call_module(readlink, [tokenize_path(Path),ReqState], ReqState)), ReqState);
                {open, Path, Fi} ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(open, [tokenize_path(Path),Fi, ReqState], ReqState)), ReqState);
                {read, Path, Size, Offset} ->
                    {ResultContentType, Result} = 
                        case call_module(read, [tokenize_path(Path),Size, Offset, ReqState], ReqState) of
                            Datum when is_binary(Datum) ->
                                {?CONTENT_TYPE_BIN, Datum};
                            Datum ->
                                {?CONTENT_TYPE_BERT, term_to_binary(Datum)}
                        end,
                    send_response(ReplyTo, MessageId, ResultContentType, [ttl(Path, ReqState)], Result, ReqState);
                {write, Path, Data, Offset} ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(output, [tokenize_path(Path),Data, Offset, ReqState], ReqState)), ReqState);
                {getattr, Path} ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [ttl(Path, ReqState)], term_to_binary(call_module(getattr, [tokenize_path(Path),ReqState], ReqState)), ReqState);
                {setattr, Path, Stat, Attr, ToSet} ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(setattr, [tokenize_path(Path),Stat, Attr, ToSet, ReqState], ReqState)), ReqState);
                {release, Path, Fi} ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(release, [tokenize_path(Path),Fi, ReqState], ReqState)), ReqState); 
                {get_lock, Path, Fi, Lock} ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [ttl(Path, ReqState)], term_to_binary(call_module(get_lock, [tokenize_path(Path),Fi, Lock, ReqState], ReqState)), ReqState);
                {set_lock, Path, Fi, Lock, Sleep} ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(set_lock, [tokenize_path(Path),Fi, Lock, Sleep, ReqState], ReqState)), ReqState);                
                {flush, Path, Fi} ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(flush, [tokenize_path(Path),Fi, ReqState], ReqState)), ReqState);                
                {access, Path, Mask} ->
                    send_response(ReplyTo, MessageId, ?CONTENT_TYPE_BERT, [], term_to_binary(call_module(access, [tokenize_path(Path),Mask, ReqState], ReqState)), ReqState);                
                _ ->
                    ignore
            end
    end;

handle_info_async(Msg, State) ->
    call_module(handle_info, [Msg, State], State).

code_change(_OldVsn, State, _Extra) ->
    State.

terminate(_Reason, _State) ->
    ok.
%%%%


send_response(ReplyTo, MessageId, ContentType, Headers, Content, #amqpfs_provider_state{channel = Channel, app_id = AppId, user_id = UserId}) ->
    amqp_channel:call(Channel, #'basic.publish'{exchange = <<"amqpfs.response">>, routing_key = ReplyTo},
                      {amqp_msg, #'P_basic'{correlation_id = MessageId,
                                            content_type = ContentType,
                                            app_id = AppId,
                                            user_id = UserId,
                                            headers = Headers
                                            },
                       Content}).

ttl(Path, State) ->
    case call_module(ttl, [tokenize_path(Path), State], State) of
        forever ->
            {"ttl", long, -1};
        Val ->
            {"ttl", long, Val}
    end.

%%%%

announce(directory, Name, #amqpfs_provider_state{ channel = Channel, app_id = AppId, user_id = UserId } = State) ->
    setup_listener(Name, State),
    amqp_channel:call(Channel, #'basic.publish'{exchange= <<"amqpfs.announce">>}, {amqp_msg, #'P_basic'{content_type = ?CONTENT_TYPE_BERT, app_id = AppId, user_id = UserId}, term_to_binary({announce, directory, {Name,on_demand}})}).

%%%% 

setup(#amqpfs_provider_state{}=State) ->
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
    amqpfs_util:setup_provider_queue(Channel, provider_name(State)),
    AppId = list_to_binary(amqpfs_util:term_to_string(provider_name(State))),
    UserId = list_to_binary(ossp_uuid:make(v1)),
    State#amqpfs_provider_state { connection = Connection, channel = Channel, app_id = AppId, user_id = UserId }.
    


setup_listener(Name, #amqpfs_provider_state{channel = Channel, user_id = UserId}=State) ->
    Queue = amqpfs_util:provider_queue_name(provider_name(State)),
    #'queue.bind_ok'{} = amqp_channel:call(Channel, #'queue.bind'{
                                             queue = Queue, exchange = <<"amqpfs">>,
                                             routing_key = amqpfs_util:path_to_matching_routing_key(Name),
                                             nowait = false, arguments = []}),
    #'queue.bind_ok'{} = amqp_channel:call(Channel, #'queue.bind'{
                                             queue = Queue, exchange = <<"amqpfs.provider">>,
                                             routing_key = UserId,
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

%%

provider_name(#amqpfs_provider_state{ module = Module, args = Args }) ->
    proplists:get_value(name, Args, Module).

%% 

tokenize_path(Path) ->
    string:tokens(Path,"/").
