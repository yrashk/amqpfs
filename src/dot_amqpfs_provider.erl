-module(dot_amqpfs_provider). 

-export([init/1, 
         list_dir/2,
         open/3,
         object/2,
         mtime/2,
         allow_request/1]).

-include_lib("amqpfs/include/amqpfs.hrl").

-define(AMQPFS_STATE, (State#amqpfs_provider_state.extra)).

init(State) ->
    amqpfs ! {deliver_state, self()},
    AmqpfsState =
    receive
        {amqpfs_state, RequestedState} ->
            RequestedState
    end,
    amqpfs_provider:announce(directory, "/.amqpfs", State),
    State#amqpfs_provider_state{ extra = AmqpfsState }.

list_dir("/.amqpfs", _State) ->
    [{"version", {file, on_demand}},{"providers",{directory, on_demand}}];

list_dir("/.amqpfs/providers", _State) ->
    [{"instances", {directory, on_demand}},{"applications",{directory, on_demand}}];

list_dir("/.amqpfs/providers/instances", State) ->
    Providers = (?AMQPFS_STATE)#amqpfs.providers,
    lists:map(fun ([Provider]) -> {binary_to_list(Provider), {directory, on_demand}} end, ets:match(Providers, {'$1','_','_'}));

list_dir("/.amqpfs/providers/applications", State) ->
    Providers = (?AMQPFS_STATE)#amqpfs.providers,
    lists:ukeysort(1, lists:map(fun ([Provider]) -> {binary_to_list(Provider), {directory, on_demand}} end, ets:match(Providers, {'_','$1','_'})));

list_dir("/.amqpfs/providers/instances/" ++ _Provider, _State) ->
    [{"application", {file, on_demand}}, {"announcements", {file, on_demand}}];

list_dir("/.amqpfs/providers/applications/" ++ ApplicationKey, State) ->
    list_applications(string:tokens(ApplicationKey, "/"), State).


list_applications([Application], State) ->
    Providers = (?AMQPFS_STATE)#amqpfs.providers,
    lists:map(fun ([Provider]) -> {binary_to_list(Provider), {directory, on_demand}} end, ets:match(Providers, {'$1',list_to_binary(Application),'_'}));

list_applications([_Application,Provider], State) ->
    list_dir("/.amqpfs/providers/instances/" ++ Provider, State).


open("/.amqpfs/version", _Fi, _State) ->
    ok.

object("/.amqpfs/version", _State) ->
    ?AMQPFS_VERSION;

object("/.amqpfs/providers/instances/" ++ ProviderKey, State) ->
    render_providers(string:tokens(ProviderKey, "/"), State);

object("/.amqpfs/providers/applications/" ++ ApplicationKey, State) ->
    render_providers(tl(string:tokens(ApplicationKey,"/")), State).

render_providers([Provider, "application"], State) ->
    Providers = (?AMQPFS_STATE)#amqpfs.providers,
    case ets:lookup(Providers, list_to_binary(Provider)) of
        [] ->
            "";
        [{_, Application, _}] ->
            Application
    end;
render_providers([Provider, "announcements"], State) ->
    Announcements = (?AMQPFS_STATE)#amqpfs.announcements,
    lists:flatten(string:join(ets:match(Announcements, {'$1', '_', list_to_binary(Provider), '_'}),[10]));

render_providers(_, _) -> % this is because default getattr will use default size which will use object...
    "".

mtime("/.amqpfs/providers/instances/" ++ ProviderKey, State) ->
    render_mtime(string:tokens(ProviderKey, "/"), State);
mtime("/.amqpfs/providers/applications/" ++ ApplicationKey=Path, State) ->
    case tl(string:tokens(ApplicationKey, "/")) of
        [] ->
            amqpfs_provider_base:mtime(Path, State);
        TL ->
            render_mtime(TL, State)
    end.

render_mtime([Provider|_], State) ->
    Providers = (?AMQPFS_STATE)#amqpfs.providers,
    case ets:lookup(Providers, list_to_binary(Provider)) of
        [] ->
            "";
        [{_, _, LastUpdate}] ->
            amqpfs_util:unixtime_to_datetime(LastUpdate)
    end.

allow_request(#amqpfs_provider_state{request_headers = Headers}) ->
    {ok, Hostname} = inet:gethostname(),
    HostnameBin = list_to_binary(Hostname),
    case lists:keysearch(<<"hostname">>, 1, Headers) of 
        {value, {<<"hostname">>, _, HostnameBin}} ->
            true;
        _ ->
            false
    end.
