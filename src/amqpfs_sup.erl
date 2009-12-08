-module(amqpfs_sup).
-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link(_Args) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init ([]) ->
    Srv = {amqpfs_server, {amqpfs_server, start_link, []},
           permanent, 10000, worker, [ amqpfs_server ]},
    {ok,{{one_for_one,3,10}, [Srv]}}.
