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
    Inode = {amqpfs_inode, {amqpfs_inode, start_link, []},
             permanent, 3000, worker, [ amqpfs_inode ]},
    RootAmqpfsProvider = {root_amqpfs_provider, {amqpfs_provider, start_link, [root_amqpfs_provider]},
                          permanent, 3000, worker, [ root_amqpfs_provider ]},
    DotAmqpfsProvider = {dot_amqpfs_provider, {amqpfs_provider, start_link, [dot_amqpfs_provider]},
                         permanent, 3000, worker, [ dot_amqpfs_provider ]},
    Srv = {amqpfs_server, {amqpfs_server, start_link, []},
           permanent, 10000, worker, [ amqpfs_server ]},
    {ok,{{one_for_one,3,10}, [Inode, RootAmqpfsProvider, DotAmqpfsProvider, Srv]}}.
