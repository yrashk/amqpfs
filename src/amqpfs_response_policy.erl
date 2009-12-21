-module(amqpfs_response_policy).
-export([first/3, all/3]).
-export([new/1]).

-include_lib("amqpfs/include/amqpfs.hrl").

first(_Route, _Response, _State) ->
    last_response.

all(Route, _Response, #amqpfs{ response_routes = Routes, response_buffers = ResponseBuffers, announcements = Announcements } = State) ->
    case ets:lookup(Routes, Route) of
        [{Route, _Pid, Path, _Command}] ->
            NumberOfResponses =
            case ets:lookup(ResponseBuffers, Route) of
                Buffers when is_list(Buffers) ->
                    length(Buffers);
                _ ->
                    0
            end,
            NumberOfAnnounces =
            case ets:lookup(Announcements, amqpfs_server:path_to_announced(Path, State)) of
                Announces when is_list(Announces) ->
                    length(Announces);
                _ ->
                    0
            end,
            case NumberOfResponses of
                NumberOfAnnounces ->
                    last_response;
                _ ->
                    continue
            end;
        _ ->
            ignore
    end.
    
    
%%

new(PropList) ->
    lists:ukeysort(1,PropList ++ ?DEFAULT_RESPONSE_POLICIES).
