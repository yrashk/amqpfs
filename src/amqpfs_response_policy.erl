-module(amqpfs_response_policy).
-export([first/3, all/3, number/4, percentile/4]).

-include_lib("amqpfs/include/amqpfs.hrl").

first(_Route, _Response, _State) ->
    last_response.

all(Route, Response, #amqpfs{ response_routes = Routes, announcements = Announcements } = State) ->
    case ets:lookup(Routes, Route) of
        [{Route, _Pid, Path, _Command}] ->
            NumberOfAnnounces =
            case ets:lookup(Announcements, amqpfs_server:path_to_announced(Path, State)) of
                Announces when is_list(Announces) ->
                    length(Announces);
                _ ->
                    0
            end,
            number(NumberOfAnnounces, Route, Response, State);
        _ ->
            ignore
    end.

percentile(Percentile, Route, Response, #amqpfs{ response_routes = Routes, announcements = Announcements } = State) ->
    case ets:lookup(Routes, Route) of
        [{Route, _Pid, Path, _Command}] ->
            NumberOfAnnounces =
                case ets:lookup(Announcements, amqpfs_server:path_to_announced(Path, State)) of
                    Announces when is_list(Announces) ->
                    length(Announces);
                    _ ->
                        0
                end,
            number(round(NumberOfAnnounces/100*Percentile), Route, Response, State);
        _ ->
            ignore
    end.

number(Num, Route, _Response,  #amqpfs{ response_buffers = ResponseBuffers } = _State) ->
    NumberOfResponses =
        case ets:lookup(ResponseBuffers, Route) of
            Buffers when is_list(Buffers) ->
                length(Buffers);
            _ ->
                0
        end,
    case NumberOfResponses of
        Num ->
            last_response;
        _ ->
            continue
    end.
