-module(aws_request).

%% -export([post/4]).
%% 
%% -spec post(binary(), [{binary(), binary()}], binary(), [any()]) -> ok.
%% post(Url, Headers, Body, _Options) ->
%%     case hackney:post(Url, Headers, Body) of
%%         {ok, 200, RespHeaders, ClientRef} ->
%%             ?debugVal(RespHeaders),
%%             ok;
%%         {ok, _StatusCode, _RespHeaders, _ClientRef} ->
%%             ?debugVal(_StatusCode),
%%             ?debugVal(_RespHeaders),
%%             error(not_implemented)
%%     end.

