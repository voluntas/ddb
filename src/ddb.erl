-module(ddb).

-export([connection/4]).
-export([connection_local/0, connection_local/2]).

-export([create_table/4]).
-export([delete_item/4]).
-export([delete_table/2]).
-export([get_item/4]).
-export([get_item/6]).
-export([list_tables/1]).
-export([put_item/3]).
-export([update_item/5]).
-export([scan/2, scan/3, scan/4, scan/6]).

-export_type([config/0]).

-include_lib("eunit/include/eunit.hrl").

-define(SERVICE, <<"dynamodb">>).

-record(ddb_config, {
          access_key_id :: binary(),
          secret_access_key :: binary(),
          is_secure = true :: boolean(),
          endpoint :: binary(),
          service :: binary(),
          region :: binary(),

          local = false :: boolean(),
          host :: binary(),
          port :: inet:port_number()
         }).

-type config() :: #ddb_config{}.


%% http://docs.aws.amazon.com/general/latest/gr/rande.html#ddb_region

%% XXX(nakai): サービスの扱いをどうするか考える

-spec connection(binary(), binary(), binary(), boolean()) -> #ddb_config{}.
connection(AccessKeyId, SecretAccessKey, Region, IsSecure) ->
    #ddb_config{
       access_key_id = AccessKeyId,
       secret_access_key = SecretAccessKey,
       region = Region,
       is_secure = IsSecure,
       service = ?SERVICE,
       endpoint = endpoint(?SERVICE, Region)
      }.


-spec endpoint(binary(), binary()) -> binary().
endpoint(Service, Region) ->
    <<Service/binary, $., Region/binary, $., "amazonaws.com">>.


connection_local() ->
    connection_local(<<"127.0.0.1">>, 8000).

connection_local(Host, Port) ->
    #ddb_config{
       host = Host,
       port = Port,
       access_key_id = <<"ACCESS_KEY_ID">>,
       secret_access_key = <<"SECRET_ACCESS_KEY">>,
       endpoint = <<Host/binary, $:, (integer_to_binary(Port))/binary>>,
       region = <<"ap-northeast-1">>,
       service = ?SERVICE,
       local = true,
       is_secure = false
      }.


-spec put_item(#ddb_config{}, binary(), [{binary(), binary()}]) -> ok | {error, term()}.
put_item(Config, TableName, Item) ->
    Target = x_amz_target(put_item),
    Payload = put_item_payload(TableName, Item),
    case post(Config, Target, Payload) of
        {ok, _Json} ->
            ok;
        {error, Reason} ->
            ?debugVal(Reason),
            {error, Reason}
    end.


put_item_payload(TableName, Item) ->
    F = fun({Name, _Value}) ->
           %% FIXME(nakai): 上書き禁止を固定している
           {Name, [{<<"Exists">>, false}]}
        end,
    Expected = lists:map(F, Item),
    Json = [{<<"TableName">>, TableName},
            {<<"Expected">>, Expected},
            {<<"Item">>, typed_item(Item)}],
    jsone:encode(Json).


%% テーブルの主キーはhashタイプ(1要素主キー)と、hash-and-rangeタイプ(2要素で主キー)があり得る
%% http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html
-spec get_item(#ddb_config{}, binary(), binary(), binary()) -> not_found | [{binary(), binary()}].
get_item(Config, TableName, HashKey, HashValue) ->
    Target = x_amz_target(get_item),
    Payload = get_item_payload(TableName, HashKey, HashValue),
    get_item_request(Config, Target, Payload).

-spec get_item(#ddb_config{}, binary(), binary(), binary(), binary(), binary()) -> not_found | [{binary(), binary()}].
get_item(Config, TableName, HashKey, HashValue, RangeKey, RangeValue) ->
    Target = x_amz_target(get_item),
    Payload = get_item_payload(TableName, HashKey, HashValue, RangeKey, RangeValue),
    get_item_request(Config, Target, Payload).


get_item_request(Config, Target, Payload) ->
    case post(Config, Target, Payload) of
        {ok, [{}]} ->
            not_found;
        {ok, Json} ->
            %% XXX(nakai): Item はあえて出している
            cast_item(proplists:get_value(<<"Item">>, Json));
        {error, Reason} ->
            ?debugVal(Reason),
            error(Reason)
    end.


get_item_payload(TableName, HashKey, HashValue) ->
    %% http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html
    Json = [{<<"TableName">>, TableName},
            {<<"Key">>, typed_item([{HashKey, HashValue}])},
            {<<"ConsistentRead">>, true}],
    jsone:encode(Json).

get_item_payload(TableName, HashKey, HashValue, RangeKey, RangeValue) ->
    %% http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html
    Json = [{<<"TableName">>, TableName},
            {<<"Key">>, typed_item([{HashKey, HashValue}, {RangeKey, RangeValue}])},
            {<<"ConsistentRead">>, true}],
    jsone:encode(Json).


typed_item(Item) ->
    lists:map(fun typed_attribute/1, Item).


typed_attribute({Key, Value}) ->
    {Key, typed_value(Value)}.


typed_value(Value) when is_binary(Value) ->
    [{<<"S">>, Value}];
typed_value(Value) when is_integer(Value) ->
    [{<<"N">>, integer_to_binary(Value)}].


-spec list_tables(#ddb_config{}) -> [binary()].
list_tables(Config) ->
    Target = x_amz_target(list_tables),
    Payload = jsone:encode({[]}),
    case post(Config, Target, Payload) of
        {ok, Json} ->
            proplists:get_value(<<"TableNames">>, Json);
        {error, Reason} ->
            ?debugVal(Reason),
            error(Reason)
    end.


create_table(Config, TableName, AttributeName, KeyType) ->
    Target = x_amz_target(create_table),
    Payload = create_table_payload(TableName, AttributeName, KeyType),
    case post(Config, Target, Payload) of
        {ok, _Json} ->
            ok;
        {error, Reason} ->
            ?debugVal(Reason),
            error(Reason)
    end.


%% KeyType HASH RANGE
create_table_payload(TableName, AttributeName, KeyType) ->
    %% http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html
    Json = [
        {
            <<"TableName">>, TableName
        },
        {
            <<"AttributeDefinitions">>,
            [[
                {<<"AttributeName">>, AttributeName},
                {<<"AttributeType">>, <<"S">>}
            ]]
        },
        {
            <<"ProvisionedThroughput">>,
            [
                {<<"ReadCapacityUnits">>, 1},
                {<<"WriteCapacityUnits">>, 1}
            ]
        },
        {
            <<"KeySchema">>,
            [[
                {<<"AttributeName">>, AttributeName},
                {<<"KeyType">>, KeyType}
            ]]
        }
    ],
    jsone:encode(Json).


delete_item(Config, TableName, Key, Value) ->
    Target = x_amz_target(delete_item),
    Payload = delete_item_payload(TableName, Key, Value),
    case post(Config, Target, Payload) of
        {ok, _Json} ->
            ok;
        {error, Reason} ->
            ?debugVal(Reason),
            error(Reason)
    end.


%% FIXME(nakai): S/N しかない
delete_item_payload(TableName, Key, Value) when is_binary(Value) ->
    delete_item_payload(TableName, Key, <<"S">>, Value);
delete_item_payload(TableName, Key, Value) when is_binary(Value) ->
    delete_item_payload(TableName, Key, <<"N">>, Value).

delete_item_payload(TableName, Key, Type, Value) ->
    Json = [{<<"TableName">>, TableName},
            {<<"Key">>, [{Key, [{Type, Value}]}]}],
    jsone:encode(Json).


delete_table(Config, TableName) ->
    Target = x_amz_target(delete_table),
    Payload = delete_table_payload(TableName),
    case post(Config, Target, Payload) of
	{ok, _JSON} ->
	    ok;
	{error, Reason} ->
	    error(Reason)
    end.


delete_table_payload(TableName) ->
    Json = [{<<"TableName">>, TableName}],
    jsone:encode(Json).


-spec update_item(#ddb_config{}, binary(), binary(), binary(), [{binary(), binary(), binary()}]) -> term().
update_item(Config, TableName, Key, Value, AttributeUpdates) ->
    Target = x_amz_target(update_item),
    Payload = update_item_payload(TableName, Key, Value, AttributeUpdates),
    case post(Config, Target, Payload) of
        {ok, _Json} ->
            ok;
        {error, Reason} ->
            ?debugVal(Reason),
            error(Reason)
    end.


%% AttributeUpdates [{AttributeName, Action, Value}] 
update_item_payload(TableName, Key, Value, AttributeUpdates) when is_binary(Value) ->
    update_item_payload(TableName, Key, <<"S">>, Value, AttributeUpdates);
update_item_payload(TableName, Key, Value, AttributeUpdates) when is_integer(Value) ->
    update_item_payload(TableName, Key, <<"N">>, Value, AttributeUpdates).


update_item_payload(TableName, Key, Type, Value, AttributeUpdates) ->
    F = fun({AttributeName, Action, V}) when is_binary(V) ->
                {AttributeName, [{<<"Action">>, Action},
                                 {<<"Value">>, [{<<"S">>, V}]}]};
           ({AttributeName, Action, V}) when is_integer(V) ->
                {AttributeName, [{<<"Action">>, Action},
                                 {<<"Value">>, [{<<"N">>, integer_to_binary(V)}]}]}
        end,
    AttributeUpdates1 = lists:map(F, AttributeUpdates),
    Json = [{<<"TableName">>, TableName},
            {<<"Key">>, [{Key, [{Type, Value}]}]},
            {<<"AttributeUpdates">>, AttributeUpdates1}],
    jsone:encode(Json).


-spec scan(#ddb_config{}, binary()) -> not_found | [{binary(), binary()}].
scan(Config, TableName) ->
  {Items, undefined} = scan(Config, TableName, undefined),
  Items.

-spec scan(#ddb_config{}, binary(), integer()) -> {not_found | [{binary(), binary()}], undefined | binary()}.
scan(Config, TableName, Limit) ->
  scan(Config, TableName, Limit, undefined).

-spec scan(#ddb_config{}, binary(), integer(), binary()) -> {not_found | [{binary(), binary()}], undefined | binary()}.
scan(Config, TableName, Limit, ExclusiveStartKey) ->
  scan(Config, TableName, Limit, ExclusiveStartKey, undefined, undefined).

-spec scan(#ddb_config{}, binary(), integer(), binary(), binary(), [{binary(), binary()}]) -> {[{binary(), binary()}], undefined | binary()}.
scan(Config, TableName, Limit, ExclusiveStartKey, FilterExpression, ExpressionAttributeValues) ->
    Target = x_amz_target(scan),
    Payload = scan_payload(TableName, Limit, ExclusiveStartKey, FilterExpression, ExpressionAttributeValues),
    scan_request(Config, Target, Payload).


scan_payload(TableName, Limit, ExclusiveStartKey, FilterExpression, ExpressionAttributeValues) ->
    Json = [{<<"TableName">>, TableName},
            {<<"ReturnConsumedCapacity">>, <<"TOTAL">>}],
    JsonWithLimit = add_limit_to_scan_payload(Json, Limit),
    JsonWithExclusiveStartKey = add_exclusive_start_key_to_scan_payload(JsonWithLimit, ExclusiveStartKey),
    JsonWithFilter = add_filter_to_scan_payload(JsonWithExclusiveStartKey, FilterExpression, ExpressionAttributeValues),
    jsone:encode(JsonWithFilter).


add_limit_to_scan_payload(Json, undefined) ->
    Json;
add_limit_to_scan_payload(Json, Limit) ->
    [{<<"Limit">>, Limit} | Json].


add_exclusive_start_key_to_scan_payload(Json, undefined) ->
    Json;
add_exclusive_start_key_to_scan_payload(Json, ExclusiveStartKey) ->
    [{<<"ExclusiveStartKey">>, typed_item(ExclusiveStartKey)} | Json].


add_filter_to_scan_payload(Json, undefined, undefined) ->
    Json;
add_filter_to_scan_payload(Json, FilterExpression, ExpressionAttributeValues) ->
    JsonWithExpression = [{<<"FilterExpression">>, FilterExpression} | Json],
    Values = typed_item(ExpressionAttributeValues),
    [{<<"ExpressionAttributeValues">>, Values} | JsonWithExpression].


scan_request(Config, Target, Payload) ->
    case post(Config, Target, Payload) of
        {ok, Json} ->
            Items = proplists:get_value(<<"Items">>, Json),
            LastEvaluatedKey = proplists:get_value(<<"LastEvaluatedKey">>, Json, undefined),
            {cast_items(Items), cast_last_evaluated_key(LastEvaluatedKey)};
        {error, Reason} ->
            ?debugVal(Reason),
            error(Reason)
    end.
 

cast_last_evaluated_key(undefined) ->
    undefined;
cast_last_evaluated_key(LastEvaluatedKey) ->
    cast_item(LastEvaluatedKey).


cast_items(Items) ->
    lists:map(fun cast_item/1, Items).


cast_item(Item) ->
    lists:map(fun cast_attribute/1, Item).


cast_attribute({AttributeName, [{<<"N">>, V}]}) ->
    {AttributeName, binary_to_integer(V)};
cast_attribute({AttributeName, [{_T, V}]}) ->
    {AttributeName, V}.


-spec x_amz_target(atom()) -> binary().
x_amz_target(batch_get_item) ->
    error(not_implemented);
x_amz_target(batch_write_item) ->
    error(not_implemented);
x_amz_target(create_table) ->
    <<"DynamoDB_20120810.CreateTable">>;
x_amz_target(delete_item) ->
    <<"DynamoDB_20120810.DeleteItem">>;
x_amz_target(delete_table) ->
    <<"DynamoDB_20120810.DeleteTable">>;
x_amz_target(describe_table) ->
    error(not_implemented);
x_amz_target(get_item) ->
    <<"DynamoDB_20120810.GetItem">>;
x_amz_target(list_tables) ->
    <<"DynamoDB_20120810.ListTables">>;
x_amz_target(put_item) ->
    <<"DynamoDB_20120810.PutItem">>;
x_amz_target(query) ->
    error(not_implemented);
x_amz_target(scan) ->
    <<"DynamoDB_20120810.Scan">>;
x_amz_target(update_item) ->
    <<"DynamoDB_20120810.UpdateItem">>;
x_amz_target(update_table) ->
    error(not_implemented);
x_amz_target(_OperationName) ->
    error({not_implemented, _OperationName}).


url(true, Endpoint) ->
    <<"https://", Endpoint/binary>>;
url(false, Endpoint) ->
    <<"http://", Endpoint/binary>>.


post(#ddb_config{
        access_key_id = AccessKeyId,
        secret_access_key = SecretAccessKey,
        service = Service,
        region = Region,
        endpoint = Endpoint,
        is_secure = IsSecure
       }, Target, Payload) ->
    Headers0 = [{<<"x-amz-target">>, Target}, 
                {<<"host">>, Endpoint}],
    DateTime = aws:iso_8601_basic_format(os:timestamp()),
    Headers = aws:signature_version_4_signing(DateTime, AccessKeyId, SecretAccessKey, Headers0,
                                              Payload, Service, Region),
    Headers1 = [{<<"accept-encoding">>, <<"identity">>},
                {<<"content-type">>, <<"application/x-amz-json-1.0">>}|Headers],

    Url = url(IsSecure, Endpoint),

    case hackney:post(Url, Headers1, Payload, [{pool, default}]) of
        {ok, 200, _RespHeaders, ClientRef} ->
            {ok, Body} = hackney:body(ClientRef),
            hackney:close(ClientRef),
            {ok, jsone:decode(Body, [{object_format, proplist}])};
        {ok, _StatusCode, _RespHeaders, ClientRef} ->
            {ok, Body} = hackney:body(ClientRef),
            Json = jsone:decode(Body, [{object_format, proplist}]),
            Type = proplists:get_value(<<"__type">>, Json),
            Message = proplists:get_value(<<"message">>, Json),
            hackney:close(ClientRef),
            {error, {Type, Message}}
    end.


