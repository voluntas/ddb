-module(ddb).

-export([connection/4]).
-export([connection_local/0, connection_local/2]).

-export([put_item/3]).
-export([get_item/4]).
-export([list_tables/1]).
-export([create_table/4]).
-export([delete_table/2]).
-export([update_item/5]).

-include_lib("eunit/include/eunit.hrl").

-record(ddb_connection, {access_key_id :: binary(),
                         secret_access_key :: binary(),
                         is_secure = true :: boolean(),
                         endpoint :: binary(),
                         service = <<"dynamodb">> :: binary(),
                         local = false :: boolean(),
                         host :: binary(),
                         port :: inet:port_number()}).


%% とにかく低機能で使えるヤツを作る
%% - TCP は毎回開く
%% - app.config に設定を保持する

%% 設定は一回 app.config に逃すようにする

-spec connection(binary(), binary(), binary(), boolean()) -> #ddb_connection{}.
connection(AccessKeyId, SecretAccessKey, Region, IsSecure) ->
    #ddb_connection{access_key_id = AccessKeyId,
                    secret_access_key = SecretAccessKey,
                    is_secure = IsSecure,
                    endpoint = host(<<"dynamodb">>, Region)}. 


connection_local() ->
    connection_local(<<"127.0.0.1">>, 8000).

connection_local(Host, Port) ->
    #ddb_connection{host = Host,
                    port = Port,
                    access_key_id = <<"ACCESS_KEY_ID">>,
                    secret_access_key = <<"SECRET_ACCESS_KEY">>,
                    endpoint = <<Host/binary, $:, (integer_to_binary(Port))/binary>>,
                    local = true,
                    is_secure = false}. 

-spec put_item(#ddb_connection{}, binary(), [{binary(), binary()}]) -> ok.
put_item(#ddb_connection{access_key_id = AccessKeyId,
                         secret_access_key = SecretAccessKey,
                         service = Service}, TableName, Item) ->
    Region = <<"ap-northeast-1">>,
    Target = x_amz_target(put_item),
    Host = <<"127.0.0.1:8000">>,
    %% Host = host(Service, Region),
    Payload = put_item_payload(TableName, Item),
    post(AccessKeyId, SecretAccessKey, Host, Payload, Service, Region, Target).


put_item_payload(TableName, Item) ->
    F = fun({Name, Value}) when is_binary(Value) ->
                {Name, [{<<"S">>, Value}]}
        end,
    Item1 = lists:map(F, Item),

    Json = [{<<"TableName">>, TableName},
            {<<"Item">>, Item1}],
    jsonx:encode(Json).


-spec get_item(#ddb_connection{}, binary(), binary(), binary()) -> not_found | [{binary(), binary()}].
get_item(#ddb_connection{access_key_id = AccessKeyId,
                         secret_access_key = SecretAccessKey,
                         service = Service}, TableName, Key, Value) ->
    Region = <<"ap-northeast-1">>,
    Target = x_amz_target(get_item),
    Host = <<"127.0.0.1:8000">>,
    %% Host = host(Service, Region),
    Payload = get_item_payload(TableName, Key, Value),
    post(AccessKeyId, SecretAccessKey, Host, Payload, Service, Region, Target).

x_amz_target(batch_get_item) ->
    error(not_implemented);
x_amz_target(batch_write_item) ->
    error(not_implemented);
x_amz_target(create_table) ->
    <<"DynamoDB_20120810.CreateTable">>;
x_amz_target(delete_item) ->
    error(not_implemented);
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
    error(not_implemented);
x_amz_target(update_item) ->
    <<"DynamoDB_20120810.UpdateItem">>;
x_amz_target(update_table) ->
    error(not_implemented);
x_amz_target(_OperationName) ->
    error({not_implemented, _OperationName}).

host(Service, Region) ->
    <<Service/binary, $., Region, $., "amazonaws.com">>.


post(AccessKeyId, SecretAccessKey, Host, Payload, Service, Region, Target) ->
    Headers0 = [{<<"x-amz-target">>, Target}, 
                {<<"host">>, Host}],
    DateTime = aws:iso_8601_basic_format(os:timestamp()),
    Headers = aws:signature_version_4_signing(DateTime, AccessKeyId, SecretAccessKey, Headers0,
                                              Payload, Service, Region),
    Headers1 = [{<<"accept-encoding">>, <<"identity">>},
                {<<"content-type">>, <<"application/x-amz-json-1.0">>}|Headers],
    case hackney:post(Host, Headers1, Payload) of
        {ok, 200, _RespHeaders, ClientRef} ->
            {ok, Body} = hackney:body(ClientRef),
            jsonx:decode(Body, [{format, proplist}]);
        {ok, _StatusCode, _RespHeaders, ClientRef} ->
            io:format("~p~n", [hackney:body(ClientRef)]),
            ?debugVal(_StatusCode),
            ?debugVal(_RespHeaders),
            error(not_implemented)
    end.

%% get_item_payload(TableName, Key, Value, AttributesToGet) ->
get_item_payload(TableName, Key, Value) ->
    %% http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html
    Json = [{<<"TableName">>, TableName},
            {<<"Key">>, [{Key, [{<<"S">>, Value}]}]},
            {<<"ConsistentRead">>, true}],
            %% {<<"ReturnConsumedCapacity">>, <<"TOTAL">>}
    jsonx:encode(Json).


list_tables(#ddb_connection{access_key_id = AccessKeyId,
                            secret_access_key = SecretAccessKey,
                            service = Service}) ->
    Region = <<"ap-northeast-1">>,
    Target = x_amz_target(list_tables),
    Host = <<"127.0.0.1:8000">>,
    %% Host = host(Service, Region),
    Payload = jsonx:encode({[]}),
    post(AccessKeyId, SecretAccessKey, Host, Payload, Service, Region, Target).


create_table(#ddb_connection{access_key_id = AccessKeyId,
                             secret_access_key = SecretAccessKey,
                             service = Service}, TableName, AttributeName, KeyType) ->
    Region = <<"ap-northeast-1">>,
    Target = x_amz_target(create_table),
    Host = <<"127.0.0.1:8000">>,
    %% Host = host(Service, Region),
    Payload = create_table_payload(TableName, AttributeName, KeyType),
    post(AccessKeyId, SecretAccessKey, Host, Payload, Service, Region, Target).

%% KeyType HASH RANGE
create_table_payload(TableName, AttributeName, KeyType) ->
    %% http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html
    Json = [{<<"TableName">>, TableName},
            {<<"AttributeDefinitions">>, [
                                          [{<<"AttributeName">>, AttributeName},
                                           {<<"AttributeType">>, <<"S">>}
                                          ]
                                         ]
            },
            {<<"ProvisionedThroughput">>, [{<<"ReadCapacityUnits">>, 1},
                                           {<<"WriteCapacityUnits">>, 1}
                                          ]
            },
            {<<"KeySchema">>, [
                               [{<<"AttributeName">>, AttributeName},
                                {<<"KeyType">>, KeyType}]
                              ]
            }
           ],
    jsonx:encode(Json).


delete_table(#ddb_connection{access_key_id = AccessKeyId,
                             secret_access_key = SecretAccessKey,
                             service = Service}, TableName) ->
    Region = <<"ap-northeast-1">>,
    Target = x_amz_target(delete_table),
    Host = <<"127.0.0.1:8000">>,
    %% Host = host(Service, Region),
    Payload = delete_table_payload(TableName),
    post(AccessKeyId, SecretAccessKey, Host, Payload, Service, Region, Target).


delete_table_payload(TableName) ->
    Json = [{<<"TableName">>, TableName}],
    jsonx:encode(Json).


update_item(#ddb_connection{access_key_id = AccessKeyId,
                            secret_access_key = SecretAccessKey,
                            service = Service}, TableName, Key, Value, AttributeUpdates) ->
    Region = <<"ap-northeast-1">>,
    Target = x_amz_target(update_item),
    Host = <<"127.0.0.1:8000">>,
    %% Host = host(Service, Region),
    Payload = update_item_payload(TableName, Key, Value, AttributeUpdates),
    ?debugVal(Payload),
    post(AccessKeyId, SecretAccessKey, Host, Payload, Service, Region, Target).


%% AttributeUpdates [{AttributeName, Action, Value}] 
update_item_payload(TableName, Key, Value, AttributeUpdates) ->
    F = fun({AttributeName, Action, V}) when is_binary(Value) ->
                %% FIXME(nakai): S 固定
                {AttributeName, [{<<"Action">>, Action}, {<<"Value">>, [{<<"S">>, V}]}]}
        end,
    AttributeUpdates1 = lists:map(F, AttributeUpdates),
    ?debugVal(AttributeUpdates1),
    %% FIXME(nakai): S 固定
    Json = [{<<"TableName">>, TableName},
            {<<"Key">>, [{Key, [{<<"S">>, Value}]}]},
            {<<"AttributeUpdates">>, AttributeUpdates1}],
    ?debugVal(Json),
    jsonx:encode(Json).




-ifdef(TEST).

get_item_test() ->
    application:start(crypto),
    application:start(asn1),
    application:start(public_key),
    application:start(ssl),
    application:start(mimetypes),
    application:start(hackney_lib),
    application:start(hackney),

    C = ddb:connection_local(<<"localhost">>, 8000),
    ?assertEqual([{<<"TableNames">>, []}], ddb:list_tables(C)),
    ddb:create_table(C, <<"users">>, <<"user_id">>, <<"HASH">>),
    ddb:put_item(C, <<"users">>, [{<<"user_id">>, <<"USER-ID">>},
                                  {<<"password">>, <<"PASSWORD">>},
                                  {<<"gender">>, <<"GENDER">>}]),
    ddb:update_item(C, <<"users">>, <<"user_id">>, <<"USER-ID">>, [{<<"gender">>, <<"PUT">>, <<"gender">>}]),
    ddb:get_item(C, <<"users">>, <<"user_id">>, <<"USER-ID">>),
    ddb:delete_table(C, <<"users">>),

    application:stop(crypto),
    application:stop(asn1),
    application:stop(public_key),
    application:stop(ssl),
    application:stop(mimetypes),
    application:stop(hackney_lib),
    application:stop(hackney),

    ok.

-endif.
