#################################################
とりあえず動く DynamoDB クライアント (Erlang/OTP)
#################################################

.. image:: https://circleci.com/gh/voluntas/ddb.svg?style=svg
    :target: https://circleci.com/gh/voluntas/ddb

注意
====

- とにかく動かす事を目的として書かれています
- コードはかなりやっつけです
- テストもかなりやっつけです
- 必要最低限の機能だけ実装しています
- 今後大幅に実装方法が変更される可能性があります
- リポジトリが突然消えることがあります

特徴
====

- 処理を全てバイナリで扱っている
- DynamoDB API の最新バージョンに対応している
- Signature version 4 に対応している

AmazonDynamo Local
==================

docker を使っています。

:URL: https://registry.hub.docker.com/u/tray/dynamodb-local/

DynamoDB local 起動::

    make start_dynamodb_local

DynamoDB local 終了::

    make stop_dynamodb_local


実装済
======

.. code-block:: erlang

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
