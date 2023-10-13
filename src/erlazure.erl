%% Copyright (c) 2013 - 2015, Dmintry Kataskin
%% All rights reserved.
%%
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted provided that the following conditions are met:
%%
%% * Redistributions of source code must retain the above copyright notice,
%% this list of conditions and the following disclaimer.
%% * Redistributions in binary form must reproduce the above copyright
%% notice, this list of conditions and the following disclaimer in the
%% documentation and/or other materials provided with the distribution.
%% * Neither the name of erlazure nor the names of its contributors may be used to
%% endorse or promote products derived from this software without specific
%% prior written permission.
%%
%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
%% AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
%% ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
%% LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
%% CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
%% SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
%% INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
%% CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
%% ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
%% POSSIBILITY OF SUCH DAMAGE.

%% ============================================================================
%% Azure Storage API.
%% ============================================================================

-module(erlazure).
-author("Dmitry Kataskin").

-include("erlazure.hrl").

-define(json_content_type, "application/json").
-define(gen_server_call_default_timeout, 30000).

-behaviour(gen_server).

%% API
-export([start_link/3]).

%% Queue API
-export([list_queues/1, list_queues/2, list_queues/3]).
-export([set_queue_acl/3, set_queue_acl/4, set_queue_acl/5]).
-export([get_queue_acl/2, get_queue_acl/3, get_queue_acl/4]).
-export([create_queue/2, create_queue/3, create_queue/4]).
-export([delete_queue/2, delete_queue/3, delete_queue/4]).
-export([put_message/3, put_message/4, put_message/5]).
-export([get_messages/2, get_messages/3, get_messages/4]).
-export([peek_messages/2, peek_messages/3, peek_messages/4]).
-export([delete_message/4, delete_message/5, delete_message/6]).
-export([clear_messages/2, clear_messages/3, clear_messages/4]).
-export([update_message/4, update_message/5, update_message/6]).

%% Blob API
-export([list_containers/1, list_containers/2, list_containers/3]).
-export([create_container/2, create_container/3, create_container/4]).
-export([delete_container/2, delete_container/3, delete_container/4]).
-export([lease_container/3, lease_container/4, lease_container/5]).
-export([list_blobs/2, list_blobs/3, list_blobs/4]).
-export([put_block_blob/4, put_block_blob/5, put_block_blob/6]).
-export([put_page_blob/4, put_page_blob/5, put_page_blob/6]).
-export([get_blob/3, get_blob/4, get_blob/5]).
-export([snapshot_blob/3, snapshot_blob/4, snapshot_blob/5]).
-export([copy_blob/4, copy_blob/5, copy_blob/6]).
-export([delete_blob/3, delete_blob/4, delete_blob/5]).
-export([put_block/5, put_block/6, put_block/7]).
-export([put_block_list/4, put_block_list/5, put_block_list/6]).
-export([get_block_list/3, get_block_list/4, get_block_list/5]).
-export([acquire_blob_lease/4, acquire_blob_lease/5, acquire_blob_lease/7]).

%% Table API
-export([list_tables/1, list_tables/3, new_table/2, delete_table/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(st, {
    account = "",
    key = "",
    service = undefined,
    service_context = undefined,
    param_specs = [],
    conn_pid = undefined,
    conn_ref = undefined
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(string(), string(), atom()) -> {ok, pid()}.
start_link(Account, Key, Context) ->
    gen_server:start_link(?MODULE, {Account, Key, Context}, []).

%%====================================================================
%% Queue
%%====================================================================

-spec list_queues(pid()) -> enum_parse_result(queue()).
list_queues(Pid) ->
    list_queues(Pid, []).

-spec list_queues(pid(), common_opts()) -> enum_parse_result(queue()).
list_queues(Pid, Options) ->
    list_queues(Pid, Options, ?gen_server_call_default_timeout).

-spec list_queues(pid(), common_opts(), pos_integer()) -> enum_parse_result(queue()).
list_queues(Pid, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
    Service = ?queue_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{params => [{comp, list}] ++ Options},
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_OK, erlazure_queue:parse_queue_list(Body));
        Error ->
            Error
    end.

-type queue_acl_opts() :: req_param_timeout() | req_param_clientrequestid().
-spec set_queue_acl(pid(), string(), signed_id()) -> {ok, created}.
set_queue_acl(Pid, Queue, SignedId = #signed_id{}) ->
    set_queue_acl(Pid, Queue, SignedId, []).

-spec set_queue_acl(pid(), string(), signed_id(), list(queue_acl_opts())) -> {ok, created}.
set_queue_acl(Pid, Queue, SignedId = #signed_id{}, Options) ->
    set_queue_acl(Pid, Queue, SignedId, Options, ?gen_server_call_default_timeout).

-spec set_queue_acl(pid(), string(), signed_id(), list(queue_acl_opts()), pos_integer()) ->
    {ok, created}.
set_queue_acl(Pid, Queue, SignedId = #signed_id{}, Options, Timeout) when
    is_list(Options); is_integer(Timeout)
->
    Service = ?queue_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                method => ?HTTP_METHOD_PUT,
                path => string:to_lower(Queue),
                body => erlazure_queue:get_request_body(set_queue_acl, SignedId),
                params => [{comp, acl}] ++ Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_NO_CONTENT, created);
        Error ->
            Error
    end.

-spec get_queue_acl(pid(), string()) -> {ok, no_acl} | {ok, signed_id()}.
get_queue_acl(Pid, Queue) ->
    get_queue_acl(Pid, Queue, []).

-spec get_queue_acl(pid(), string(), list(queue_acl_opts())) -> {ok, no_acl} | {ok, signed_id()}.
get_queue_acl(Pid, Queue, Options) ->
    get_queue_acl(Pid, Queue, Options, ?gen_server_call_default_timeout).

-spec get_queue_acl(pid(), string(), list(queue_acl_opts()), pos_integer()) ->
    {ok, no_acl} | {ok, signed_id()}.
get_queue_acl(Pid, Queue, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
    Service = ?queue_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                path => string:to_lower(Queue),
                params => [{comp, acl}] ++ Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(
                Code, Body, St, ?STATUS_CODE_OK, erlazure_queue:parse_queue_acl_response(Body)
            );
        Error ->
            Error
    end.

-spec create_queue(pid(), string()) -> created_response() | already_created_response().
create_queue(Pid, Queue) ->
    create_queue(Pid, Queue, []).
create_queue(Pid, Queue, Options) ->
    create_queue(Pid, Queue, Options, ?gen_server_call_default_timeout).
create_queue(Pid, Queue, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
    Service = ?queue_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                method => ?HTTP_METHOD_PUT,
                path => string:to_lower(Queue),
                params => Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            case Code of
                ?STATUS_CODE_CREATED ->
                    {ok, created};
                ?STATUS_CODE_NO_CONTENT ->
                    {error, already_created};
                _HttpStatus ->
                    {error, Body}
            end;
        Error ->
            Error
    end.

delete_queue(Pid, Queue) ->
    delete_queue(Pid, Queue, []).
delete_queue(Pid, Queue, Options) ->
    delete_queue(Pid, Queue, Options, ?gen_server_call_default_timeout).
delete_queue(Pid, Queue, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
    Service = ?queue_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                method => ?HTTP_METHOD_DELETE,
                path => string:to_lower(Queue),
                params => Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_NO_CONTENT, deleted);
        Error ->
            Error
    end.

put_message(Pid, Queue, Message) ->
    put_message(Pid, Queue, Message, []).
put_message(Pid, Queue, Message, Options) ->
    put_message(Pid, Queue, Message, Options, ?gen_server_call_default_timeout).
put_message(Pid, Queue, Message, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
    Service = ?queue_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                method => ?HTTP_METHOD_POST,
                path => lists:concat([string:to_lower(Queue), "/messages"]),
                body => erlazure_queue:get_request_body(put_message, Message),
                params => Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_CREATED, created);
        Error ->
            Error
    end.

get_messages(Pid, Queue) ->
    get_messages(Pid, Queue, []).
get_messages(Pid, Queue, Options) ->
    get_messages(Pid, Queue, Options, ?gen_server_call_default_timeout).
get_messages(Pid, Queue, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
    Service = ?queue_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                path => string:to_lower(Queue) ++ "/messages",
                params => Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(
                Code, Body, St, ?STATUS_CODE_OK, erlazure_queue:parse_queue_messages_list(Body)
            );
        Error ->
            Error
    end.

peek_messages(Pid, Queue) ->
    peek_messages(Pid, Queue, []).
peek_messages(Pid, Queue, Options) ->
    peek_messages(Pid, Queue, Options, ?gen_server_call_default_timeout).
peek_messages(Pid, Queue, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
    Service = ?queue_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                path => string:to_lower(Queue) ++ "/messages",
                params => [{peek_only, true}] ++ Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(
                Code, Body, St, ?STATUS_CODE_OK, erlazure_queue:parse_queue_messages_list(Body)
            );
        Error ->
            Error
    end.

delete_message(Pid, Queue, MessageId, PopReceipt) ->
    delete_message(Pid, Queue, MessageId, PopReceipt, []).
delete_message(Pid, Queue, MessageId, PopReceipt, Options) ->
    delete_message(Pid, Queue, MessageId, PopReceipt, Options, ?gen_server_call_default_timeout).
delete_message(Pid, Queue, MessageId, PopReceipt, Options, Timeout) when
    is_list(Options); is_integer(Timeout)
->
    Service = ?queue_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                method => ?HTTP_METHOD_DELETE,
                path => lists:concat([string:to_lower(Queue), "/messages/", MessageId]),
                params => [{pop_receipt, PopReceipt}] ++ Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_NO_CONTENT, deleted);
        Error ->
            Error
    end.

clear_messages(Pid, Queue) ->
    clear_messages(Pid, Queue, []).
clear_messages(Pid, Queue, Options) ->
    clear_messages(Pid, Queue, Options, ?gen_server_call_default_timeout).
clear_messages(Pid, Queue, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
    Service = ?queue_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                method => ?HTTP_METHOD_DELETE,
                path => string:to_lower(Queue) ++ "/messages",
                params => Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_NO_CONTENT, deleted);
        Error ->
            Error
    end.

update_message(Pid, Queue, UpdatedMessage = #queue_message{}, VisibilityTimeout) ->
    update_message(Pid, Queue, UpdatedMessage, VisibilityTimeout, []).
update_message(Pid, Queue, UpdatedMessage = #queue_message{}, VisibilityTimeout, Options) ->
    update_message(
        Pid, Queue, UpdatedMessage, VisibilityTimeout, Options, ?gen_server_call_default_timeout
    ).
update_message(
    Pid, Queue, UpdatedMessage = #queue_message{}, VisibilityTimeout, Options, Timeout
) when is_list(Options); is_integer(Timeout) ->
    Service = ?queue_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            Params = [
                {pop_receipt, UpdatedMessage#queue_message.pop_receipt},
                {message_visibility_timeout, integer_to_list(VisibilityTimeout)}
            ],
            ReqOptions = #{
                method => ?HTTP_METHOD_PUT,
                path => lists:concat([
                    string:to_lower(Queue), "/messages/", UpdatedMessage#queue_message.id
                ]),
                body => erlazure_queue:get_request_body(
                    update_message, UpdatedMessage#queue_message.text
                ),
                params => Params ++ Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_NO_CONTENT, updated);
        Error ->
            Error
    end.

%%====================================================================
%% Blob
%%====================================================================
list_containers(Pid) ->
    list_containers(Pid, []).
list_containers(Pid, Options) ->
    list_containers(Pid, Options, ?gen_server_call_default_timeout).
list_containers(Pid, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
    Service = ?blob_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{params => [{comp, list}] ++ Options},
            case
                erlazure_http:request(
                    St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
                )
            of
                {?STATUS_CODE_OK, Body} ->
                    case erlazure_blob:parse_container_list(binary_to_list(Body)) of
                        {ok, Containers} ->
                            {reply, Containers, St};
                        Error ->
                            {reply, Error, St}
                    end;
                {_Code, Body} ->
                    {reply, {error, Body}, St}
            end;
        Error ->
            Error
    end.

create_container(Pid, Name) ->
    create_container(Pid, Name, []).
create_container(Pid, Name, Options) ->
    create_container(Pid, Name, Options, ?gen_server_call_default_timeout).
create_container(Pid, Name, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
    Service = ?blob_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                method => ?HTTP_METHOD_PUT,
                path => Name,
                params => [{res_type, container}] ++ Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            case Code of
                ?STATUS_CODE_CREATED -> {reply, {ok, created}, St};
                _ -> {reply, {error, Body}, St}
            end;
        Error ->
            Error
    end.

delete_container(Pid, Name) ->
    delete_container(Pid, Name, []).
delete_container(Pid, Name, Options) ->
    delete_container(Pid, Name, Options, ?gen_server_call_default_timeout).
delete_container(Pid, Name, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
    Service = ?blob_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                method => ?HTTP_METHOD_DELETE,
                path => Name,
                params => [{res_type, container}] ++ Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_ACCEPTED, deleted);
        Error ->
            Error
    end.

put_block_blob(Pid, Container, Name, Data) ->
    put_block_blob(Pid, Container, Name, Data, []).
put_block_blob(Pid, Container, Name, Data, Options) ->
    put_block_blob(Pid, Container, Name, Data, Options, ?gen_server_call_default_timeout).
put_block_blob(Pid, Container, Name, Data, Options, Timeout) when
    is_list(Options); is_integer(Timeout)
->
    Service = ?blob_service,
    Type = block_blob,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ContentType =
                case proplists:get_value(content_type, Options) of
                    undefined -> "application/octet-stream";
                    Other -> Other
                end,
            ReqOptions = #{
                method => ?HTTP_METHOD_PUT,
                path => lists:concat([Container, "/", Name]),
                body => Data,
                content_type => ContentType,
                params => [{blob_type, Type}] ++ Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_CREATED, created);
        Error ->
            Error
    end.

put_page_blob(Pid, Container, Name, ContentLength) ->
    put_page_blob(Pid, Container, Name, ContentLength, []).
put_page_blob(Pid, Container, Name, ContentLength, Options) ->
    put_page_blob(Pid, Container, Name, ContentLength, Options, ?gen_server_call_default_timeout).
put_page_blob(Pid, Container, Name, ContentLength, Options, Timeout) when
    is_list(Options); is_integer(Timeout)
->
    Service = ?blob_service,
    Type = page_blob,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            Params = [{blob_type, Type}, {blob_content_length, ContentLength}],
            ReqOptions = #{
                method => ?HTTP_METHOD_PUT,
                path => lists:concat([Container, "/", Name]),
                params => Params ++ Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_CREATED, created);
        Error ->
            Error
    end.

list_blobs(Pid, Container) ->
    list_blobs(Pid, Container, []).
list_blobs(Pid, Container, Options) ->
    list_blobs(Pid, Container, Options, ?gen_server_call_default_timeout).
list_blobs(Pid, Container, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
    Service = ?blob_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            Params = [{comp, list}, {res_type, container}],
            ReqOptions = #{path => Container, params => Params ++ Options},
            case
                erlazure_http:request(
                    St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
                )
            of
                {?STATUS_CODE_OK, Body} ->
                    case erlazure_blob:parse_blob_list(Body) of
                        {ok, Blobs} ->
                            {reply, Blobs, St};
                        Error ->
                            {reply, Error, St}
                    end;
                {_Code, Body} ->
                    {reply, {error, Body}, St}
            end;
        Error ->
            Error
    end.

get_blob(Pid, Container, Blob) ->
    get_blob(Pid, Container, Blob, []).
get_blob(Pid, Container, Blob, Options) ->
    get_blob(Pid, Container, Blob, Options, ?gen_server_call_default_timeout).
get_blob(Pid, Container, Blob, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
    Service = ?blob_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                path => lists:concat([Container, "/", Blob]),
                params => Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            case Code of
                ?STATUS_CODE_OK ->
                    {reply, {ok, Body}, St};
                ?STATUS_CODE_PARTIAL_CONTENT ->
                    {reply, {ok, Body}, St};
                _ ->
                    {reply, {error, Body}, St}
            end;
        Error ->
            Error
    end.

snapshot_blob(Pid, Container, Blob) ->
    snapshot_blob(Pid, Container, Blob, []).
snapshot_blob(Pid, Container, Blob, Options) ->
    snapshot_blob(Pid, Container, Blob, Options, ?gen_server_call_default_timeout).
snapshot_blob(Pid, Container, Blob, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
    Service = ?blob_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                method => ?HTTP_METHOD_PUT,
                path => lists:concat([Container, "/", Blob]),
                params => [{comp, snapshot}] ++ Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_CREATED, created);
        Error ->
            Error
    end.

copy_blob(Pid, Container, Blob, Source) ->
    copy_blob(Pid, Container, Blob, Source, []).
copy_blob(Pid, Container, Blob, Source, Options) ->
    copy_blob(Pid, Container, Blob, Source, Options, ?gen_server_call_default_timeout).
copy_blob(Pid, Container, Blob, Source, Options, Timeout) when
    is_list(Options); is_integer(Timeout)
->
    Service = ?blob_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                method => ?HTTP_METHOD_PUT,
                path => lists:concat([Container, "/", Blob]),
                params => [{blob_copy_source, Source}] ++ Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, ervice, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_ACCEPTED, created);
        Error ->
            Error
    end.

delete_blob(Pid, Container, Blob) ->
    delete_blob(Pid, Container, Blob, []).
delete_blob(Pid, Container, Blob, Options) ->
    delete_blob(Pid, Container, Blob, Options, ?gen_server_call_default_timeout).
delete_blob(Pid, Container, Blob, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
    Service = ?blob_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                method => ?HTTP_METHOD_DELETE,
                path => lists:concat([Container, "/", Blob]),
                params => Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_ACCEPTED, deleted);
        Error ->
            Error
    end.

put_block(Pid, Container, Blob, BlockId, BlockContent) ->
    put_block(Pid, Container, Blob, BlockId, BlockContent, []).
put_block(Pid, Container, Blob, BlockId, BlockContent, Options) ->
    put_block(
        Pid, Container, Blob, BlockId, BlockContent, Options, ?gen_server_call_default_timeout
    ).
put_block(Pid, Container, Blob, BlockId, BlockContent, Options, Timeout) when
    is_list(Options); is_integer(Timeout)
->
    Service = ?blob_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            Params = [
                {comp, block},
                {blob_block_id, base64:encode_to_string(BlockId)}
            ],
            ReqOptions = #{
                method => ?HTTP_METHOD_PUT,
                path => lists:concat([Container, "/", Blob]),
                body => BlockContent,
                params => Params ++ Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_CREATED, created);
        Error ->
            Error
    end.

put_block_list(Pid, Container, Blob, BlockRefs) ->
    put_block_list(Pid, Container, Blob, BlockRefs, []).
put_block_list(Pid, Container, Blob, BlockRefs, Options) ->
    put_block_list(Pid, Container, Blob, BlockRefs, Options, ?gen_server_call_default_timeout).
put_block_list(Pid, Container, Blob, BlockRefs, Options, Timeout) when
    is_list(Options); is_integer(Timeout)
->
    Service = ?blob_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                method => ?HTTP_METHOD_PUT,
                path => lists:concat([Container, "/", Blob]),
                body => erlazure_blob:get_request_body(BlockRefs),
                params => [{comp, "blocklist"}] ++ Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_CREATED, created);
        Error ->
            Error
    end.

get_block_list(Pid, Container, Blob) ->
    get_block_list(Pid, Container, Blob, []).
get_block_list(Pid, Container, Blob, Options) ->
    get_block_list(Pid, Container, Blob, Options, ?gen_server_call_default_timeout).
get_block_list(Pid, Container, Blob, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
    Service = ?blob_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                path => lists:concat([Container, "/", Blob]),
                params => [{comp, "blocklist"}] ++ Options
            },
            case
                erlazure_http:request(
                    St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
                )
            of
                {?STATUS_CODE_OK, Body} ->
                    case erlazure_blob:parse_block_list(Body) of
                        {ok, BlockList} ->
                            {reply, BlockList, St};
                        Error ->
                            {reply, Error, St}
                    end;
                {_Code, Body} ->
                    {reply, {error, Body}, St}
            end;
        Error ->
            Error
    end.

acquire_blob_lease(Pid, Container, Blob, Duration) ->
    acquire_blob_lease(Pid, Container, Blob, Duration, []).
acquire_blob_lease(Pid, Container, Blob, Duration, Options) ->
    acquire_blob_lease(
        Pid, Container, Blob, "", Duration, Options, ?gen_server_call_default_timeout
    ).
acquire_blob_lease(Pid, Container, Blob, ProposedId, Duration, Options, Timeout) when
    is_list(Options); is_integer(Timeout)
->
    Service = ?blob_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            Params = [
                {lease_action, acquire},
                {proposed_lease_id, ProposedId},
                {lease_duration, Duration},
                {comp, lease}
            ],
            ReqOptions = #{
                method => ?HTTP_METHOD_PUT,
                path => lists:concat([Container, "/", Blob]),
                params => Params ++ Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_CREATED, acquired);
        Error ->
            Error
    end.

lease_container(Pid, Name, Mode) ->
    lease_container(Pid, Name, Mode, []).
lease_container(Pid, Name, Mode, Options) ->
    lease_container(Pid, Name, Mode, Options, ?gen_server_call_default_timeout).
lease_container(Pid, Name, Mode, Options, Timeout) when
    is_atom(Mode); is_list(Options); is_integer(Timeout)
->
    Service = ?blob_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            Params = [{comp, lease}, {res_type, container}, {lease_action, Mode}],
            ReqOptions = #{
                method => ?HTTP_METHOD_PUT,
                path => Name,
                params => Params ++ Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_ACCEPTED, deleted);
        Error ->
            Error
    end.

%%====================================================================
%% Table
%%====================================================================
list_tables(Pid) ->
    list_tables(Pid, [], ?gen_server_call_default_timeout).
list_tables(Pid, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
    Service = ?table_service,
    case gen_server:call(Pid, {get_state, Service, Options}, Timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                path => "Tables",
                params => Options
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(
                Code, Body, St, ?STATUS_CODE_OK, {ok, erlazure_table:parse_table_list(Body)}
            );
        Error ->
            Error
    end.

new_table(Pid, TableName) when is_list(TableName) ->
    new_table(Pid, list_to_binary(TableName));
new_table(Pid, TableName) when is_binary(TableName) ->
    Service = ?table_service,
    case gen_server:call(Pid, {get_state, Service, []}, ?gen_server_call_default_timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                path => "Tables",
                method => ?HTTP_METHOD_POST,
                content_type => ?json_content_type,
                body => njson:encode(#{<<"TableName">> => TableName})
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_CREATED, created);
        Error ->
            Error
    end.

delete_table(Pid, TableName) when is_binary(TableName) ->
    delete_table(Pid, binary_to_list(TableName));
delete_table(Pid, TableName) when is_list(TableName) ->
    Service = ?table_service,
    case gen_server:call(Pid, {get_state, Service, []}, ?gen_server_call_default_timeout) of
        {ok, #st{service_context = ServiceContext} = St} ->
            ReqOptions = #{
                path => "Tables('" ++ TableName ++ "')",
                method => ?HTTP_METHOD_DELETE
            },
            {Code, Body} = erlazure_http:request(
                St#st.conn_pid, Service, ServiceContext, St#st.param_specs, ReqOptions
            ),
            return_response(Code, Body, St, ?STATUS_CODE_NO_CONTENT, {ok, deleted});
        Error ->
            Error
    end.
%%====================================================================
%% gen_server callbacks
%%====================================================================
init({Account, Key, Service}) when
    Service == ?blob_service;
    Service == ?table_service;
    Service == ?queue_service
->
    case erlazure_http:open(Service, Account) of
        {ok, ConnPid} ->
            ConnRef = erlang:monitor(process, ConnPid),
            St = #st{
                    account = Account,
                    key = Key,
                    param_specs = get_req_param_specs(),
                    service = Service,
                    conn_pid = ConnPid,
                    conn_ref = ConnRef
                   },
            ServiceContext = new_service_context(Service, St),
            {ok, St#st{service_context = ServiceContext}};
        {error, _} = Error ->
            Error
    end.

handle_call({get_state, Service, _Options}, _From, St) when Service == St#st.service ->
    {reply, {ok, St}, St};
handle_call({get_state, _Service, _Options}, _From, St) ->
    {reply, {error, invalid_service}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info(
    {'DOWN', ConnRef, process, ConnPid, Reason}, #st{conn_pid = ConnPid, conn_ref = ConnRef} = St
) ->
    logger:debug("[erlazure] down with reason: ~p", [Reason]),
    erlang:demonitor(ConnRef),
    case erlazure_http:open(St#st.service, St#st.account) of
        {ok, NewConnPid} ->
            NewConnRef = erlang:monitor(process, NewConnPid),
            {noreply, St#st{
                conn_pid = NewConnPid,
                conn_ref = NewConnRef
            }};
        {error, Reason} ->
            {stop, Reason};
        Error ->
            {stop, Error}
    end.

terminate(_Reason, St) ->
    erlang:demonitor(St#st.conn_ref),
    gun:close(St#st.conn_pid),
    ok.

code_change(_OldVer, St, _Extra) ->
    {ok, St}.

%%--------------------------------------------------------------------
%% Private functions
%%--------------------------------------------------------------------
new_service_context(?queue_service, St = #st{}) ->
    #service_context{
        service = ?queue_service,
        api_version = ?queue_service_ver,
        account = St#st.account,
        key = St#st.key
    };
new_service_context(?blob_service, St = #st{}) ->
    #service_context{
        service = ?blob_service,
        api_version = ?blob_service_ver,
        account = St#st.account,
        key = St#st.key
    };
new_service_context(?table_service, St = #st{}) ->
    #service_context{
        service = ?table_service,
        api_version = ?table_service_ver,
        account = St#st.account,
        key = St#st.key
    }.

get_req_param_specs() ->
    ProcessFun = fun(Spec = #param_spec{}, Dictionary) ->
        orddict:store(Spec#param_spec.id, Spec, Dictionary)
    end,

    CommonParamSpecs = lists:foldl(ProcessFun, orddict:new(), get_req_common_param_specs()),
    BlobParamSpecs = lists:foldl(
        ProcessFun, CommonParamSpecs, erlazure_blob:get_request_param_specs()
    ),

    lists:foldl(ProcessFun, BlobParamSpecs, erlazure_queue:get_request_param_specs()).

get_req_common_param_specs() ->
    [
        #param_spec{id = comp, type = uri, name = "comp"},
        #param_spec{id = ?req_param_timeout, type = uri, name = "timeout"},
        #param_spec{id = ?req_param_maxresults, type = uri, name = "maxresults"},
        #param_spec{id = ?req_param_prefix, type = uri, name = "prefix"},
        #param_spec{id = ?req_param_include, type = uri, name = "include"},
        #param_spec{id = ?req_param_marker, type = uri, name = "marker"}
    ].

return_response(Code, Body, St, ExpectedResponseCode, SuccessAtom) ->
    case Code of
        ExpectedResponseCode ->
            {reply, {ok, SuccessAtom}, St};
        _ ->
            {reply, {error, Body}, St}
    end.
