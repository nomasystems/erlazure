%% Copyright (c) 2013 - 2015, Dmitry Kataskin
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
%% * Neither the name of  nor the names of its contributors may be used to
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

-author("Dmitry Kataskin").

-define(HTTP_METHOD_DELETE, <<"DELETE">>).
-define(HTTP_METHOD_GET, <<"GET">>).
-define(HTTP_METHOD_HEAD, <<"HEAD">>).
-define(HTTP_METHOD_OPTIONS, <<"OPTIONS">>).
-define(HTTP_METHOD_POST, <<"POST">>).
-define(HTTP_METHOD_PUT, <<"PUT">>).
-define(HTTP_METHOD_TRACE, <<"TRACE">>).

-define(STATUS_CODE_CONTINUE, 100).
-define(STATUS_CODE_SWITCHING_PROTOCOLS, 101).
-define(STATUS_CODE_PROCESSING, 102).
-define(STATUS_CODE_OK, 200).
-define(STATUS_CODE_CREATED, 201).
-define(STATUS_CODE_ACCEPTED, 202).
-define(STATUS_CODE_NON_AUTHORITATIVE, 203).
-define(STATUS_CODE_NO_CONTENT, 204).
-define(STATUS_CODE_RESET_CONTENT, 205).
-define(STATUS_CODE_PARTIAL_CONTENT, 206).
-define(STATUS_CODE_MULTI_STATUS, 207).
-define(STATUS_CODE_IM_USED, 226).
-define(STATUS_CODE_MULTIPLE_CHOICES, 300).
-define(STATUS_CODE_MOVED_PERMANENTLY, 301).
-define(STATUS_CODE_FOUND, 302).
-define(STATUS_CODE_SEE_OTHER, 303).
-define(STATUS_CODE_NOT_MODIFIED, 304).
-define(STATUS_CODE_USE_PROXY, 305).
-define(STATUS_CODE_SWITCH_PROXY, 306).
-define(STATUS_CODE_TEMPORARY_REDIRECT, 307).
-define(STATUS_CODE_BAD_REQUEST, 400).
-define(STATUS_CODE_UNAUTHORIZED, 401).
-define(STATUS_CODE_PAYMENT_REQUIRED, 402).
-define(STATUS_CODE_FORBIDDEN, 403).
-define(STATUS_CODE_NOT_FOUND, 404).
-define(STATUS_CODE_METHOD_NOT_ALLOWED, 405).
-define(STATUS_CODE_NOT_ACCEPTABLE, 406).
-define(STATUS_CODE_PROXY_AUTHENTICATION_REQUIRED, 407).
-define(STATUS_CODE_REQUEST_TIMEOUT, 408).
-define(STATUS_CODE_CONFLICT, 409).
-define(STATUS_CODE_GONE, 410).
-define(STATUS_CODE_LENGTH_REQUIRED, 411).
-define(STATUS_CODE_PRECONDITION_FAILED, 412).
-define(STATUS_CODE_REQUEST_ENTITY_TOO_LARGE, 413).
-define(STATUS_CODE_REQUEST_URI_TOO_LONG, 414).
-define(STATUS_CODE_UNSUPPORTED_MEDIA_TYPE, 415).
-define(STATUS_CODE_REQUEST_RANGE_NOT_SATISFIABLE, 416).
-define(STATUS_CODE_EXPECTATION_FAILED, 417).
-define(STATUS_CODE_IM_A_TEAPOT, 418).
-define(STATUS_CODE_UNPROCESSABLE_ENTITY, 422).
-define(STATUS_CODE_LOCKED, 423).
-define(STATUS_CODE_FAILED_DEPENDENCY, 424).
-define(STATUS_CODE_UNORDERED_COLLECTION, 425).
-define(STATUS_CODE_UPGRADE_REQUIRED, 426).
-define(STATUS_CODE_PRECONDITION_REQUIRED, 428).
-define(STATUS_CODE_TOO_MANY_REQUESTS, 429).
-define(STATUS_CODE_REQUEST_HEADER_FIELDS_TOO_LARGE, 431).
-define(STATUS_CODE_INTERNAL_SERVER_ERROR, 500).
-define(STATUS_CODE_NOT_IMPLEMENTED, 501).
-define(STATUS_CODE_BAD_GATEWAY, 502).
-define(STATUS_CODE_SERVICE_UNAVAILABLE, 503).
-define(STATUS_CODE_GATEWAY_TIMEOUT, 504).
-define(STATUS_CODE_HTTP_VERSION_NOT_SUPPORTED, 505).
-define(STATUS_CODE_VARIANT_ALSO_NEGOTIATES, 506).
-define(STATUS_CODE_INSUFFICIENT_STORAGE, 507).
-define(STATUS_CODE_NOT_EXTENDED, 510).
-define(STATUS_CODE_NETWORK_AUTHENTICATION_REQUIRED, 511).

-define(blob_service, blob).
-define(table_service, table).
-define(queue_service, queue).

-define(queue_service_ver, "2014-02-14").
-define(blob_service_ver, "2014-02-14").
-define(table_service_ver, "2014-02-14").

-define(content_type, "application/xml").

%% Request common parameters
-define(req_param_prefix, prefix).
-define(req_param_marker, marker).
-define(req_param_maxresults, max_results).
-define(req_param_include, include).
-define(req_param_timeout, timeout).
-define(req_param_clientrequestid, client_request_id).

-include_lib("xmerl/include/xmerl.hrl").

-ifndef(PRINT).
-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-endif.

-ifdef(TEST).
-define(account_name, "strg1").
-define(account_key, "").
-endif.

%% Types
-type xmlElement() :: #xmlElement{}.
-export_type([xmlElement/0]).

%?HTTP_METHOD_HEAD | ?HTTP_METHOD_GET | ?HTTP_METHOD_DELETE | ?HTTP_METHOD_OPTIONS | ?HTTP_METHOD_POST | ?HTTP_METHOD_PUT | ?HTTP_METHOD_TRACE.
-type method() :: binary().
-export_type([method/0]).

-type azure_service() :: ?queue_service | ?blob_service | ?table_service.
-export_type([azure_service/0]).

-type metadata() :: proplists:proplist().
-export_type([metadata/0]).

-type req_param_prefix() :: {?req_param_prefix, string()}.
-type req_param_marker() :: {?req_param_marker, string()}.
-type req_param_maxresults() :: {?req_param_maxresults, non_neg_integer()}.
-type req_param_include() :: {?req_param_include, metadata}.
-type req_param_timeout() :: {?req_param_timeout, non_neg_integer()}.
-type req_param_clientrequestid() :: {?req_param_clientrequestid, string()}.

-type request_common_opt() ::
    req_param_prefix()
    | req_param_marker()
    | req_param_maxresults()
    | req_param_include()
    | req_param_timeout()
    | req_param_clientrequestid().

-type common_opts() :: list(request_common_opt()).
-export_type([
    req_param_prefix/0,
    req_param_marker/0,
    req_param_maxresults/0,
    req_param_include/0,
    req_param_timeout/0,
    req_param_clientrequestid/0
]).
-export_type([request_common_opt/0]).
-export_type([common_opts/0]).

-type created_response() :: {ok, created}.
-type already_created_response() :: {error, already_created}.
-type updated_response() :: {ok, updated}.
-type deleted_response() :: {ok, deleted}.
-type lease_acquired_response() :: {ok, acquired}.
-type bad_response() :: {error, bad_response}.
-export_type([
    bad_response/0,
    created_response/0,
    updated_response/0,
    deleted_response/0,
    lease_acquired_response/0
]).
-export_type([already_created_response/0]).

-type enum_parse_result(T) :: bad_response() | {ok, {list(T), metadata()}}.
-export_type([enum_parse_result/1]).

-type lease_state() :: available | leased | breaking | broken | expired.
-type lease_status() :: locked | unlocked.
-type lease_duration() :: infinite | fixed.
-export_type([lease_state/0, lease_status/0, lease_duration/0]).

-type blob_block_type() :: committed | uncommitted.
-export_type([blob_block_type/0]).

-type enum_common_token() ::
    {prefix, string()}
    | {marker, string()}
    | {max_results, non_neg_integer()}
    | {delimiter, string()}
    | {next_marker, string()}.
-type enum_common_tokens() :: list(enum_common_token()).
-export_type([enum_common_token/0, enum_common_tokens/0]).

-type request_param() :: {atom(), any()}.
-type request_param_type() :: uri | header.
-type request_header() :: {string(), string()}.
-export_type([request_param/0, request_param_type/0, request_header/0]).

-record(service_context, {
    service = undefined :: undefined | azure_service(),
    api_version = "" :: string(),
    account = "" :: string(),
    key = "" :: string()
}).
-type service_context() :: #service_context{}.
-export_type([service_context/0]).

-record(req_context, {
    method = ?HTTP_METHOD_GET :: method(),
    path = "" :: string(),
    parameters = [] :: list(request_param()),
    content_type = ?content_type :: string(),
    content_length = 0 :: non_neg_integer(),
    body = "" :: string() | binary(),
    headers = [] :: list(request_header())
}).
-type req_context() :: #req_context{}.
-export_type([req_context/0]).

-record(param_spec, {
    id = undefined :: atom(),
    type = undefined :: undefined | request_param_type(),
    name = "" :: string(),
    parse_fun = fun(Value) ->
        lists:flatten(io_lib:format("~s", [Value]))
    end :: fun((any()) -> string())
}).
-type param_spec() :: #param_spec{}.
-export_type([param_spec/0]).

-record(property_spec, {
    name = undefined :: atom(),
    key = undefined :: atom(),
    parse_fun = fun(Elem = #xmlElement{}) ->
        erlazure_xml:parse_str(Elem)
    end :: fun((xmlElement()) -> string() | integer() | atom())
}).
-type property_spec() :: #property_spec{}.
-export_type([property_spec/0]).

%% @todo Improve specs.
-record(enum_parser_spec, {
    rootKey = undefined :: atom(),
    elementKey = undefined :: atom(),
    elementParser :: any(),
    customParsers = [] :: [any()]
}).
-type enum_parser_spec() :: #enum_parser_spec{}.
-export_type([enum_parser_spec/0]).

% Queue
-record(queue, {
    name = "" :: string(),
    url = "" :: string(),
    metadata = [] :: metadata()
}).
-type queue() :: #queue{}.
-export_type([queue/0]).

-record(access_policy, {
    start = "" :: string(),
    expiry = "" :: string(),
    permission = "" :: string()
}).
-type access_policy() :: #access_policy{}.
-export_type([access_policy/0]).

-record(signed_id, {
    id = "" :: string(),
    access_policy = #access_policy{} :: access_policy()
}).
-type signed_id() :: #signed_id{}.
-export_type([signed_id/0]).

-record(queue_message, {
    id = "" :: string(),
    insertion_time = "" :: string(),
    exp_time = "" :: string(),
    pop_receipt = "" :: string(),
    next_visible = "" :: string(),
    dequeue_count = 0 :: non_neg_integer(),
    text = "" :: string()
}).
-type queue_message() :: #queue_message{}.
-export_type([queue_message/0]).

% Blob
-record(blob_container, {
    name = "" :: string(),
    url = "" :: string(),
    properties = [] :: proplists:proplist(),
    metadata = [] :: metadata()
}).
-type blob_container() :: #blob_container{}.
-export_type([blob_container/0]).

-record(blob_lease, {
    id = "" :: string(),
    status = undefined :: undefined | lease_status(),
    state = undefined :: undefined | lease_state(),
    duration = undefined :: undefined | lease_duration()
}).
-type blob_lease() :: #blob_lease{}.
-export_type([blob_lease/0]).

-record(blob_copy_state, {
    id = "" :: string(),
    status = undefined,
    source = "" :: string(),
    progress = "" :: string(),
    completion_time = "" :: string(),
    status_description = "" :: string()
}).

-record(cloud_blob, {
    name = "" :: string(),
    snapshot = "" :: string(),
    url = "" :: string(),
    properties = [] :: proplists:proplist(),
    metadata = [] :: metadata()
}).
-type cloud_blob() :: #cloud_blob{}.
-export_type([cloud_blob/0]).

-record(blob_block, {
    id = "" :: string(),
    type = undefined :: undefined | blob_block_type(),
    size = 0 :: non_neg_integer()
}).
-type blob_block() :: #blob_block{}.
-export_type([blob_block/0]).

%
% Table service types
%
-record(cloud_table, {
    name = <<>> :: binary(),
    metadata = [] :: metadata()
}).
-type cloud_table() :: #cloud_table{}.
-export_type([cloud_table/0]).
