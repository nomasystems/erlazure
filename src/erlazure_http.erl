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

-module(erlazure_http).
-author("Dmitry Kataskin").

-include("erlazure.hrl").

%% API
-export([
    open/2,
    request/5
]).

-export([
    get_signature_string/6,
    get_shared_key/7,
    sign_string/2
]).


-define(GUN_OPTS, #{
    http_opts => #{version => 'HTTP/1.1'},
    transport => tls,
    tls_opts => [{verify, verify_none}]
}).

open(Service, Account) ->
    Host = host(Service, Account),
    case gun:open(Host, 443, ?GUN_OPTS) of
        {ok, ConnPid} ->
            {ok, _Protocol} = gun:await_up(ConnPid),
            {ok, ConnPid};
        Error ->
            Error
    end.

host(Service, Account) ->
    lists:concat([Account, ".", erlang:atom_to_list(Service), ".core.windows.net"]).

request(ConnPid, Service, ServiceContext = #service_context{}, ParamSpecs, Options) ->
    ReqContext = req_context(Service, ParamSpecs, Options),
    DateHeader =
        if
            (ServiceContext#service_context.service =:= ?table_service) ->
                {"date", httpd_util:rfc1123_date()};
            true ->
                {"x-ms-date", httpd_util:rfc1123_date()}
        end,

    Headers = [
        DateHeader,
        {"x-ms-version", ServiceContext#service_context.api_version},
        {"host",
            host(
                ServiceContext#service_context.service,
                ServiceContext#service_context.account
            )}
    ],

    Headers1 =
        if
            (ReqContext#req_context.method =:= ?HTTP_METHOD_PUT orelse
                ReqContext#req_context.method =:= ?HTTP_METHOD_POST) ->
                ContentHeaders = [
                    {"content-type", ReqContext#req_context.content_type},
                    {"content-length", integer_to_list(ReqContext#req_context.content_length)}
                ],
                lists:append([Headers, ContentHeaders, ReqContext#req_context.headers]);
            true ->
                lists:append([Headers, ReqContext#req_context.headers])
        end,

    AuthHeader =
        {"authorization",
            get_shared_key(
                ServiceContext#service_context.service,
                ServiceContext#service_context.account,
                ServiceContext#service_context.key,
                ReqContext#req_context.method,
                ReqContext#req_context.path,
                ReqContext#req_context.parameters,
                Headers1
            )},

    {Resource, FinalHeaders, Body} = create_request(ReqContext, [AuthHeader | Headers1]),
    StreamRef = gun:request(
        ConnPid,
        ReqContext#req_context.method,
        Resource,
        FinalHeaders,
        Body
    ),
    Response = response(ConnPid, StreamRef),
    case Response of
        {ok, Code, _Headers, ResponseBody} when
            % 200 - 206
            Code >= ?STATUS_CODE_OK, Code =< ?STATUS_CODE_PARTIAL_CONTENT
        ->
            {Code, ResponseBody};
        {ok, _Code, _Headers, ResponseBody} ->
            try get_error_code(ResponseBody) of
                ErrorCodeAtom -> {error, ErrorCodeAtom}
            catch
                _ -> {error, ResponseBody}
            end
    end.

response(ConnPid, StreamRef) ->
    case gun:await(ConnPid, StreamRef) of
        {response, fin, Status, Headers} ->
            {ok, Status, Headers, undefined};
        {response, nofin, Status, Headers} ->
            {ok, Body} = gun:await_body(ConnPid, StreamRef),
            {ok, Status, Headers, Body}
    end.

req_context(Service, ParamSpecs, Options) ->
    Method = maps:get(method, Options, ?HTTP_METHOD_GET),
    Path = maps:get(path, Options, "/"),
    Body = maps:get(body, Options, undefined),
    Headers = maps:get(headers, Options, []),
    Params = maps:get(params, Options, []),
    ContentType = maps:get(content_type, Options, ?content_type),
    AddHeaders =
        if
            (Service =:= ?table_service) ->
                case lists:keyfind("Accept", 1, Headers) of
                    false -> [{"Accept", "application/json;odata=fullmetadata"}];
                    _ -> []
                end;
            true ->
                []
        end,

    ReqParams = get_req_uri_params(Params, ParamSpecs),
    ReqHeaders = lists:append([Headers, AddHeaders, get_req_headers(Params, ParamSpecs)]),

    #req_context{
        path = Path,
        method = Method,
        body = Body,
        content_type = ContentType,
        content_length = content_length(Body),
        parameters = ReqParams,
        headers = ReqHeaders
    }.

get_req_headers(Params, ParamSpecs) ->
    get_req_params(Params, ParamSpecs, header).

get_req_uri_params(Params, ParamSpecs) ->
    get_req_params(Params, ParamSpecs, uri).

get_req_params(Params, ParamSpecs, Type) ->
    ParamDefs = orddict:filter(fun(_, Value) -> Value#param_spec.type =:= Type end, ParamSpecs),
    FoldFun = fun
        ({_ParamName, ""}, Acc) ->
            Acc;
        ({ParamName, ParamValue}, Acc) ->
            case orddict:find(ParamName, ParamDefs) of
                {ok, Value} ->
                    [{Value#param_spec.name, (Value#param_spec.parse_fun)(ParamValue)} | Acc];
                error ->
                    Acc
            end
    end,
    lists:foldl(FoldFun, [], Params).

get_error_code(Body) ->
    {ParseResult, _} = xmerl_scan:string(binary_to_list(Body)),
    ErrorContent = ParseResult#xmlElement.content,
    ErrorContentHead = hd(ErrorContent),
    CodeContent = ErrorContentHead#xmlElement.content,
    CodeContentHead = hd(CodeContent),
    ErrorCodeText = CodeContentHead#xmlText.value,
    list_to_atom(ErrorCodeText).

get_shared_key(Service, Account, Key, HttpMethod, Path, Parameters, Headers) ->
    SignatureString = get_signature_string(Service, HttpMethod, Headers, Account, Path, Parameters),
    "SharedKey " ++ Account ++ ":" ++ base64:encode_to_string(sign_string(Key, SignatureString)).

get_signature_string(Service, HttpMethod, Headers, Account, Path, Parameters) ->
    SigStr1 = verb_to_str(HttpMethod) ++ "\n" ++ get_headers_string(Service, Headers),

    SigStr2 =
        if
            (Service =:= ?queue_service) orelse (Service =:= ?blob_service) ->
                SigStr1 ++ canonicalize_headers(Headers);
            true ->
                SigStr1
        end,
    SigStr2 ++ canonicalize_resource(Account, Path, Parameters).

get_headers_string(Service, Headers) ->
    FoldFun = fun(HeaderName, Acc) ->
        case lists:keyfind(HeaderName, 1, Headers) of
            {HeaderName, Value} -> lists:concat([Acc, Value, "\n"]);
            false -> lists:concat([Acc, "\n"])
        end
    end,
    lists:foldl(FoldFun, "", get_header_names(Service)).

-spec sign_string(base64:base64_string() | base64:base64_binary(), string()) -> binary().
sign_string(Key, StringToSign) ->
    hmac(base64:decode(Key), StringToSign).

hmac(Key, Str) ->
    crypto:mac(hmac, sha256, Key, Str).

-spec canonicalize_headers([string()]) -> string().
canonicalize_headers(Headers) ->
    MSHeaderNames = [
        HeaderName
     || {HeaderName, _} <- Headers, string:str(HeaderName, "x-ms-") =:= 1
    ],
    SortedHeaderNames = lists:sort(MSHeaderNames),
    FoldFun = fun(HeaderName, Acc) ->
        {_, Value} = lists:keyfind(HeaderName, 1, Headers),
        lists:concat([Acc, HeaderName, ":", Value, "\n"])
    end,
    lists:foldl(FoldFun, "", SortedHeaderNames).

canonicalize_resource(Account, Path, []) ->
    lists:concat(["/", Account, "/", Path]);
canonicalize_resource(Account, Path, Parameters) ->
    SortFun = fun({ParamNameA, ParamValA}, {ParamNameB, ParamValB}) ->
        ParamNameA ++ ParamValA =< ParamNameB ++ ParamValB
    end,
    SortedParameters = lists:sort(SortFun, Parameters),
    [H | T] = SortedParameters,
    "/" ++ Account ++ "/" ++ Path ++ combine_canonical_param(H, "", "", T).

combine_canonical_param({Param, Value}, Param, Acc, []) ->
    Acc ++ "," ++ Value;
combine_canonical_param({Param, Value}, _PreviousParam, Acc, []) ->
    Acc ++ "\n" ++ string:to_lower(Param) ++ ":" ++ Value;
combine_canonical_param({Param, Value}, Param, Acc, ParamList) ->
    [H | T] = ParamList,
    combine_canonical_param(H, Param, Acc ++ "," ++ Value, T);
combine_canonical_param({Param, Value}, _PreviousParam, Acc, ParamList) ->
    [H | T] = ParamList,
    combine_canonical_param(
        H, Param, Acc ++ "\n" ++ string:to_lower(Param) ++ ":" ++ Value, T
    ).

get_header_names(?blob_service) ->
    get_header_names(?queue_service);
get_header_names(?queue_service) ->
    [
        "Content-Encoding",
        "Content-Language",
        "Content-Length",
        "Constent-MD5",
        "Content-Type",
        "Date",
        "If-Modified-Since",
        "If-Match",
        "If-None-Match",
        "If-Unmodified-Since",
        "Range"
    ];
get_header_names(?table_service) ->
    [
        "Content-MD5",
        "Content-Type",
        "Date"
    ].

verb_to_str(Method) ->
    binary_to_list(Method).

create_request(ReqContext = #req_context{method = ?HTTP_METHOD_GET}, Headers) ->
    {construct_url(ReqContext), Headers, undefined};
create_request(ReqContext = #req_context{method = ?HTTP_METHOD_DELETE}, Headers) ->
    {construct_url(ReqContext), Headers, undefined};
create_request(ReqContext = #req_context{}, Headers) ->
    {
        construct_url(ReqContext),
        [{"Content-Type", ReqContext#req_context.content_type} | Headers],
        ReqContext#req_context.body
    }.

construct_url(ReqContext = #req_context{}) ->
    FoldFun = fun({ParamName, ParamValue}, Acc) ->
        if
            Acc =:= "" ->
                lists:concat(["?", ParamName, "=", ParamValue]);
            true ->
                lists:concat([Acc, "&", ParamName, "=", ParamValue])
        end
    end,
    "/"++ReqContext#req_context.path ++
        lists:foldl(FoldFun, "", ReqContext#req_context.parameters).

content_length(undefined) ->
    0;
content_length(Content) when is_list(Content) ->
    erlang:iolist_size(Content);
content_length(Content) when is_binary(Content) ->
    byte_size(Content).
