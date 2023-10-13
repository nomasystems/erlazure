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

-module(erlazure_blob_tests).
-author("Dmitry Kataskin").

-compile(export_all).

-include("../include/erlazure.hrl").
-include_lib("eunit/include/eunit.hrl").

parse_container_list_test() ->
    Response = test_utils:read_file("list_containers.xml"),
    {ok, ParseResult} = erlazure_blob:parse_container_list(Response),

    ?assertMatch(
        {
            [
                #blob_container{
                    name = "cntr1",
                    url = "https://strg1.blob.core.windows.net/cntr1",
                    properties = [
                        {last_modified, "Thu, 01 May 2014 06:20:08 GMT"},
                        {etag, "\"0x8D1331C88F47AF1\""},
                        {lease_status, unlocked},
                        {lease_state, available}
                    ]
                },
                #blob_container{
                    name = "cntr2",
                    url = "https://strg1.blob.core.windows.net/cntr2",
                    properties = [
                        {last_modified, "Thu, 01 May 2014 06:20:16 GMT"},
                        {etag, "\"0x8D1331C8DB0C279\""},
                        {lease_status, unlocked},
                        {lease_state, leased},
                        {lease_duration, infinite}
                    ]
                }
            ],
            [
                {prefix, "tstprefix"},
                {marker, "mrkr12344321"},
                {max_results, 255},
                {next_marker, ""}
            ]
        },
        ParseResult
    ).

parse_blob_test() ->
    Response = test_utils:read_file("list_blobs.xml"),
    {ParseResult, _} = xmerl_scan:string(Response),
    BlobsNode = lists:keyfind('Blobs', 2, ParseResult#xmlElement.content),
    Blob1 = lists:keyfind('Blob', 2, BlobsNode#xmlElement.content),
    ParsedBlob1 = erlazure_blob:parse_blob_response(Blob1),
    ?assertMatch(
        #cloud_blob{
            name = "blb1.txt",
            snapshot = "Mon, 05 May 2014 16:08:11 GMT",
            url = "https://strg1.blob.core.windows.net/cntr1/blb1.txt",
            properties = [
                {last_modified, "Mon, 05 May 2014 16:08:11 GMT"},
                {etag, "0x8D13693589FF293"},
                {content_length, 4},
                {content_type, "text/plain"},
                {content_encoding, ""},
                {content_language, ""},
                {content_md5, "CY9rzUYh03PK3k6DJie09g=="},
                {cache_control, ""},
                {sequence_number, "13"},
                {blob_type, block_blob},
                {lease_status, locked},
                {lease_state, leased},
                {lease_duration, infinite},
                {copy_id, "3d6a6a35-bc97-46e5-bb11-cb1b73a402b6"},
                {copy_status, pending},
                {copy_source, "https://strg1.blob.core.windows.net/cntr1/blb1.txt"},
                {copy_progress, "104/5456"},
                {copy_completion_time, "Mon, 05 May 2014 16:08:11 GMT"},
                {copy_status_description, "copy status"}
            ]
        },
        ParsedBlob1
    ).

parse_blob_list_test() ->
    Response = test_utils:read_file("list_blobs.xml"),
    {ok, {[Blob1, Blob2], _}} = erlazure_blob:parse_blob_list(Response),

    ?assertMatch(#cloud_blob{name = "blb1.txt"}, Blob1),
    ?assertMatch(#cloud_blob{name = "blb2.txt"}, Blob2).

parse_block_list_test() ->
    Response = test_utils:read_file("list_blocks.xml"),
    {ok, {CommittedBlocks, UncommittedBlocks}} = erlazure_blob:parse_block_list(Response),

    ?assertMatch(
        [
            #blob_block{id = "BlockId001", type = committed, size = 4194304},
            #blob_block{id = "BlockId002", type = committed, size = 41912}
        ],
        CommittedBlocks
    ),

    ?assertMatch(
        [
            #blob_block{id = "BlockId003", type = uncommitted, size = 4194304},
            #blob_block{id = "BlockId004", type = uncommitted, size = 1024000}
        ],
        UncommittedBlocks
    ).
