% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_ldb_engine_stream).

-export([
    foldl/3,
    seek/2,
    write/2,
    finalize/1,
    to_disk_term/1
]).


foldl({_Fd, []}, _Fun, Acc) ->
    Acc;

foldl({Fd, [Bin | Rest]}, Fun, Acc) when is_binary(Bin) ->
    % We're processing the first bit of data
    % after we did a seek for a range fold.
    foldl({Fd, Rest}, Fun, Fun(Bin, Acc));

foldl({Fd, [Ptr | Rest]}, Fun, Acc) ->
    {ok, Bin} = couch_ngen_file:read_bin(Fd, Ptr),
    foldl({Fd, Rest}, Fun, Fun(Bin, Acc)).


seek({Fd, [Ptr | Rest]}, Offset) ->
    Length = couch_ngen_file:length(Ptr),
    case Length =< Offset of
        true ->
            seek({Fd, Rest}, Offset - Length);
        false ->
            seek_trunc({Fd, [Ptr | Rest]}, Offset)
    end.


seek_trunc({Fd, [Ptr | Rest]}, Offset) ->
    {ok, Bin} = couch_ngen_file:read_bin(Fd, Ptr),
    true = iolist_size(Bin) < Offset,
    <<_:Offset/binary, Tail/binary>> = Bin,
    {ok, {Fd, [Tail | Rest]}}.


write({Fd, Written}, Data) ->
    {ok, Ptr} = couch_ngen_file:append_bin(Fd, Data),
    {ok, {Fd, [Ptr | Written]}}.


finalize({Fd, Written}) ->
    {ok, {Fd, lists:reverse(Written)}}.


to_disk_term({_Fd, Written}) ->
    {ok, Written}.

