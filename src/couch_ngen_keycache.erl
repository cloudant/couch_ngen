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

-module(couch_ngen_keycache).

-behaviour(gen_server).

-export([start_link/0, get_key/1]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).

-record(st, {tab}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


get_key(Name) ->
    gen_server:call(?MODULE, {get_key, Name}).


init(_) ->
    process_flag(sensitive, true),
    Tab = ets:new(?MODULE, [private]),
    {ok, #st{tab = Tab}}.


handle_call({get_key, Name}, _From, St) ->
    case ets:lookup(St#st.tab, Name) of
        [] ->
            {reply, {error, not_found}, St};
        [{Name, Key}] ->
            {reply, {ok, Key}, St}
    end;

handle_call(_Msg, _From, St) ->
    {noreply, St}.


handle_cast(_Msg, St) ->
    {noreply, St}.


handle_info(_Msg, St) ->
    {noreply, St}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, nil, _Extra) ->
    {ok, nil}.
