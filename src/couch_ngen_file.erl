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

-module(couch_ngen_file).
-behaviour(gen_server).
-vsn(1).


-define(INITIAL_WAIT, 60000).
-define(MONITOR_CHECK, 10000).


-record(ngenfd, {
    pid,
    fd,
    t2bopts
}).


-record(st, {
    path,
    fd,
    is_sys = false,
    eof = 0,
    t2bopts = [],
    db_pid
}).


-export([
    init_delete_dir/1,
    delete/2,
    delete/3,
    nuke_dir/2,
    nuke_dir/3
]).

-export([
    open/1,
    open/2,
    monitor/1,
    monitored_by/1,
    close/1,

    set_db_pid/2,

    length/1,

    rename/2,
    path/1,
    bytes/1,
    sync/1,
    truncate/2,

    append_term/2,
    append_bin/2,

    read_term/2,
    read_bin/2
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


init_delete_dir(RootDir) ->
    Dir = filename:join(RootDir,".delete"),
    filelib:ensure_dir(filename:join(Dir, "foo")),
    spawn(fun() ->
        {ok, RawFiles} = nifile:lsdir(Dir),
        Files = RawFiles -- [<<".">>, <<"..">>],
        lists:foreach(fun(FileName) ->
            FilePath = filename:join(Dir, FileName),
            ok = nifile:remove(FilePath)
        end, Files)
    end),
    ok.


delete(RootDir, FilePath) ->
    delete(RootDir, FilePath, true).


delete(RootDir, FilePath, DelOpts) ->
    UUID = binary_to_list(couch_uuids:random()),
    DelFile = filename:join([RootDir,".delete", UUID]),
    case nifile:rename(FilePath, DelFile) of
        ok ->
            case proplists:get_value(async, DelOpts) of
                true -> spawn(nifile, remove, [DelFile]);
                _ -> nifile:remove(DelFile)
            end;
        Error ->
            Error
    end.


nuke_dir(RootDelDir, Dir) ->
    nuke_dir(RootDelDir, Dir, true).


nuke_dir(RootDelDir, Dir, Async) ->
    DelFun = fun(File) ->
        Path = filename:join(Dir, File),
        case filelib:is_dir(Path) of
            true ->
                ok = nuke_dir(RootDelDir, Path, Async),
                nifile:rmdir(Path);
            false ->
                delete(RootDelDir, Path, Async)
        end
    end,
    case nifile:lsdir(Dir) of
        {ok, RawFiles} ->
            Files = RawFiles -- [<<".">>, <<"..">>],
            lists:foreach(DelFun, Files);
        {error, enoent} ->
            ok
    end,
    nifile:rmdir(Dir).


open(FilePath) ->
    open(FilePath, []).


open(FilePath, Options) ->
    InitArg = [{FilePath, self(), Options}],
    proc_lib:start_link(?MODULE, init, InitArg).


monitor(#ngenfd{} = Fd) ->
    erlang:monitor(process, Fd#ngenfd.pid).


monitored_by(#ngenfd{} = Fd) ->
    case erlang:process_info(Fd#ngenfd.pid, monitored_by) of
        {monitored_by, Pids} ->
            Pids;
        _ ->
            []
    end.


close(#ngenfd{} = Fd) ->
    gen_server:call(Fd#ngenfd.pid, close, infinity).


set_db_pid(#ngenfd{} = Fd, Pid) ->
    gen_server:call(Fd#ngenfd.pid, {set_db_pid, Pid}).


length({Pos, Len}) when is_integer(Pos), is_integer(Len) ->
    Len.


sync(#ngenfd{} = Fd) ->
    gen_server:call(Fd#ngenfd.pid, sync, infinity).


rename(#ngenfd{} = Fd, TgtPath) ->
    gen_server:call(Fd#ngenfd.pid, {rename, TgtPath}, infinity);

rename(SrcPath, TgtPath) ->
    nifile:rename(SrcPath, TgtPath).


path(#ngenfd{} = Fd) ->
    gen_server:call(Fd#ngenfd.pid, path, infinity).


bytes(#ngenfd{} = Fd) ->
    gen_server:call(Fd#ngenfd.pid, bytes, infinity).


append_term(#ngenfd{} = Fd, Term) ->
    Bin = term_to_binary(Term, Fd#ngenfd.t2bopts),
    append_bin(Fd, Bin).


append_bin(Fd, Bin) when is_list(Bin) ->
    append_bin(Fd, iolist_to_binary(Bin));

append_bin(#ngenfd{} = Fd, Bin) when is_binary(Bin) ->
    gen_server:call(Fd#ngenfd.pid, {append, Bin}).


read_term(Fd, Ptr) ->
    {ok, Bin} = read_bin(Fd, Ptr),
    {ok, binary_to_term(Bin)}.


read_bin(#ngenfd{} = Fd, {Pos, Len}) ->
    nifile:pread(Fd#ngenfd.fd, Len, Pos).


truncate(Fd, Pos) ->
    gen_server:call(Fd#ngenfd.pid, {truncate, Pos}, infinity).


init({FilePath, Parent, Options}) ->
    process_flag(trap_exit, true),

    Create = lists:member(create, Options),
    Overwrite = lists:member(overwrite, Options),
    ExtraOpenOpts = case {Create, Overwrite} of
        {true, true} -> [create];
        {true, false} -> [create, exclusive];
        {false, _} -> []
    end,
    OpenOpts = [read_write, append] ++ ExtraOpenOpts,

    case nifile:open(FilePath, OpenOpts) of
        {ok, Fd} ->
            St = init_st(FilePath, Fd, Parent, Options),
            ExtFd = #ngenfd{
                pid = self(),
                fd = Fd,
                t2bopts = St#st.t2bopts
            },
            proc_lib:init_ack({ok, ExtFd}),
            gen_server:enter_loop(?MODULE, [], St, ?INITIAL_WAIT);
        Error ->
            proc_lib:init_ack(Error)
    end.


terminate(_Reason, St) ->
    if St#st.fd == undefined -> ok; true ->
        nifile:close(St#st.fd)
    end.


handle_call(close, _From, #st{fd=Fd}=St) ->
    {stop, normal, nifile:close(Fd), St#st{fd = undefined}};

handle_call({set_db_pid, Pid}, _From, St) ->
    OldPid = St#st.db_pid,
    if not is_pid(OldPid) -> ok; true ->
        unlink(OldPid),
        receive
            {'EXIT', OldPid, _} -> ok
            after 0 -> ok
        end
    end,
    link(Pid),
    {reply, ok, St#st{db_pid=Pid}, ?MONITOR_CHECK};

handle_call({rename, TgtPath}, _From, St) ->
    case nifile:rename(St#st.path, TgtPath) of
        ok ->
            {reply, ok, St#st{path = TgtPath}};
        Else ->
            {reply, Else, St}
    end;

handle_call(path, _From, St) ->
    {reply, St#st.path, St};

handle_call(bytes, _From, St) ->
    {ok, Bytes} = nifile:seek(St#st.fd, 0, seek_end),
    case Bytes /= St#st.eof of
        true -> {stop, invalid_eof, invalid_eof, St};
        false -> {reply, {ok, Bytes}, St, ?MONITOR_CHECK}
    end;

handle_call(sync, _From, St) ->
    {reply, nifile:sync(St#st.fd), St, ?MONITOR_CHECK};

handle_call({truncate, Pos}, _From, St) ->
    {ok, Pos} = nifile:seek(St#st.fd, Pos, seek_set),
    case nifile:truncate(St#st.fd, Pos) of
        ok -> {reply, ok, St#st{eof = Pos}, ?MONITOR_CHECK};
        Error -> {reply, Error, St, ?MONITOR_CHECK}
    end;

handle_call({append, Bin}, _From, St) ->
    #st{
        eof = Eof
    } = St,

    BinSize = size(Bin),
    case nifile:write(St#st.fd, Bin) of
        {ok, BinSize} ->
            NewSt = St#st{
                eof = Eof + BinSize
            },
            {reply, {ok, {Eof, BinSize}}, NewSt, ?MONITOR_CHECK};
        {ok, WrongSize} ->
            NewSt = St#st{
                eof = Eof + WrongSize
            },
            {reply, {error, incomplete_write}, NewSt, ?MONITOR_CHECK};
        Error ->
            {ok, Eof} = nifile:seek(St#st.fd, 0, seek_end),
            {reply, Error, St#st{eof = Eof}, ?MONITOR_CHECK}
    end.


handle_cast(close, Fd) ->
    {stop, normal, Fd}.


handle_info(timeout, St) ->
    case is_idle(St) of
        true ->
            {stop, normal, St};
        false ->
            {noreply, St, ?MONITOR_CHECK}
    end;

handle_info({'EXIT', Pid, _}, #st{db_pid = Pid} = St) ->
    case is_idle(St) of
        true -> {stop, normal, St};
        false -> {noreply, St, ?MONITOR_CHECK}
    end;

handle_info({'EXIT', _, normal}, St) ->
    {noreply, St, ?MONITOR_CHECK};

handle_info({'EXIT', _, Reason}, St) ->
    {stop, Reason, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


init_st(FilePath, RawFd, Parent, Options) ->
    maybe_track(Options),
    case proplists:get_value(overwrite, Options) of
        true ->
            nifile:truncate(RawFd, 0);
        _ ->
            ok
    end,
    CompressOpts = case proplists:get_value(compression, Options) of
        true ->
            [compressed];
        N when is_integer(N), N >= 0, N =< 9 ->
            [{compressed, N}];
        _ ->
            []
    end,
    {ok, Bytes} = nifile:seek(RawFd, 0, seek_end),
    #st{
        path = iolist_to_binary(FilePath),
        fd = RawFd,
        eof = Bytes,
        is_sys = lists:member(sys_db, Options),
        t2bopts = CompressOpts ++ [{minor_version, 1}],
        db_pid = Parent
    }.


maybe_track(Options) ->
    IsSys = lists:member(sys_db, Options),
    if IsSys -> ok; true ->
        couch_stats_process_tracker:track([couchdb_ngen, open_nifiles])
    end.


is_idle(#st{is_sys = true}) ->
    case process_info(self(), monitored_by) of
        {monitored_by, []} -> true;
        _ -> false
    end;

is_idle(#st{}) ->
    Tracker = whereis(couch_stats_process_tracker),
    case process_info(self(), monitored_by) of
        {monitored_by, []} -> true;
        {monitored_by, [Tracker]} -> true;
        {monitored_by, [_]} -> exit(tracker_monitoring_failed);
        _ -> false
    end.
