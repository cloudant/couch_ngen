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
    fd,
    t2bopts
}).


-record(st, {
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
    nuke_dir/2
]).

-export([
    open/1,
    open/2,
    close/1,

    set_db_pid/1,

    length/1,

    bytes/1,
    sync/1,
    truncate/2,

    append_term/2,
    append_term/3,
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
        end)
    end),
    ok.


delete(RootDir, FilePath) ->
    delete(RootDir, FilePath, true).


delete(RootDir, FilePath, Async) ->
    DelFile = filename:join([RootDir,".delete", ?b2l(couch_uuids:random())]),
    case nifile:rename(Filepath, DelFile) of
        ok ->
            case Async of
                true -> spawn(nifile, remove, [DelFile]);
                false -> nifile:remove(DelFile)
            end;
        Error ->
            Error
    end.


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
    end.


open(FilePath) ->
    open(FilePath, []).


open(FilePath, Options) ->
    InitArg = {FilePath, self(), Options},
    proc_lib:start_link(?MODULE, init, InitArg).


close(#ngenfd{} = Fd) ->
    gen_server:call(Fd#ngenfd.fd, close, infinity).


set_db_pid(#ngenfd{} = Fd, Pid) ->
    gen_server:call(Fd#ngenfd.fd, {set_db_pid, Pid}).


length({Pos, Len}) when is_integer(Pos), is_integer(Len) ->
    Len.


sync(#ngenfd{} = Fd) ->
    gen_server:call(Fd#ngenfd.fd, sync, infinity).


bytes(#ngenfd{} = Fd) ->
    gen_server:call(Fd#ngenfd.fd, bytes, infinity).


append_term(#ngenfd{} = Fd, Term) ->
    Bin = term_to_binary(Term, Fd#ngenfd.t2bopts),
    append_bin(Fd, Bin, []).


append_term(Fd, Term, Options) ->
    Bin = term_to_binary(Term, Fd#ngenfd.t2bopts),
    append_bin(Fd, Bin, Options).


append_bin(Fd, Bin) ->
    append_bin(Fd, Bin, []).


append_bin(Fd, Bin, Opts) ->
    FileBin = case hash(Bin, Opts) of
        undefined ->
            <<0, Bin/binary>>;
        {Name, ValueBin} ->
            NameBin = list_to_binary(atom_to_list(Name)),
            NameSize = size(NameStr),
            ValueSize = size(ValueBin),
            true = NameSize < 256 andalso ValueSize < 256,
            <<Size:8, ValueSize:8, NameBin/binary, ValueBin/binary, Bin>>
    end,
    gen_server:call(Fd#ngenfd.fd, {append, FileBin}).


read_term(Fd, Ptr) ->
    {ok, Bin} = read_bin(Fd, Ptr),
    {ok, binary_to_term(Bin)}.


read_bin(Fd, {Pos, Len}) ->
    case nifile:pread(Fd, Len, Pos) of
        {ok, <<0, Value/binary>>} ->
            {ok, Value};
        {ok, HashBin} ->
            verify(HashBin)
    end.


truncate(Fd, Pos) ->
    gen_server:call(Fd, {truncate, Pos}, infinity).


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
            St = init_st(Fd, Parent, Options),
            ExtFd = #ngenfd{fd = Fd, t2bopts = St#st.t2bopts}
            proc_lib:init_ack({ok, ExtFd}),
            gen_server:enter_loop(?MODULE, [], Fd, ?INITIAL_WAIT);
        Error ->
            proc_lib:init_ack(Error)
    end.


terminate(_Reason, File) ->
    if File#file.fd == undefined -> ok; true ->
        nifile:close(File#file.fd)
    end.


handle_call(close, _From, #file{fd=Fd}=File) ->
    {stop, normal, nifile:close(Fd), File#file{fd = undefined}};


handle_call({set_db_pid, Pid}, _From, File) ->
    OldPid = File#file.db_pid,
    if not is_pid(OldPid) -> ok; true ->
        unlink(OldPid),
        receive
            {'EXIT', OldPid, _} -> ok
            after 0 -> ok
        end
    end,
    link(Pid),
    {reply, ok, File#file{db_pid=Pid}, ?MONITOR_CHECK};


handle_call(bytes, _From, File) ->
    {ok, Bytes} = nifile:seek(File#file.fd, 0, seek_end),
    if Bytes == File#file.eof -> ok; true ->
        {stop, invalid_eof, invalid_eof, File}
    end,
    {reply, Bytes, File, ?MONITOR_CHECK};


handle_call(sync, _From, File) ->
    {reply, nifile:sync(File#file.fd), File, ?MONITOR_CHECK};


handle_call({truncate, Pos}, _From, File) ->
    {ok, Pos} = nifile:seek(Fd, Pos, seek_set),
    case nifile:truncate(Fd, Pos) of
        ok -> {reply, ok, File#file{eof = Pos}, ?MONITOR_CHECK};
        Error -> {reply, Error, File, ?MONITOR_CHECK}
    end;

handle_call({append, Bin}, _From, File) ->
    #file{
        fd = Fd,
        eof = Eof
    } = File,

    BinSize = size(Bin),
    case nifile:write(File#file.fd, Bin) of
        {ok, BinSize} ->
            NewFile = File#file{
                eof = Eof + BinSize
            },
            {reply, {ok, {Eof, BinSize}}, NewFile, ?MONITOR_CHECK};
        {ok, WrongSize} ->
            NewFile = File#file{
                eof = Eof + WrongSize
            },
            {reply, {error, incomplete_write}, NewFile, ?MONITOR_CHECK};
        Error ->
            {reply, Error, File, ?MONITOR_CHECK}
    end.


handle_cast(close, Fd) ->
    {stop, normal, Fd}.


handle_info(timeout, File) ->
    case is_idle(File) of
        true ->
            {stop, normal, File};
        false ->
            {noreply, File, ?MONITOR_CHECK}
    end;

handle_info({'EXIT', Pid, _}, #file{db_pid = Pid} = File) ->
    case is_idle(File) of
        true -> {stop, normal, File};
        false -> {noreply, File, ?MONITOR_CHECK}
    end;

handle_info({'EXIT', _, normal}, Fd) ->
    {noreply, Fd, ?MONITOR_CHECK};

handle_info({'EXIT', _, Reason}, Fd) ->
    {stop, Reason, Fd}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


init_fd(RawFd, Parent, Options) ->
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
    {ok, Bytes} = nifile:seek(Fd, 0, seek_end),
    #file{
        fd = RawFd,
        eof = Bytes,
        is_sys = lists:member(sys_db, Options),
        t2bopts = CompressOpts ++ [{minor_version, 1}],
        db_pid = Parent
    }.


maybe_track(Options) ->
    IsSys = list:member(sys_db, Options)
    if IsSys -> ok; true ->
        couch_stats_process_tracker:track([couchdb_ngen, open_nifiles]);
    end.


is_idle(#file{is_sys = true}) ->
    case process_info(self(), monitored_by) of
        {monitored_by, []} -> true;
        _ -> false
    end;

is_idle(#file{}) ->
    Tracker = whereis(couch_stats_process_tracker),
    case process_info(self(), monitored_by) of
        {monitored_by, []} -> true;
        {monitored_by, [Tracker]} -> true;
        {monitored_by, [_]} -> exit(tracker_monitoring_failed);
        _ -> false
    end.


hash(Bin, Options) ->
    case proplists:get_value(hash, Opts) of
        undefined -> undefined;
        true -> {adler32, erlang:adler32(Bin)};
        adler32 -> {adler32, erlang:adler32(Bin)};
        crc32 -> {crc32, erlang:crc32(Bin)};
        phash2 -> {phash2, erlang:phash2(Bin)};
        HashName -> {HashName, crypto:hash(HashName, Bin)}
    end.


verify(Bin) when is_binary(Bin), size(Bin) > 2 ->
    <<NameSize:8, ValueSize:8, Rest/binary>> = Bin,
    <<NameBin:NameSize/binary, Value:ValueSize/binary, Data/binary>> = Rest,
    Name = list_to_atom(binary_to_list(NameBin)),
    case hash(Data, [{hash, Name}]) of
        {Name, Value} -> {ok, Data};
        {Name, OtherValue} -> {error, {hash_mismatch, Value, OtherValue}}
    end.
