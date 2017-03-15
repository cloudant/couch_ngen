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
    t2bopts,
    mode :: raw | crc32 | {gcm, pid()}
}).


-record(st, {
    path,
    fd,
    is_sys = false,
    eof = 0,
    t2bopts = [],
    db_pid,
    mode :: raw | crc32 | {gcm, pid()}
}).


-include_lib("kernel/include/file.hrl").


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

append_bin(#ngenfd{mode = crc32} = Fd, Bin) when is_binary(Bin) ->
    Crc32 = erlang:crc32(Bin),
    gen_server:call(Fd#ngenfd.pid,
        {append, <<Crc32:32/integer, Bin/binary>>});

append_bin(#ngenfd{} = Fd, Bin) ->
    gen_server:call(Fd#ngenfd.pid, {append, Bin}).


read_term(Fd, Ptr) ->
    {ok, Bin} = read_bin(Fd, Ptr),
    {ok, binary_to_term(Bin)}.


read_bin(#ngenfd{mode = raw} = Fd, {Pos, Len}) ->
    nifile:pread(Fd#ngenfd.fd, Len, Pos);

read_bin(#ngenfd{mode = crc32} = Fd, {Pos, Len}) ->
    case nifile:pread(Fd#ngenfd.fd, Len, Pos) of
        {ok, <<Crc32:32/integer, Bin/binary>>} ->
            case erlang:crc32(Bin) of
                Crc32 ->
                    {ok, Bin};
                _Other ->
                    {error, crc32_failed}
            end;
        {error, Reason} ->
            {error, Reason}
    end;

read_bin(#ngenfd{mode = {gcm, Pid}} = Fd, {Pos, Len}) ->
    case nifile:pread(Fd#ngenfd.fd, Len, Pos) of
        {ok, Bin} ->
            case couch_ngen_crypto:decrypt(Pid, Pos, Bin) of
                error ->
                    {error, decryption_failed};
                PlainText ->
                    {ok, PlainText}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


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
            case init_st(FilePath, Fd, Parent, Options) of
                {ok, #st{} = St} ->
                    ExtFd = #ngenfd{
                        pid = self(),
                        fd = Fd,
                        t2bopts = St#st.t2bopts,
                        mode = St#st.mode
                    },
                    proc_lib:init_ack({ok, ExtFd}),
                    gen_server:enter_loop(?MODULE, [], St, ?INITIAL_WAIT);
                {error, Reason} ->
                    proc_lib:init_ack({error, Reason})
            end;
        {error, Reason} ->
            proc_lib:init_ack({error, Reason})
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

handle_call({append, Bin0}, _From, St) ->
    #st{
        eof = Eof
    } = St,

    Bin = maybe_encrypt(St, Bin0),
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
            %% some data might have been written before the error
            %% so we need to check eof the slow way.
            {ok, Eof} = file:position(St#st.fd, eof),
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
    init_mode(#st{
        path = iolist_to_binary(FilePath),
        fd = RawFd,
        eof = Bytes,
        is_sys = lists:member(sys_db, Options),
        t2bopts = CompressOpts ++ [{minor_version, 1}],
        db_pid = Parent
    }, Options).

init_mode(#st{} = St, Options) ->
    Raw = lists:member(raw, Options),
    Creating = lists:member(create, Options),
    HasKeyIdOpt = keyid(Options) /= undefined,

    case {Raw, Creating, HasKeyIdOpt} of
        {true, _, _} ->
            set_raw_mode(St);
        {false, true, false} ->
            init_crc32_mode(St);
        {false, true, true} ->
            init_gcm_mode(St, Options);
        {false, false, _} ->
            case get_mode(St) of
                {ok, <<"crc32", _/binary>>} ->
                    set_crc32_mode(St);
                {ok, <<"gcm", _/binary>> = Mode} ->
                    set_gcm_mode(St, Mode);
                {error, Reason} ->
                    {error, Reason}
            end;
        _Else ->
            {error, invalid_file}
    end.


get_mode(St) ->
    file:read_file(mode_file(St)).


mode_file(St) ->
    <<(St#st.path)/binary, ".mode">>.


set_raw_mode(St) ->
    {ok, St#st{mode = raw}}.


init_crc32_mode(#st{} = St) ->
    ok = file:write_file(mode_file(St), <<"crc32">>, [exclusive]),
    ok = set_read_only(mode_file(St)),
    set_crc32_mode(St).


set_crc32_mode(St) ->
    {ok, St#st{mode = crc32}}.


init_gcm_mode(#st{} = St, Options) ->
    KeyId = keyid(Options),
    KeyIdSize = byte_size(KeyId),
    {ok, Pid, WrappedKey} = couch_ngen_crypto:start_link(KeyId),
    Bin = <<$g, $c, $m,
            KeyIdSize:16,
            KeyId/binary,
            WrappedKey/binary>>,
    ok = file:write_file(mode_file(St), Bin, [exclusive]),
    ok = set_read_only(mode_file(St)),
    {ok, St#st{mode = {gcm, Pid}}}.


set_gcm_mode(St, Mode) ->
    <<$g, $c, $m,
      KeyIdSize:16,
      KeyId:KeyIdSize/binary,
      WrappedKey/binary>> = Mode,
    case couch_ngen_crypto:start_link(KeyId, WrappedKey) of
        {ok, Pid} ->
            {ok, St#st{mode = {gcm, Pid}}};
        {error, Reason} ->
            {error, Reason}
    end.


set_read_only(Filename) ->
    {ok, FileInfo} = file:read_file_info(Filename),
    Mode0 = FileInfo#file_info.mode,
    Mode1 = Mode0 band 8#00444,
    file:write_file_info(Filename, FileInfo#file_info{mode = Mode1}).

keyid(Options) when is_list(Options) ->
    {EngineOptions} = proplists:get_value(engine_options, Options, {[]}),
    proplists:get_value(<<"keyid">>, EngineOptions).


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


maybe_encrypt(#st{mode = {gcm, Pid}} = St, Bin) ->
    couch_ngen_crypto:encrypt(Pid, St#st.eof, Bin);

maybe_encrypt(#st{}, Bin) ->
    Bin.
