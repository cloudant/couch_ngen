-module(couch_ngen).
-behavior(couch_db_engine).

-export([
    exists/1,

    delete/3,
    delete_compaction_files/2,

    init/2,
    terminate/2,
    handle_call/2,
    handle_info/2,

    incref/1,
    decref/1,
    monitored_by/1,

    get/2,
    get/3,
    set/3,

    open_docs/2,
    open_local_docs/2,
    read_doc/2,

    make_doc_summary/2,
    write_doc_summary/2,
    write_doc_infos/4,

    commit_data/1,

    open_write_stream/2,
    open_read_stream/2,
    is_active_stream/2,

    fold_docs/4,
    fold_local_docs/4,
    fold_changes/5,
    count_changes_since/2,

    start_compaction/4,
    finish_compaction/4
]).


% These are used by the compactor
-export([
    init_state/6,
    open_idx_data_files/3,
    read_header/2,
    write_header/3,
    update_header/2,
    write_doc_info/2
]).


-export([
    id_seq_tree_split/2,
    id_seq_tree_join/3,

    id_tree_reduce/2,
    seq_tree_reduce/2,

    local_tree_split/2,
    local_tree_join/3
]).


-include_lib("couch/include/couch_db.hrl").
-include("couch_ngen.hrl").


-record(wiacc, {
    new_ids = [],
    rem_ids = [],
    new_seqs = [],
    rem_seqs = [],
    update_seq
}).


exists(DirPath) ->
    CPFile = filename:join(DirPath, "COMMITS"),
    filelib:is_file(CPFile).


delete(RootDir, DirPath, Async) ->
    %% Delete any leftover compaction files. If we don't do this a
    %% subsequent request for this DB will try to open them to use
    %% as a recovery.
    couch_ngen_file:nuke_dir(RootDir, DirPath, Async).


delete_compaction_files(RootDir, DirPath) ->
    nifile:lsdir(DirPath, fun(FName, _) ->
        FNameLen = size(FName),
        WithoutCompact = FNameLen - 8,
        case FName of
            <<_:WithoutCompact/binary, ".compact">> ->
                couch_ngen_file:delete(RootDir, FName);
            _ ->
                ok
        end
    end, nil).


init(DirPath, Options) ->
    {ok, CPFd, IdxFd, DataFd} = open_db_files(DirPath, Options),
    Header = case lists:member(create, Options) of
        true ->
            delete_compaction_files(DirPath),
            couch_ngen_header:new();
        false ->
            case read_header(CPFd, IdxFd) of
                {ok, Header0} ->
                    Header0;
                no_valid_header ->
                    delete_compaction_files(DirPath),
                    Header0 =  couch_db_header:new(),
                    ok = write_header(CPFd, IdxFd, Header0),
                    Header0
            end
    end,
    {ok, init_state(DirPath, CPFd, IdxFd, DataFd, Header, Options)}.


terminate(_Reason, St) ->
    lists:foreach(fun(Fd) ->
        catch couch_ngen_file:close(Fd),
        couch_util:shutdown_sync(Fd)
    end, [St#st.cp_fd, St#st.idx_fd, St#st.data_fd]),
    ok.


handle_call(Msg, St) ->
    {stop, {invalid_call, Msg}, {invalid_call, Msg}, St}.


handle_info({'DOWN', _, _, _, _}, St) ->
    {stop, normal, St}.


incref(St) ->
    Monitors = [
        couch_ngen_file:monitor(St#st.idx_fd),
        couch_ngen_file:monitor(St#st.data_fd)
    ],
    {ok, St#st{fd_monitors = Monitors}}.


decref(St) ->
    lists:foreach(fun(Ref) ->
        true = erlang:demonitor(Ref, [flush])
    end, St#st.fd_monitors),
    ok.


monitored_by(St) ->
    lists:foreach(fun(Fd, Acc) ->
        case erlang:process_info(couch_ngen_file:pid(Fd), monitored_by) of
            {monitred_by, MB} ->
                lists:umerge(lists:usort(MB), Acc);
            _ ->
                Acc
        end
    end, [], [St#st.cp_fd, St#st.idx_fd, St#st.data_fd]).


get(#st{} = St, DbProp) ->
    ?MODULE:get(St, DbProp, undefined).


get(#st{} = St, last_purged, _) ->
    case ?MODULE:get(St, purged_docs, nil) of
        nil ->
            [];
        Pointer ->
            {ok, Purged} = couch_ngen_file:read_term(St#st.data_fd, Pointer),
            Purged
    end;

get(#st{} = St, doc_count, _) ->
    {ok, {Count, _, _}} = couch_ngen_btree:full_reduce(St#st.id_tree),
    Count;

get(#st{} = St, del_doc_count, _) ->
    {ok, {_, DelCount, _}} = couch_ngen_btree:full_reduce(St#st.id_tree),
    DelCount;

get(#st{} = St, size_info, _) ->
    {ok, IdxSize} = couch_ngen_file:bytes(St#st.idx_fd),
    {ok, DataSize} = couch_ngen_file:bytes(St#st.data_fd),
    FileSize = IdxSize + DataSize,

    {ok, DbReduction} = couch_ngen_btree:full_reduce(St#st.id_tree),
    SizeInfo0 = element(3, DbReduction),
    SizeInfo = case SizeInfo0 of
        SI when is_record(SI, size_info) ->
            SI;
        {AS, ES} ->
            #size_info{active=AS, external=ES};
        AS ->
            #size_info{active=AS}
    end,
    ActiveSize = active_size(St, SizeInfo),
    ExternalSize = SizeInfo#size_info.external,
    [
        {active, ActiveSize},
        {external, ExternalSize},
        {file, FileSize}
    ];

get(#st{} = St, security, _) ->
    case ?MODULE:get(St, security_ptr, nil) of
        nil ->
            [];
        Pointer ->
            {ok, SecProps} = couch_ngen_file:read_term(St#st.data_fd, Pointer),
            SecProps
    end;

get(#st{header = Header}, DbProp, Default) ->
    couch_ngen_header:get(Header, DbProp, Default).


set(#st{} = St, security, NewSecurity) ->
    {ok, Ptr} = couch_ngen_file:append_term(St#st.data_fd, NewSecurity),
    set(St, security_ptr, Ptr);

set(#st{} = St, update_seq, Value) ->
    #st{
        header = Header
    } = St,
    {ok, St#st{
        header = couch_ngen_header:set(Header, [
            {update_seq, Value}
        ]),
        needs_commit = true
    }};

set(#st{} = St, compacted_seq, Value) ->
    #st{
        header = Header
    } = St,
    {ok, St#st{
        header = couch_ngen_header:set(Header, [
            {compacted_seq, Value}
        ]),
        needs_commit = true
    }};

set(#st{} = St, DbProp, Value) ->
    #st{
        header = Header
    } = St,
    UpdateSeq = couch_ngen_header:get(Header, update_seq),
    {ok, St#st{
        header = couch_ngen_header:set(Header, [
            {DbProp, Value},
            {update_seq, UpdateSeq + 1}
        ]),
        needs_commit = true
    }}.


open_docs(#st{} = St, DocIds) ->
    Results = couch_ngen_btree:lookup(St#st.id_tree, DocIds),
    lists:map(fun
        ({ok, FDI}) ->
            FDI;
        (not_found) ->
            not_found
    end, Results).


open_local_docs(#st{} = St, DocIds) ->
    Results = couch_ngen_btree:lookup(St#st.local_tree, DocIds),
    lists:map(fun
        ({ok, Doc}) ->
            Doc;
        (not_found) ->
            not_found
    end, Results).


read_doc(#st{} = St, Ptr) ->
    couch_ngen_file:read_term(St#st.data_fd, Ptr).


make_doc_summary(#st{}, {Body, Atts}) ->
    ?term_to_bin({Body, Atts}).


write_doc_summary(#st{} = St, Bin) ->
    #st{
        data_fd = Fd
    } = St,
    {ok, {Pos, Len}} = couch_ngen_file:append_bin(Fd, Bin),
    {ok, {Pos, Len}, Len}.


write_doc_infos(#st{} = St, Pairs, LocalDocs, PurgeInfo) ->
    #st{
        id_tree = IdTree,
        seq_tree = SeqTree,
        local_tree = LocalTree
    } = St,

    #wiacc{
        new_ids = NewIds,
        rem_ids = RemIds,
        new_seqs = NewSeqs,
        rem_seqs = RemSeqs,
        update_seq = NewSeq
    } = get_write_info(St, Pairs),

    {ok, IdTree2} = couch_ngen_btree:add_remove(IdTree, NewIds, RemIds),
    {ok, SeqTree2} = couch_ngen_btree:add_remove(SeqTree, NewSeqs, RemSeqs),

    {AddLDocs, RemLDocIds} = lists:foldl(fun(Doc, {AddAcc, RemAcc}) ->
        case Doc#doc.deleted of
            true ->
                {AddAcc, [Doc#doc.id | RemAcc]};
            false ->
                {[Doc | AddAcc], RemAcc}
        end
    end, {[], []}, LocalDocs),
    {ok, LocalTree2} = couch_ngen_btree:add_remove(
            LocalTree, AddLDocs, RemLDocIds),

    NewHeader = case PurgeInfo of
        [] ->
            couch_ngen_header:set(St#st.header, [
                {update_seq, NewSeq}
            ]);
        _ ->
            {ok, Ptr} = couch_ngen_file:append_term(St#st.data_fd, PurgeInfo),
            OldPurgeSeq = couch_ngen_header:get(St#st.header, purge_seq),
            couch_ngen_header:set(St#st.header, [
                {update_seq, NewSeq + 1},
                {purge_seq, OldPurgeSeq + 1},
                {purged_docs, Ptr}
            ])
    end,

    {ok, St#st{
        header = NewHeader,
        id_tree = IdTree2,
        seq_tree = SeqTree2,
        local_tree = LocalTree2,
        needs_commit = true
    }}.


commit_data(St) ->
    #st{
        fsync_options = FsyncOptions,
        header = OldHeader,
        needs_commit = NeedsCommit
    } = St,

    Fds = [St#st.cp_fd, St#st.idx_fd, St#st.data_fd],

    NewHeader = update_header(St, OldHeader),

    case NewHeader /= OldHeader orelse NeedsCommit of
        true ->
            Before = lists:member(before_header, FsyncOptions),
            After = lists:member(after_header, FsyncOptions),

            if not Before -> ok; true ->
                [couch_ngen_file:sync(Fd) || Fd <- Fds]
            end,

            ok = write_header(St#st.cp_fd, St#st.idx_fd, NewHeader),

            if not After -> ok; true ->
                [couch_ngen_file:sync(Fd) || Fd <- Fds]
            end,

            {ok, St#st{
                header = NewHeader,
                needs_commit = false
            }};
        false ->
            {ok, St}
    end.


open_write_stream(#st{} = St, Options) ->
    couch_stream:open({couch_ngen_stream, {St#st.data_fd, []}}, Options).


open_read_stream(#st{} = St, StreamSt) ->
    {ok, {couch_ngen_stream, {St#st.data_fd, StreamSt}}}.


is_active_stream(#st{} = St, {couch_ngen_stream, {Fd, _}}) ->
    St#st.data_fd == Fd;
is_active_stream(_, _) ->
    false.


fold_docs(St, UserFun, UserAcc, Options) ->
    fold_docs_int(St#st.id_tree, UserFun, UserAcc, Options).


fold_local_docs(St, UserFun, UserAcc, Options) ->
    fold_docs_int(St#st.local_tree, UserFun, UserAcc, Options).


fold_changes(St, SinceSeq, UserFun, UserAcc, Options) ->
    Fun = fun drop_reductions/4,
    InAcc = {UserFun, UserAcc},
    Opts = [{start_key, SinceSeq + 1}] ++ Options,
    {ok, _, OutAcc} = couch_ngen_btree:fold(St#st.seq_tree, Fun, InAcc, Opts),
    {_, FinalUserAcc} = OutAcc,
    {ok, FinalUserAcc}.


count_changes_since(St, SinceSeq) ->
    BTree = St#st.seq_tree,
    FoldFun = fun(_SeqStart, PartialReds, 0) ->
        {ok, couch_ngen_btree:final_reduce(BTree, PartialReds)}
    end,
    Opts = [{start_key, SinceSeq + 1}],
    {ok, Changes} = couch_ngen_btree:fold_reduce(BTree, FoldFun, 0, Opts),
    Changes.


start_compaction(St, DbName, Options, Parent) ->
    Args = [St, DbName, Options, Parent],
    Pid = spawn_link(couch_ngen_compactor, start, Args),
    {ok, St, Pid}.


finish_compaction(SrcSt, DbName, Options, DirPath) ->
    {ok, TgtSt1} = ?MODULE:init(DirPath, [compactor | Options]),
    SrcSeq = ?MODULE:get(SrcSt, update_seq),
    TgtSeq = ?MODULE:get(TgtSt1, update_seq),
    case SrcSeq == TgtSeq of
        true ->
            finish_compaction_int(SrcSt, TgtSt1);
        false ->
            couch_log:info("Compaction file still behind main file "
                           "(update seq=~p. compact update seq=~p). Retrying.",
                           [SrcSeq, TgtSeq]),
            ok = decref(TgtSt1),
            start_compaction(SrcSt, DbName, Options, self())
    end.


id_seq_tree_split({Key, Ptr}, _DataFd) ->
    {Key, Ptr}.


id_seq_tree_join(_Key, DiskPtr, DataFd) ->
    {ok, DiskTerm} = couch_ngen_file:read_term(DataFd, DiskPtr),
    {Id, Seq, Deleted, Sizes, DiskTree} = DiskTerm,
    #full_doc_info{
        id = Id,
        update_seq = Seq,
        deleted = ?i2b(Deleted),
        sizes = couch_db_updater:upgrade_sizes(Sizes),
        rev_tree = rev_tree(DiskTree)
    }.


id_tree_reduce(reduce, FullDocInfos) ->
    FoldFun = fun(Info, {Count, DelCount, Sizes}) ->
        Sizes2 = reduce_sizes(Sizes, Info#full_doc_info.sizes),
        case Info#full_doc_info.deleted of
            true -> {Count, DelCount + 1, Sizes2};
            false -> {Count + 1, DelCount, Sizes2}
        end
    end,
    lists:foldl(FoldFun, {0, 0, #size_info{}}, FullDocInfos);
id_tree_reduce(rereduce, Reds) ->
    FoldFun = fun({Count1, DelCount1, Sizes1}, {Count2, DelCount2, Sizes2}) ->
            Sizes3 = reduce_sizes(Sizes1, Sizes2),
            {Count1 + Count2, DelCount1 + DelCount2, Sizes3}
    end,
    lists:foldl(FoldFun, {0, 0, #size_info{}}, Reds).


seq_tree_reduce(reduce, DocInfos) ->
    % count the number of documents
    length(DocInfos);
seq_tree_reduce(rereduce, Reds) ->
    lists:sum(Reds).


local_tree_split(#doc{} = Doc, DataFd) ->
    #doc{
        id = Id,
        revs = {0, [Rev]},
        body = BodyData
    } = Doc,
    DiskTerm = {Rev, BodyData},
    {ok, Ptr} = couch_ngen_file:append_term(DataFd, DiskTerm),
    {Id, Ptr}.


local_tree_join(Id, Ptr, DataFd) ->
    {ok, {Rev, BodyData}} = couch_ngen_file:read_term(DataFd, Ptr),
    #doc{
        id = Id,
        revs = {0, [Rev]},
        body = BodyData
    }.


read_header(CPFd, IdxFd) ->
    {ok, FileSize} = couch_ngen_file:bytes(CPFd),
    LastHeader = 16 * (FileSize div 16),
    read_header(CPFd, IdxFd, LastHeader).


% 80 buffer because the Data and Index UUID names
% are the first 64 bytes and then 16 for the last
% possible header position makes 80
read_header(CPFd, IdxFd, FileSize) when FileSize >= 80 ->
    Ptr = {FileSize - 16, 16},
    {ok, <<Pos:64, Len:64>>} = couch_ngen_file:read_bin(CPFd, Ptr),
    case couch_ngen_file:read_term(IdxFd, {Pos, Len}) of
        {ok, Header} ->
            {ok, Header};
        {error, _} ->
            read_header(CPFd, IdxFd, FileSize - 16)
    end;

read_header(_, _, _) ->
    no_valid_header.


write_header(CPFd, IdxFd, Header) ->
    {ok, {Pos, Len}} = couch_ngen_file:append_term(IdxFd, Header),

    {ok, CPSize} = couch_ngen_file:bytes(CPFd),
    if (CPSize rem 16) == 0 -> ok; true ->
        throw({invalid_commits_file, CPSize})
    end,

    CPBin = <<Pos:64, Len:64>>,
    {ok, _} = couch_ngen_file:append_bin(CPFd, CPBin),
    ok.


open_db_files(DirPath, Options) ->
    CPPath = db_filepath(DirPath, "COMMITS", "", Options),
    case lists:member(create, Options) of
        true -> filelib:ensure_dir(CPPath);
        false -> ok
    end,
    case couch_ngen_file:open(CPPath, [raw | Options]) of
        {ok, Fd} ->
            open_idx_data_files(DirPath, Fd, Options);
        {error, enoent} ->
            % If we're recovering from a COMMITS.compact we
            % only treat that as valid if we've already
            % moved the index and data files or else compaction
            % wasn't finished. Hence why we're not renaming them
            % here.
            case couch_ngen_file:open(CPPath ++ ".compact", [raw]) of
                {ok, Fd} ->
                    Fmt = "Recovering from compaction file: ~s~s",
                    couch_log:info(Fmt, [CPPath, ".compact"]),
                    ok = couch_ngen_file:rename(Fd, CPPath),
                    ok = couch_ngen_file:sync(Fd),
                    open_idx_data_files(DirPath, Fd, Options);
                {error, enoent} ->
                    throw({not_found, no_db_file})
            end;
        Error ->
            throw(Error)
    end.


open_idx_data_files(DirPath, CPFd, Options) ->
    % TODO: Grab this from the config
    HashOpt = {hash, crc32},
    {ok, IdxPath, DataPath} = get_file_paths(DirPath, CPFd, Options),
    {ok, IdxFd} = couch_ngen_file:open(IdxPath, [HashOpt | Options]),
    {ok, DataFd} = couch_ngen_file:open(DataPath, [HashOpt | Options]),
    {ok, CPFd, IdxFd, DataFd}.


get_file_paths(DirPath, CPFd, Options) ->
    case couch_ngen_file:read_bin(CPFd, {0, 64}) of
        {ok, <<>>} ->
            IdxName = couch_uuids:random(),
            DataName = couch_uuids:random(),
            {ok, _} = couch_ngen_file:append_bin(CPFd, IdxName),
            {ok, _} = couch_ngen_file:append_bin(CPFd, DataName),
            couch_ngen_file:sync(CPFd),

            IdxPath = db_filepath(DirPath, IdxName, ".idx", Options),
            DataPath = db_filepath(DirPath, DataName, ".data", Options),

            {ok, IdxPath, DataPath};
        {ok, <<IdxName:32/binary, DataName:32/binary>>} ->
            IdxPath = db_filepath(DirPath, IdxName, ".idx", Options),
            DataPath = db_filepath(DirPath, DataName, ".data", Options),
            {ok, IdxPath, DataPath};
        {ok, Else} ->
            erlang:error({corrupt_checkpoints_file, Else});
        {error, Reason} ->
            erlang:error(Reason)
    end.


init_state(DirPath, CPFd, IdxFd, DataFd, Header0, Options) ->
    DefaultFSync = "[before_header, after_header, on_file_open]",
    FsyncStr = config:get("couchdb", "fsync_options", DefaultFSync),
    {ok, FsyncOptions} = couch_util:parse_term(FsyncStr),

    FsyncOnOpen = lists:member(on_file_open, FsyncOptions),
    if not FsyncOnOpen -> ok; true ->
        [ok = couch_ngen_file:sync(Fd) || Fd <- [CPFd, IdxFd, DataFd]]
    end,

    Header = couch_ngen_header:upgrade(Header0),

    IdTreeState = couch_ngen_header:id_tree_state(Header),
    {ok, IdTree} = couch_ngen_btree:open(IdTreeState, IdxFd, [
            {split, fun ?MODULE:id_seq_tree_split/2},
            {join, fun ?MODULE:id_seq_tree_join/3},
            {reduce, fun ?MODULE:id_tree_reduce/2},
            {user_ctx, DataFd}
        ]),

    SeqTreeState = couch_ngen_header:seq_tree_state(Header),
    {ok, SeqTree} = couch_ngen_btree:open(SeqTreeState, IdxFd, [
            {split, fun ?MODULE:id_seq_tree_split/2},
            {join, fun ?MODULE:id_seq_tree_join/3},
            {reduce, fun ?MODULE:seq_tree_reduce/2},
            {user_ctx, DataFd}
        ]),

    LocalTreeState = couch_ngen_header:local_tree_state(Header),
    {ok, LocalTree} = couch_ngen_btree:open(LocalTreeState, IdxFd, [
            {split, fun ?MODULE:local_tree_split/2},
            {join, fun ?MODULE:local_tree_join/3},
            {user_ctx, DataFd}
        ]),

    [couch_ngen_file:set_db_pid(Fd, self()) || Fd <- [CPFd, IdxFd, DataFd]],

    St = #st{
        dirpath = DirPath,
        cp_fd = CPFd,
        idx_fd = IdxFd,
        data_fd = DataFd,
        fd_monitors = [
            couch_ngen_file:monitor(CPFd),
            couch_ngen_file:monitor(IdxFd),
            couch_ngen_file:monitor(DataFd)
        ],
        fsync_options = FsyncOptions,
        header = Header,
        needs_commit = false,
        id_tree = IdTree,
        seq_tree = SeqTree,
        local_tree = LocalTree
    },

    UpgradedHeader = Header /= Header0,
    IsNewDb = couch_ngen_file:bytes(IdxFd) == {ok, 0},
    NeedsUpgrade = UpgradedHeader orelse IsNewDb,
    if not NeedsUpgrade -> St; true ->
        {ok, NewSt} = commit_data(St),
        NewSt
    end.


update_header(St, Header) ->
    couch_ngen_header:set(Header, [
        {seq_tree_state, couch_ngen_btree:get_state(St#st.seq_tree)},
        {id_tree_state, couch_ngen_btree:get_state(St#st.id_tree)},
        {local_tree_state, couch_ngen_btree:get_state(St#st.local_tree)}
    ]).


delete_compaction_files(DirPath) ->
    RootDir = config:get("couchdb", "database_dir", "."),
    delete_compaction_files(RootDir, DirPath).


get_write_info(St, Pairs) ->
    Acc = #wiacc{update_seq = ?MODULE:get(St, update_seq)},
    get_write_info(St, Pairs, Acc).


get_write_info(_St, [], Acc) ->
    Acc;

get_write_info(St, [{OldFDI, NewFDI} | Rest], Acc) ->
    NewAcc = case {OldFDI, NewFDI} of
        {not_found, #full_doc_info{}} ->
            #full_doc_info{
                id = Id,
                update_seq = Seq
            } = NewFDI,
            {ok, Ptr} = write_doc_info(St, NewFDI),
            Acc#wiacc{
                new_ids = [{Id, Ptr} | Acc#wiacc.new_ids],
                new_seqs = [{Seq, Ptr} | Acc#wiacc.new_seqs],
                update_seq = erlang:max(Seq, Acc#wiacc.update_seq)
            };
        {#full_doc_info{id = Id}, #full_doc_info{id = Id}} ->
            #full_doc_info{
                update_seq = OldSeq
            } = OldFDI,
            #full_doc_info{
                update_seq = NewSeq
            } = NewFDI,
            {ok, Ptr} = write_doc_info(St, NewFDI),
            Acc#wiacc{
                new_ids = [{Id, Ptr} | Acc#wiacc.new_ids],
                new_seqs = [{NewSeq, Ptr} | Acc#wiacc.new_seqs],
                rem_seqs = [OldSeq | Acc#wiacc.rem_seqs],
                update_seq = erlang:max(NewSeq, Acc#wiacc.update_seq)
            };
        {#full_doc_info{}, not_found} ->
            #full_doc_info{
                id = Id,
                update_seq = Seq
            } = OldFDI,
            Acc#wiacc{
                rem_ids = [Id | Acc#wiacc.rem_ids],
                rem_seqs = [Seq | Acc#wiacc.rem_seqs]
            }
    end,
    get_write_info(St, Rest, NewAcc).


write_doc_info(St, FDI) ->
    #full_doc_info{
        id = Id,
        update_seq = Seq,
        deleted = Deleted,
        sizes = SizeInfo,
        rev_tree = Tree
    } = FDI,
    DataFd = St#st.data_fd,
    DiskTerm = {Id, Seq, ?b2i(Deleted), split_sizes(SizeInfo), disk_tree(Tree)},
    couch_ngen_file:append_term(DataFd, DiskTerm).


rev_tree(DiskTree) ->
    couch_key_tree:map(fun
        (_RevId, {Del, Ptr, Seq}) ->
            #leaf{
                deleted = ?i2b(Del),
                ptr = Ptr,
                seq = Seq
            };
        (_RevId, {Del, Ptr, Seq, Size}) ->
            #leaf{
                deleted = ?i2b(Del),
                ptr = Ptr,
                seq = Seq,
                sizes = couch_db_updater:upgrade_sizes(Size)
            };
        (_RevId, {Del, Ptr, Seq, Sizes, Atts}) ->
            #leaf{
                deleted = ?i2b(Del),
                ptr = Ptr,
                seq = Seq,
                sizes = couch_db_updater:upgrade_sizes(Sizes),
                atts = Atts
            };
        (_RevId, ?REV_MISSING) ->
            ?REV_MISSING
    end, DiskTree).


disk_tree(RevTree) ->
    couch_key_tree:map(fun
        (_RevId, ?REV_MISSING) ->
            ?REV_MISSING;
        (_RevId, #leaf{} = Leaf) ->
            #leaf{
                deleted = Del,
                ptr = Ptr,
                seq = Seq,
                sizes = Sizes,
                atts = Atts
            } = Leaf,
            {?b2i(Del), Ptr, Seq, split_sizes(Sizes), Atts}
    end, RevTree).


split_sizes(#size_info{}=SI) ->
    {SI#size_info.active, SI#size_info.external}.


reduce_sizes(nil, _) ->
    nil;
reduce_sizes(_, nil) ->
    nil;
reduce_sizes(#size_info{}=S1, #size_info{}=S2) ->
    #size_info{
        active = S1#size_info.active + S2#size_info.active,
        external = S1#size_info.external + S2#size_info.external
    };
reduce_sizes(S1, S2) ->
    US1 = couch_db_updater:upgrade_sizes(S1),
    US2 = couch_db_updater:upgrade_sizes(S2),
    reduce_sizes(US1, US2).


active_size(#st{} = St, Size) when is_integer(Size) ->
    active_size(St, #size_info{active=Size});
active_size(#st{} = St, #size_info{} = SI) ->
    Trees = [
        St#st.id_tree,
        St#st.seq_tree,
        St#st.local_tree
    ],
    lists:foldl(fun(T, Acc) ->
        case couch_ngen_btree:size(T) of
            _ when Acc == null ->
                null;
            undefined ->
                null;
            Size ->
                Acc + Size
        end
    end, SI#size_info.active, Trees).


fold_docs_int(Tree, UserFun, UserAcc, Options) ->
    Fun = fun skip_deleted/4,
    RedFun = case lists:member(include_reductions, Options) of
        true -> fun include_reductions/4;
        false -> fun drop_reductions/4
    end,
    InAcc = {RedFun, {UserFun, UserAcc}},
    {ok, Reds, OutAcc} = couch_ngen_btree:fold(Tree, Fun, InAcc, Options),
    {_, {_, FinalUserAcc}} = OutAcc,
    case lists:member(include_reductions, Options) of
        true ->
            {ok, fold_docs_reduce_to_count(Reds), FinalUserAcc};
        false ->
            {ok, FinalUserAcc}
    end.


% First element of the reductions is the total
% number of undeleted documents.
skip_deleted(traverse, _Entry, {0, _, _} = _Reds, Acc) ->
    {skip, Acc};
skip_deleted(visit, #full_doc_info{deleted = true}, _, Acc) ->
    {ok, Acc};
skip_deleted(Case, Entry, Reds, {UserFun, UserAcc}) ->
    {Go, NewUserAcc} = UserFun(Case, Entry, Reds, UserAcc),
    {Go, {UserFun, NewUserAcc}}.


include_reductions(visit, FDI, Reds, {UserFun, UserAcc}) ->
    {Go, NewUserAcc} = UserFun(FDI, Reds, UserAcc),
    {Go, {UserFun, NewUserAcc}};
include_reductions(_, _, _, Acc) ->
    {ok, Acc}.


drop_reductions(visit, FDI, _Reds, {UserFun, UserAcc}) ->
    {Go, NewUserAcc} = UserFun(FDI, UserAcc),
    {Go, {UserFun, NewUserAcc}};
drop_reductions(_, _, _, Acc) ->
    {ok, Acc}.


fold_docs_reduce_to_count(Reds) ->
    RedFun = fun id_tree_reduce/2,
    FinalRed = couch_ngen_btree:final_reduce(RedFun, Reds),
    element(1, FinalRed).


finish_compaction_int(#st{} = OldSt, #st{} = NewSt1) ->
    #st{
        dirpath = DirPath,
        local_tree = OldLocal
    } = OldSt,
    #st{
        dirpath = DirPath,
        header = Header,
        local_tree = NewLocal1
    } = NewSt1,

    % suck up all the local docs into memory and write them to the new db
    LoadFun = fun(Value, _Offset, Acc) ->
        {ok, [Value | Acc]}
    end,
    {ok, _, LocalDocs} = couch_ngen_btree:foldl(OldLocal, LoadFun, []),
    {ok, NewLocal2} = couch_ngen_btree:add(NewLocal1, LocalDocs),

    {ok, NewSt2} = commit_data(NewSt1#st{
        header = couch_bt_engine_header:set(Header, [
            {compacted_seq, ?MODULE:get(OldSt, update_seq)},
            {revs_limit, ?MODULE:get(OldSt, revs_limit)}
        ]),
        local_tree = NewLocal2
    }),

    % Move our compaction files into place
    ok = remove_compact_suffix(NewSt2#st.idx_fd),
    ok = remove_compact_suffix(NewSt2#st.data_fd),
    ok = delete_fd(NewSt2#st.idx_fd),

    % Remove the old database files
    ok = delete_fd(OldSt#st.data_fd),
    ok = delete_fd(OldSt#st.idx_fd),
    ok = delete_fd(OldSt#st.cp_fd),

    % Final swap to finish compaction
    ok = remove_compact_suffix(NewSt2#st.cp_fd),

    decref(OldSt),
    {ok, NewSt2, undefined}.


remove_compact_suffix(Fd) ->
    Path = couch_ngen_file:path(Fd),
    PathWithoutCompact = size(Path) - size(<<".compact">>),
    <<FileName:PathWithoutCompact/binary, ".compact">> = Path,
    couch_ngen_file:rename(Fd, FileName).


delete_fd(Fd) ->
    RootDir = config:get("couchdb", "database_dir", "."),
    DelDir = filename:join(RootDir, ".delete"),
    DelFname = filename:join(DelDir, couch_uuids:random()),
    couch_ngen_file:rename(Fd, DelFname).


db_filepath(DirPath, BaseName0, Suffix, Options) ->
    BaseName1 = if is_list(BaseName0) -> BaseName0; true ->
        binary_to_list(BaseName0)
    end,
    BaseName2 = BaseName1 ++ Suffix,
    case lists:member(compactor, Options) of
        true ->
            filename:join(DirPath, BaseName2 ++ ".compact");
        false ->
            filename:join(DirPath, BaseName2)
    end.

