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

-module(couch_ngen).
-behavior(couch_db_engine).

-export([
    exists/1,

    delete/3,
    delete_compaction_files/3,

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
    read_doc_body/2,

    serialize_doc/2,
    write_doc_body/2,
    write_doc_infos/3,
    purge_doc_revs/3,

    commit_data/1,

    open_write_stream/2,
    open_read_stream/2,
    is_active_stream/2,

    fold_docs/4,
    fold_local_docs/4,
    fold_changes/5,
    count_changes_since/2,
    fold_purged_docs/5,

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
    local_tree_join/3,
    purge_tree_reduce/2
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


delete_compaction_files(RootDir, DirPath, _DelOpts) ->
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
                    Header0 =  couch_ngen_header:new(),
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
    lists:foldl(fun(Fd, Acc) ->
        MB = couch_ngen_file:monitored_by(Fd),
        lists:umerge(lists:sort(MB), Acc)
    end, [], [St#st.cp_fd, St#st.idx_fd, St#st.data_fd]).


get(#st{} = St, DbProp) ->
    ?MODULE:get(St, DbProp, undefined).


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
    Pointer = ?MODULE:get(St, security_ptr, undefined),
    {ok, SecProps} = couch_ngen_file:read_term(St#st.data_fd, Pointer),
    SecProps;

get(#st{} = St, oldest_purge_seq, _) ->
    {ok, _, OldestSeq} = couch_ngen_btree:foldl(St#st.purge_tree,
            fun({K, _V}, _) -> {stop, K} end, 0),
    OldestSeq;

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


read_doc_body(#st{} = St, #doc{} = Doc) ->
    case couch_ngen_file:read_term(St#st.data_fd, Doc#doc.body) of
        {ok, {Body, Atts0}} ->
            Atts = couch_compress:decompress(Atts0),
            Doc#doc{
                body = Body,
                atts = Atts
            };
        Else ->
            Else
    end.


serialize_doc(#st{} = St, #doc{} = Doc) ->
    Compress = fun(Term) ->
        case couch_compress:is_compressed(Term, St#st.compression) of
            true -> Term;
            false -> couch_compress:compress(Term, St#st.compression)
        end
    end,
    Body = Compress(Doc#doc.body),
    Atts = Compress(Doc#doc.atts),
    Doc#doc{body = ?term_to_bin({Body, Atts})}.


write_doc_body(#st{} = St, #doc{} = Doc) ->
    #st{
        data_fd = Fd
    } = St,
    {ok, {_Pos, Len} = Ptr} = couch_ngen_file:append_bin(Fd, Doc#doc.body),
    {ok, Doc#doc{body = Ptr}, Len}.


write_doc_infos(#st{} = St, Pairs, LocalDocs) ->
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

    NewHeader = couch_ngen_header:set(St#st.header, [
        {update_seq, NewSeq}
    ]),

    {ok, St#st{
        header = NewHeader,
        id_tree = IdTree2,
        seq_tree = SeqTree2,
        local_tree = LocalTree2,
        needs_commit = true
    }}.


purge_doc_revs(#st{} = St, {OldFDI, NewFDI}, PurgedIdRevs) ->
    #st{
        id_tree = IdTree,
        seq_tree = SeqTree,
        purge_tree = PurgeTree
    } = St,
    {Add, RemIds, RemSeqs, NewUpdateSeq} = case {OldFDI, NewFDI} of
        {#full_doc_info{id = Id}, #full_doc_info{id = Id}} ->
            {
               [NewFDI],
               [],
               [OldFDI#full_doc_info.update_seq],
                NewFDI#full_doc_info.update_seq
            };
        {#full_doc_info{id = Id}, not_found} ->
            % We bump NewUpdateSeq because we have to ensure that
            % indexers see that they need to process the new purge
            % information.
            {
                [],
                [Id],
                [OldFDI#full_doc_info.update_seq],
                ?MODULE:get(St, update_seq) + 1
            }
    end,

    {ok, IdTree2} = couch_ngen_btree:add_remove(IdTree, Add, RemIds),
    {ok, SeqTree2} = couch_ngen_btree:add_remove(SeqTree, Add, RemSeqs),

    NewPurgeSeq = couch_ngen_header:get(St#st.header, purge_seq) + 1,
    AddReqs = [{NewPurgeSeq, PurgedIdRevs}],

    % check if oldest purge request need to be deleted
    % to keep only PurgeDocsLimit number of requests in purge_tree
    Limit = couch_ngen_header:purged_docs_limit(St#st.header),
    {ok, Count} = couch_ngen_btree:full_reduce(PurgeTree),
    RemPSeqs = if (Count < Limit) ->  []; true ->
        [?MODULE:get(St, oldest_purge_seq)]
    end,
    {ok, PurgeTree2} = couch_ngen_btree:add_remove(PurgeTree, AddReqs, RemPSeqs),

    Header2 = couch_ngen_header:set(St#st.header, [
        {update_seq, NewUpdateSeq},
        {purge_seq, NewPurgeSeq}
    ]),
    {ok, St#st{
        header = Header2,
        id_tree = IdTree2,
        seq_tree = SeqTree2,
        purge_tree = PurgeTree2,
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


fold_purged_docs(St, StartSeq0, UserFun, UserAcc, Options) ->
    StartSeq = StartSeq0 + 1,
    PurgeTree = St#st.purge_tree,
    {ok, _, MinSeq} = couch_ngen_btree:foldl(PurgeTree,
            fun({K, _V}, _) -> {stop, K} end, 0),
    if (MinSeq =< StartSeq) ->
        Fun = fun drop_reductions2/4,
        InAcc = {UserFun, UserAcc},
        Opts = [{start_key, StartSeq}] ++ Options,
        {ok, _, OutAcc} = couch_ngen_btree:fold(PurgeTree, Fun, InAcc, Opts),
        {_, FinalUserAcc} = OutAcc,
        {ok, FinalUserAcc};
    true ->
        throw({invalid_start_purge_seq, StartSeq0})
    end.


count_changes_since(St, SinceSeq) ->
    BTree = St#st.seq_tree,
    FoldFun = fun(_SeqStart, PartialReds, 0) ->
        {ok, couch_ngen_btree:final_reduce(BTree, PartialReds)}
    end,
    Opts = [{start_key, SinceSeq + 1}],
    {ok, Changes} = couch_ngen_btree:fold_reduce(BTree, FoldFun, 0, Opts),
    Changes.


start_compaction(St, DbName, Options, Parent) ->
    % recording purge_seq at the start of compaction
    NewHeader = couch_ngen_header:set(St#st.header, [
        {compact_purge_seq, couch_ngen_header:purge_seq(St#st.header)}
    ]),
    start_compaction_int(St#st{header=NewHeader}, DbName, Options, Parent).


finish_compaction(SrcSt, DbName, Options, DirPath) ->
    {ok, TgtSt1} = ?MODULE:init(DirPath, [compactor | Options]),
    SrcSeq = ?MODULE:get(SrcSt, update_seq),
    TgtSeq = ?MODULE:get(TgtSt1, update_seq),
    case SrcSeq == TgtSeq of
        true ->
            finish_compaction_int(SrcSt, TgtSt1);
        false ->
            % check if there were too many purge requests during compaction
            % We're being extra cautious here so that a bug in purge doesn't
            % give us a corrupted db after compaction
            OldestPurgeSeq = couch_ngen:get(SrcSt, oldest_purge_seq),
            CompactPurgeSeq = couch_ngen:get(SrcSt, compact_purge_seq),
            case (OldestPurgeSeq =< (CompactPurgeSeq + 1)) of
                true ->
                    couch_log:info("Compaction file still behind main file "
                            "(update seq=~p. compact update seq=~p). Retrying.",
                            [SrcSeq, TgtSeq]),
                    ok = decref(TgtSt1),
                    start_compaction_int(SrcSt, DbName, Options, self());
                false ->
                    % there were too may purge requests that overwrote CompactPurgeSeq
                    % restart compaction from the beginning
                    couch_log:info("Restarting compaction from beginning: "
                            "during compaction docs were purged beyond compact_purge_seq=~p).",
                            [CompactPurgeSeq]),
                    ok = decref(TgtSt1),
                    delete_compaction_files(DirPath),
                    start_compaction(SrcSt, DbName, Options, self())
            end
    end.

id_seq_tree_split(#full_doc_info{} = FDI, DataFd) ->
    #full_doc_info{
        id = Id,
        update_seq = Seq,
        deleted = Deleted,
        sizes = SizeInfo,
        rev_tree = Tree
    } = FDI,
    DiskTerm = {Id, Seq, ?b2i(Deleted), split_sizes(SizeInfo), disk_tree(Tree)},
    {ok, Ptr} = couch_ngen_file:append_term(DataFd, DiskTerm),
    {Id, Ptr};
id_seq_tree_split({Id, Ptr}, _DataFd) ->
    {Id, Ptr}.


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


purge_tree_reduce(reduce, IdRevs) ->
    length(IdRevs);
purge_tree_reduce(rereduce, Reds) ->
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

    Header1 = couch_ngen_header:upgrade(Header0),
    Header2 = set_default_security_object(DataFd, Header1, Options),
    Header = upgrade_purge_info(DataFd, Header2),

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

    PurgeTreeState = couch_ngen_header:purge_tree_state(Header),
    {ok, PurgeTree} = couch_ngen_btree:open(PurgeTreeState, IdxFd, [
        {reduce, fun ?MODULE:purge_tree_reduce/2}
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
        local_tree = LocalTree,
        compression = couch_compress:get_compression_method(),
        purge_tree = PurgeTree
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
        {local_tree_state, couch_ngen_btree:get_state(St#st.local_tree)},
        {purge_tree_state, couch_ngen_btree:get_state(St#st.purge_tree)}
    ]).


set_default_security_object(Fd, Header, Options) ->
    case couch_ngen_header:get(Header, security_ptr) of
        Pointer when is_tuple(Pointer) ->
            Header;
        _ ->
            Default = couch_util:get_value(default_security_object, Options),
            {ok, Ptr} = couch_ngen_file:append_term(Fd, Default),
            couch_ngen_header:set(Header, security_ptr, Ptr)
    end.


% This function is here, and not in couch_ngen_header
% because it requires modifying file contents
upgrade_purge_info(Fd, Header) ->
    case couch_ngen_header:get(Header, purge_tree_state) of
    nil ->
        Header;
    Ptr when is_tuple(Ptr) ->
        Header;
    Ptr when is_integer(Ptr)->
        % old PurgeDocs format - upgrade to purge_tree
        {ok, PurgedIdRevs} = couch_ngen_file:read_term(Fd, Ptr), %???
        PSeq = couch_ngen_header:purge_seq(Header),
        {NPSeq, AddPurgeReqs} = lists:foldl(fun(IdRevs, {Seq, Acc}) ->
            {Seq+1, [{Seq+1,IdRevs} | Acc]}
        end, {PSeq-1,[]}, PurgedIdRevs),
        {ok, PTree0} = couch_ngen_btree:open(nil, Fd, [
            {reduce, fun ?MODULE:purge_tree_reduce/2}
         ]),
        {ok, PTree} = couch_ngen_btree:add_remove(PTree0, AddPurgeReqs, []),
        PTreeState = couch_ngen_btree:get_state(PTree),
        couch_ngen:set(Header, [
            {purge_seq, NPSeq},
            {compact_purge_seq, NPSeq},
            {purge_tree_state, PTreeState}
        ])
    end.


delete_compaction_files(DirPath) ->
    RootDir = config:get("couchdb", "database_dir", "."),
    delete_compaction_files(RootDir, DirPath, []).


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


drop_reductions2(visit, {PurgeSeq, IdRevs}, _Reds, {UserFun, UserAcc}) ->
    {Go, NewUserAcc} = UserFun(PurgeSeq, IdRevs, UserAcc),
    {Go, {UserFun, NewUserAcc}};
drop_reductions2(_, _, _, Acc) ->
    {ok, Acc}.


fold_docs_reduce_to_count(Reds) ->
    RedFun = fun id_tree_reduce/2,
    FinalRed = couch_ngen_btree:final_reduce(RedFun, Reds),
    element(1, FinalRed).


start_compaction_int(St, DbName, Options, Parent) ->
    Args = [St, DbName, Options, Parent],
    Pid = spawn_link(couch_ngen_compactor, start, Args),
    {ok, St, Pid}.


finish_compaction_int(#st{} = OldSt, #st{} = NewSt1) ->
    #st{
        dirpath = DirPath,
        header = OldHdr,
        local_tree = OldLocal
    } = OldSt,
    #st{
        dirpath = DirPath,
        header = NewHdr,
        local_tree = NewLocal1
    } = NewSt1,


    % copy purge info to NewSt2 or throw an error
    PurgeSeq = couch_ngen_header:purge_seq(OldHdr),
    CompactPurgeSeq = couch_ngen_header:compact_purge_seq(OldHdr),
    PurgedDocsLimit = couch_ngen_header:purged_docs_limit(OldHdr),
    NewSt2 = if PurgeSeq == 0 -> NewSt1; true ->
        % copy purge tree
        PTreeLoadFun = fun(Value, _Offset, Acc) ->
            {ok, [Value | Acc]}
        end,
        {ok, _, PSeqIdRevs} = couch_ngen_btree:foldl(OldSt#st.purge_tree,
            PTreeLoadFun, []),
        {ok, NewPurgeTree} = couch_ngen_btree:add(NewSt1#st.purge_tree,
            lists:reverse(PSeqIdRevs)),

        % check if there were purge requests during compact
        case PurgeSeq == CompactPurgeSeq of
        true ->
            NewSt1#st{purge_tree = NewPurgeTree};
        false ->
            PurgeFoldFun = fun(_PurgeSeq, {Id, Revs}, {AccIds, AccIdRevs}) ->
                {ok, {[Id|AccIds], [{Id, Revs}|AccIdRevs]} }
            end,
            % collect purge requests since CompactPurgeSeq
            % fold_purged_docs throws an exception if
            % CompactPurgeSeq was pruned during compaction
            try
                {ok, {DocIds, IdRevs}} = fold_purged_docs(OldSt, CompactPurgeSeq,
                        PurgeFoldFun, {[], []}, []),
                OldDocInfos = open_docs(NewSt1, DocIds),

                % Since purging a document will change the update_seq,
                % finish_compaction will restart compaction in order to process
                % the new updates, which takes care of handling partially
                % purged documents.
                % collect only Ids and Seqs of docs that were completely purged
                {RemIds, RemSeqs} = lists:foldl(fun({OldFDI, {Id,Revs}},
                        {ARemIds, ARemSeqs}) ->
                    #full_doc_info{rev_tree=Tree, update_seq=UpSeq} = OldFDI,
                    case couch_key_tree:remove_leafs(Tree, Revs) of
                        {[]= _NewTree, _} ->
                           {[Id| ARemIds], [UpSeq| ARemSeqs]};
                        _ ->
                            {ARemIds, ARemSeqs}
                    end
                end, {[], []}, lists:zip(OldDocInfos, IdRevs)),

                % remove from NewSt docs that were completely purged
                case {RemIds, RemSeqs} of
                    {[], []} ->
                        NewSt1#st{purge_tree = NewPurgeTree};
                    _ ->
                        {ok, IdTree2} = couch_ngen_btree:add_remove(NewSt1#st.id_tree,
                                [], RemIds),
                        {ok, SeqTree2} = couch_ngen_btree:add_remove(NewSt1#st.seq_tree,
                                [], RemSeqs),
                        NewSt1#st{purge_tree = NewPurgeTree, id_tree = IdTree2,
                                seq_tree = SeqTree2}
                end
            catch
                throw:{invalid_start_purge_seq, CompactPurgeSeq} ->
                    throw({invalid_start_purge_seq, CompactPurgeSeq})
            end
        end
    end,

    % suck up all the local docs into memory and write them to the new db
    LoadFun = fun(Value, _Offset, Acc) ->
        {ok, [Value | Acc]}
    end,
    {ok, _, LocalDocs} = couch_ngen_btree:foldl(OldLocal, LoadFun, []),
    {ok, NewLocal3} = couch_ngen_btree:add(NewLocal1, LocalDocs),

    {ok, NewSt3} = commit_data(NewSt2#st{
        header = couch_ngen_header:set(NewHdr, [
            {compacted_seq, ?MODULE:get(OldSt, update_seq)},
            {revs_limit, ?MODULE:get(OldSt, revs_limit)},
            {purge_seq, PurgeSeq},
            {compact_purge_seq, PurgeSeq},
            {purged_docs_limit, PurgedDocsLimit}
        ]),
        local_tree = NewLocal3
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
    {ok, NewSt3, undefined}.


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

