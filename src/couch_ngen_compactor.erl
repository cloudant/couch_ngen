-module(couch_ngen_compactor).


-export([
    start/4
]).


-include_lib("couch/include/couch_db.hrl").
-include("couch_ngen.hrl").

-record(comp_header, {
    db_header,
    idsort_state
}).

-record(comp_st, {
    src_st,
    tgt_st,
    idsort_fd,
    idsort
}).

-record(cacc, {
    batch = [],

    curr_batch = 0,
    max_batch = 524288,

    since_commit = 0,
    commit_after = 5242880
}).

-record(copy_st, {
    id_tree,
    seq_tree,
    curr,
    rem_seqs,
    infos
}).


start(#st{} = St, DbName, Options, Parent) ->
    erlang:put(io_priority, {db_compact, DbName}),
    couch_log:debug("Compaction process spawned for db \"~s\"", [DbName]),

    {ok, CompSt} = init_compaction(St, Options),

    add_compact_task(CompSt, DbName),

    Stages = [
        fun copy_purge_info/1,
        fun copy_compact/1,
        fun sort_docids/1,
        fun copy_docids/1,
        fun final_commit/1,
        fun close_comp_st/1
    ],

    ok = lists:foldl(fun(Fun, CompStAcc) ->
        Fun(CompStAcc)
    end, CompSt, Stages),

    gen_server:cast(Parent, {compact_done, couch_ngen, St#st.dirpath}).


init_compaction(SrcSt, Options) ->
    #st{
        dirpath = DirPath,
        header = SrcHeader
    } = SrcSt,

    TgtHeader0 = couch_ngen_header:from(SrcHeader),

    CPPath = filename:join(DirPath, "COMMITS.compact"),
    CPFd = case couch_ngen_file:open(CPPath, Options) of
        {ok, Fd0} ->
            Fd0;
        {error, enoent} ->
            {ok, Fd0} = couch_ngen_file:open(CPPath, [create] ++ Options),
            Fd0;
        OpenCPError ->
            erlang:error(OpenCPError)
    end,

    {ok, CPFd, IdxFd, DataFd} =
        couch_ngen:open_idx_data_files(CPFd, DirPath, [compactor | Options]),

    IdxCompactPath = couch_ngen_file:path(IdxFd),
    IdSortPath = <<IdxCompactPath/binary, ".ids">>,
    IdSortFd = case couch_ngen_file:open(IdSortPath, Options) of
        {ok, Fd1} ->
            Fd1;
        {error, enoent} ->
            {ok, Fd1} = couch_ngen_file:open(IdSortPath, [create] ++ Options),
            Fd1;
        OpenSortError ->
            erlang:error(OpenSortError)
    end,

    {TgtHeader, IdSortSt} = case couch_ngen:read_header(CPFd, IdxFd) of
        #comp_header{} = Hdr->
            {Hdr#comp_header.db_header, Hdr#comp_header.idsort_state};
        no_valid_header ->
            ok = couch_ngen_file:truncate(IdxFd, 0),
            ok = couch_ngen_file:truncate(IdSortFd, 0),
            ok = couch_ngen_file:truncate(DataFd, 0),
            couch_ngen:write_header(CPFd, IdxFd, TgtHeader0),
            {TgtHeader0, undefined};
        Header0 ->
            true = couch_ngen_header:is_header(Header0),
            ok = couch_ngen_file:truncate(IdSortFd, 0),
            {Header0, undefined}
    end,

    TgtSt = couch_ngen:init_state(
            DirPath, CPFd, IdxFd, DataFd, TgtHeader, Options),

    IdSort = couch_ngen_emsort:open(IdSortFd, [{root, IdSortSt}]),

    {ok, #comp_st{
        src_st = SrcSt,
        tgt_st = TgtSt,
        idsort_fd = IdSortFd,
        idsort = IdSort
    }}.


copy_purge_info(CompSt) ->
    #comp_st{
        src_st = SrcSt,
        tgt_st = TgtSt
    } = CompSt,
    SrcHdr = SrcSt#st.header,
    TgtHdr = TgtSt#st.header,
    SrcPurgeSeq = couch_ngen_header:purge_seq(SrcHdr),
    NewTgtSt = case SrcPurgeSeq > 0 of
        true ->
            Purged = couch_ngen:get(SrcSt, last_purged),
            {ok, Ptr} = couch_ngen_file:append_term(TgtSt#st.data_fd, Purged),
            NewTgtHdr = couch_ngen_header:set(TgtHdr, [
                {purge_seq, SrcPurgeSeq},
                {purged_docs, Ptr}
            ]),
            TgtSt#st{header = NewTgtHdr};
        false ->
            TgtSt
    end,
    CompSt#comp_st{
        tgt_st = NewTgtSt
    }.


copy_compact(CompSt0) ->
    #comp_st{
        src_st = SrcSt0,
        tgt_st = TgtSt0
    } = CompSt0,

    CompUpdateSeq = couch_ngen:get(TgtSt0, update_seq),
    BufferSize = get_config_int("doc_buffer_size", 524288),
    CheckpointAfter = get_config_int("checkpoint_after", BufferSize * 10),

    {ok, _, {CompSt1, NewCAcc}} = couch_ngen_btree:foldl(
            SrcSt0#st.seq_tree,
            fun enum_by_seq_fun/3,
            #cacc{
                max_batch = BufferSize,
                commit_after = CheckpointAfter
            },
            [{start_key, CompUpdateSeq + 1}]
        ),

    #cacc{
        batch = Uncopied
    } = NewCAcc,

    CompSt2 = copy_docs(CompSt1, lists:reverse(Uncopied)),

    #comp_st{
        src_st = SrcSt1,
        tgt_st = TgtSt1
    } = CompSt2,

    % Copy the security information over
    {ok, TgtSt2} = case couch_ngen:get(SrcSt1, security) of
        [] ->
            couch_ngen:set(TgtSt1, security_ptr, nil);
        SecObj ->
            {ok, Ptr} = couch_ngen_file:append_term(TgtSt1#st.data_fd, SecObj),
            couch_ngen:set(TgtSt1, security_ptr, Ptr)
    end,

    FinalUpdateSeq = couch_ngen:get(SrcSt1, update_seq),
    {ok, TgtSt3} = couch_ngen:set(TgtSt2, update_seq, FinalUpdateSeq),

    commit_compaction_data(CompSt2#comp_st{tgt_st = TgtSt3}).


enum_by_seq_fun(FDI, _Offset, {CompSt0, CAcc}) ->
    #comp_st{
        tgt_st = TgtSt0
    } = CompSt0,

    Seq = FDI#full_doc_info.update_seq,
    NewBatch = [FDI | CAcc#cacc.batch],
    CurrBatchSize = CAcc#cacc.curr_batch + ?term_size(FDI),

    AccOut = case CurrBatchSize >= CAcc#cacc.max_batch of
        true ->
            CompSt1 = copy_docs(CompSt0, NewBatch),
            {ok, TgtSt1} = couch_ngen:set(TgtSt0, update_seq, Seq),
            CompSt2 = CompSt1#comp_st{tgt_st = TgtSt1},
            NewCAcc1 = CAcc#cacc{batch = [], curr_batch = 0},

            SinceCommit = CAcc#cacc.since_commit + CurrBatchSize,
            case SinceCommit >= CAcc#cacc.commit_after of
                true ->
                    NewCAcc2 = CAcc#cacc{since_commit = 0},
                    {commit_compaction_data(CompSt2), NewCAcc2};
                false ->
                    {CompSt2, NewCAcc1}
            end;
        false ->
            {CompSt0, CAcc#cacc{batch = NewBatch, curr_batch = CurrBatchSize}}
    end,
    {ok, AccOut}.


copy_docs(CompSt, FDIs) ->
    #comp_st{
        tgt_st = TgtSt,
        idsort = IdSort
    } = CompSt,

    % COUCHDB-968, make sure we prune duplicates during compaction
    NewInfos0 = lists:usort(fun(#full_doc_info{id=A}, #full_doc_info{id=B}) ->
        A =< B
    end, FDIs),

    Limit = couch_ngen:get(TgtSt, revs_limit),

    DocIdPtrs = lists:map(fun(Info) ->
        copy_doc(CompSt, Info, Limit)
    end, NewInfos0),

    % If compaction is being rerun to catch up to writes during
    % the first pass we may have docs that already exist
    % in the seq_tree. Here we lookup any old update_seqs so
    % that they can be removed.

    DocIds = [Id || {Id, _Ptr} <- DocIdPtrs],
    Existing = couch_btree:lookup(TgtSt#st.id_tree, DocIds),
    RemSeqs = [Seq || {ok, #full_doc_info{update_seq=Seq}} <- Existing],

    {ok, SeqTree} = couch_btree:add_remove(
            TgtSt#st.seq_tree, DocIdPtrs, RemSeqs),

    IdSortPtrs = lists:zipwith(fun(FDI, {DocId, Ptr}) ->
        {{DocId, FDI#full_doc_info.update_seq}, Ptr}
    end, NewInfos0, DocIdPtrs),

    {ok, NewIdSort} = couch_emsort:add(IdSort, IdSortPtrs),

    update_compact_task(length(DocIdPtrs)),

    CompSt#comp_st{
        tgt_st = TgtSt#st{
            seq_tree = SeqTree
        },
        idsort = NewIdSort
    }.


copy_doc(CompSt, FDI, Limit) ->
    #comp_st{
        tgt_st = TgtSt
    } = CompSt,

    InitAcc = {CompSt, {0, 0, []}},
    RevTree = FDI#full_doc_info.rev_tree,

    {NewRevTree, AccOut}
            = couch_key_tree:mapfold(fun copy_leaves/4, InitAcc, RevTree),

    {_, {FinalAS, FinalES, FinalAtts}} = AccOut,
    TotalAttSize = lists:foldl(fun({_, S}, A) -> S + A end, 0, FinalAtts),
    NewActiveSize = FinalAS + TotalAttSize,
    NewExternalSize = FinalES + TotalAttSize,

    NewFDI  = FDI#full_doc_info{
        rev_tree = couch_key_tree:stem(NewRevTree, Limit),
        sizes = #size_info{
            active = NewActiveSize,
            external = NewExternalSize
        }
    },

    couch_ngen:write_doc_info(TgtSt, NewFDI).


copy_leaves(_Rev, _Leaf, branch, Acc) ->
    {?REV_MISSING, Acc};

copy_leaves(_Rev, #leaf{}, Leaf, {CompSt, SizesAcc}) ->
    #comp_st{
        src_st = SrcSt,
        tgt_st = TgtSt
    } = CompSt,
    #leaf{
        ptr = Ptr
    } = Leaf,

    {Body, Atts} = copy_doc_attachments(SrcSt, TgtSt, Ptr),
    AttInfos = [couch_att:to_disk_term(A) || A <- Atts],
    DocBin = couch_bt_engine:make_doc_summary(TgtSt, {Body, AttInfos}),

    HashOpts = [{hash, sha256}],
    {ok, Ptr} = couch_ngen_file:write_bin(TgtSt#st.data_fd, DocBin, HashOpts),

    AttSizeFun = fun(Att) ->
        [{_, Sp}, Size] = couch_att:fetch([data, att_len], Att),
        {Sp, Size}
    end,

    NewLeaf = Leaf#leaf{
        ptr = Ptr,
        sizes = #size_info{
            active = couch_ngen_file:length(Ptr),
            external = ?term_size(Body)
        },
        atts = lists:map(AttSizeFun, Atts)
    },
    {NewLeaf, couch_db_updater:add_sizes(leaf, NewLeaf, SizesAcc)}.


copy_doc_attachments(SrcSt, TgtSt, DocPtr) ->
    {ok, {Body, AttInfos}} = couch_ngen:read_doc(SrcSt, DocPtr),

    StreamFun = fun(Sp) -> couch_ngen:open_read_stream(SrcSt, Sp) end,
    LoadFun = fun(Info) -> couch_att:from_disk_term(StreamFun, Info) end,
    Atts = lists:map(LoadFun, AttInfos),

    CopyFun = fun(Att) ->
        Dst = couch_ngen:open_write_stream(TgtSt, []),
        couch_att:copy(Att, Dst)
    end,
    CopiedAtts = lists:map(CopyFun, Atts),

    {Body, CopiedAtts}.


sort_docids(CompSt) ->
    #comp_st{
        idsort = IdSort
    } = CompSt,
    {ok, NewIdSort} = couch_emsort:merge(IdSort),

    % Need to fsync and write a header at this point

    CompSt#comp_st{
        idsort = NewIdSort
    }.


copy_docids(CompSt) ->
    #comp_st{
        tgt_st = TgtSt,
        idsort = IdSort
    } = CompSt,
    #st{
        id_tree = IdTree,
        seq_tree = SeqTree
    } = TgtSt,

    {ok, Iter} = couch_emsort:iter(IdSort),

    AccIn = #copy_st{
        id_tree = IdTree,
        seq_tree = SeqTree,
        rem_seqs = [],
        infos = []
    },

    AccOut = merge_docids(Iter, AccIn),

    #copy_st{
        id_tree = NewIdTree,
        seq_tree = NewSeqTree,
        rem_seqs = RemSeqsOut,
        infos = InfosOut
    } = AccOut,

    {ok, FinalIdTree} = couch_ngen_btree:add(NewIdTree, InfosOut),
    {ok, FinalSeqTree} = couch_btree:add_remove(NewSeqTree, [], RemSeqsOut),

    CompSt#comp_st{
        tgt_st = TgtSt#st{
            id_tree = FinalIdTree,
            seq_tree = FinalSeqTree
        }
    }.


merge_docids(Iter, #copy_st{infos = Infos} = Acc) when length(Infos) > 1000 ->
    #copy_st{
        id_tree = IdTree0,
        seq_tree = SeqTree0,
        rem_seqs = RemSeqs
    } = Acc,
    {ok, IdTree1} = couch_btree:add(IdTree0, Infos),
    {ok, SeqTree1} = couch_btree:add_remove(SeqTree0, [], RemSeqs),
    Acc1 = Acc#copy_st{
        id_tree = IdTree1,
        seq_tree = SeqTree1,
        rem_seqs = [],
        infos = []
    },
    merge_docids(Iter, Acc1);

merge_docids(Iter, #copy_st{curr = Curr} = Acc) ->
    case next_info(Iter, Curr, []) of
        {NextIter, NewCurr, DocIdPtr, Seqs} ->
            Acc1 = Acc#copy_st{
                infos = [DocIdPtr | Acc#copy_st.infos],
                rem_seqs = Seqs ++ Acc#copy_st.rem_seqs,
                curr = NewCurr
            },
            merge_docids(NextIter, Acc1);
        {finished, DocIdPtr, Seqs} ->
            Acc#copy_st{
                infos = [DocIdPtr | Acc#copy_st.infos],
                rem_seqs = Seqs ++ Acc#copy_st.rem_seqs,
                curr = undefined
            };
        empty ->
            Acc
    end.


next_info(Iter, undefined, []) ->
    case couch_emsort:next(Iter) of
        {ok, {{Id, Seq}, Ptr}, NextIter} ->
            next_info(NextIter, {Id, Seq, Ptr}, []);
        finished ->
            empty
    end;

next_info(Iter, {Id, Seq, Ptr}, Seqs) ->
    case couch_emsort:next(Iter) of
        {ok, {{Id, NSeq}, NPtr}, NextIter} ->
            next_info(NextIter, {Id, NSeq, NPtr}, [Seq | Seqs]);
        {ok, {{NId, NSeq}, NPtr}, NextIter} ->
            {NextIter, {NId, NSeq, NPtr}, {Id, Ptr}, Seqs};
        finished ->
            {finished, {Id, Ptr}, Seqs}
    end.


commit_compaction_data(CompSt) ->
    #comp_st{
        tgt_st = TgtSt,
        idsort = IdSort,
        idsort_fd = IdSortFd
    } = CompSt,

    Fds = [
        TgtSt#st.cp_fd,
        TgtSt#st.idx_fd,
        TgtSt#st.data_fd,
        IdSortFd
    ],

    lists:foreach(fun(Fd) ->
        couch_ngen_file:sync(Fd)
    end, Fds),

    TgtHeader = couch_ngen:update_header(TgtSt, TgtSt#st.header),
    IdSortSt = couch_emsort:get_state(IdSort),

    CompHeader = #comp_header{
        db_header = TgtHeader,
        idsort_state = IdSortSt
    },

    ok = couch_ngen:write_header(TgtSt#st.cp_fd, TgtSt#st.idx_fd, CompHeader),
    ok = couch_ngen_file:sync(TgtSt#st.idx_fd),
    ok = couch_ngen_file:sync(TgtSt#st.cp_fd),

    CompSt.


final_commit(CompSt) ->
    #comp_st{
        tgt_st = TgtSt
    } = CompSt,

    Fds = [
        TgtSt#st.cp_fd,
        TgtSt#st.idx_fd,
        TgtSt#st.data_fd
    ],

    lists:foreach(fun(Fd) ->
        couch_ngen_file:sync(Fd)
    end, Fds),

    TgtHeader = couch_ngen:update_header(TgtSt, TgtSt#st.header),

    ok = couch_ngen:write_header(TgtSt#st.cp_fd, TgtSt#st.idx_fd, TgtHeader),
    ok = couch_ngen_file:sync(TgtSt#st.idx_fd),
    ok = couch_ngen_file:sync(TgtSt#st.cp_fd),

    CompSt.


close_comp_st(CompSt) ->
    #comp_st{
        tgt_st = TgtSt,
        idsort_fd = IdSortFd
    } = CompSt,
    couch_ngen:decref(TgtSt),
    couch_ngen_file:close(IdSortFd),
    ok.


get_config_int(Key, Default) ->
    config:get_integer("database_compaction", Key, Default).


add_compact_task(CompSt, DbName) ->
    #comp_st{
        src_st = SrcSt,
        tgt_st = TgtSt
    } = CompSt,
    NewUpdateSeq = couch_ngen:get(TgtSt, update_seq),
    TotalChanges = couch_ngen:count_changes_since(SrcSt, NewUpdateSeq),
    TaskProps0 = [
        {type, database_compaction},
        {database, DbName},
        {progress, 0},
        {changes_done, 0},
        {total_changes, TotalChanges}
    ],
    IsRetry = couch_ngen_btree:get_state(TgtSt#st.id_tree) == nil,
    TaskExists = couch_task_status:is_task_added(),
    case IsRetry of
        true when TaskExists ->
            couch_task_status:update([
                {retry, true},
                {progress, 0},
                {changes_done, 0},
                {total_changes, TotalChanges}
            ]);
        true ->
            couch_task_status:add_task(TaskProps0 ++ [{retry, true}]);
        false ->
            couch_task_status:add_task(TaskProps0)
    end,
    couch_task_status:set_update_frequency(500).


update_compact_task(NumChanges) ->
    [Changes, Total] = couch_task_status:get([changes_done, total_changes]),
    Changes2 = Changes + NumChanges,
    Progress = if Total == 0 -> 0; true ->
        (Changes2 * 100) div Total
    end,
    couch_task_status:update([{changes_done, Changes2}, {progress, Progress}]).
