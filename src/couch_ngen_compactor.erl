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
    idsort,
    cp_path,
    idx_path,
    data_path,
    idsort_path
}).


-record(cacc, {
    batch = [],

    curr_batch = 0,
    max_batch = 524288,

    since_commit = 0,
    commit_after = 5242880
}).


-record(merge_st, {
    id_tree,
    seq_tree,
    curr,
    rem_seqs,
    infos
}).


start(#st{} = St, DbName, Options, Parent) ->
    erlang:put(io_priority, {db_compact, DbName}),
    couch_log:debug("Compaction process spawned for db \"~s\"", [DbName]),

    {ok, CmpSt} = init_compaction(St, Options),

    Stages = [
        fun copy_purge_info/1,
        fun copy_compact/1,
        fun sort_meta_data/1,
        fun commit_compaction_data/1,
        fun copy_meta_data/1,
        fun commit_data/1,
        fun close_cmp_st/1
    ],

    lists:foldl(fun(Fun, CmpStAcc) ->
        Fun(CmpStAcc)
    end, CmpSt, Stages),
    
    gen_server:cast(Parent, {compact_done, couch_ngen, DName}).


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
        Else ->
            erlang:error(Else)
    end,

    {IdxPath, DataPath} = couch_ngen:get_file_paths(CPFd, DirPath),

    CompactIdxPath = <<IdxPath/binary, ".compact">>,
    {ok, IdxFd} = couch_ngen_file:open(CompactIdxPath, Options),

    IdSortPath = <<IdxPath/binary, ".compact.ids">>,
    IdSortFd = case couch_ngen_file:open(IdSortPath, Options) of
        {ok, Fd1} ->
            Fd1;
        {error, enoent} ->
            {ok, Fd1} = couch_ngen_file:open(IdSortPath, [create] ++ Options),
            Fd1;
        Else ->
            erlang:error(Else)
    end,

    CompactDataPath = <<DataPath/binary, ".compact">>,
    {ok, DataFd} = couch_ngen_file:open(CompactDataPath, Options),

    {TgtHeader, IdSortSt} = case couch_ngen:read_header(CPFd, IdxFd) of
        #comp_header{} = Hdr->
            {Hdr#comp_header.db_header, Hdr#comp_header.idsort_state}
        no_valid_header ->
            ok = couch_ngen_file:truncate(IdxFd, 0),
            ok = couch_ngen_file:truncate(IdSortFd, 0),
            ok = couch_ngen_file:truncate(DataFd, 0),
            couch_ngen:write_header(CPFd, IdxFd, TgtHeader0),
            {NewHeader, undefined};
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
        idsort = IdSort,
        cp_path = CPPath,
        idx_path = IdxPath,
        data_path = DataPath,
        idsort_path = IdSortPath
    }}.


copy_purge_info(CmpSt) ->
    #comp_st{
        old_st = OldSt,
        new_st = NewSt
    } = CompSt,
    OldHdr = OldSt#st.header,
    NewHdr = NewSt#st.header,
    OldPurgeSeq = couch_ngen_header:purge_seq(OldHdr),
    case OldPurgeSeq > 0 of
        true ->
            Purged = couch_ngen:get(OldSt, last_purged),
            {ok, Ptr} = couch_ngen_file:append_term(NewSt#st.fd, Purged),
            NewNewHdr = couch_ngen_header:set(NewHdr, [
                {purge_seq, OldPurgeSeq},
                {purged_docs, Ptr}
            ]),
            NewSt#st{header = NewNewHdr};
        false ->
            NewSt
    end.


copy_compact(CompSt) ->
    #comp_st{
        old_st = OldSt,
        new_st = NewSt1
    } = CompSt,

    NewUpdateSeq = couch_ngen:get(NewSt1, update_seq),
    BufferSize = get_config_int("doc_buffer_size", 524288),
    CheckpointAfter = get_config_int("checkpoint_after", BufferSize * 10),

    {ok, _, {NewCompSt, NewCAcc}} = couch_ngen_btree:foldl(
            OldSt#st.seq_tree,
            fun enum_by_seq_fun/3,
            #cacc{
                max_batch = BufferSize,
                commit_after = CheckpointAfter
            },
            [{start_key, NewUpdateSeq + 1}]
        ),

    #comp_st{
        new_st = NewSt2
    } = NewCompSt,

    #cacc{
        batch = Uncopied
    } = NewCAcc,

    NewSt3 = copy_docs(OldSt, NewSt2, lists:reverse(Uncopied)),

    % Copy the security information over
    {ok, NewSt4} = case couch_ngen:get(St, security) of
        [] ->
            couch_ngen:set(NewSt3, security_ptr, nil);
        SecProps ->
            {ok, Ptr} = couch_file:append_term(NewSt3#st.data_fd, SecProps),
            couch_ngen:set(NewSt3, security_ptr, Ptr)
    end,

    FinalUpdateSeq = couch_ngen:get(OldSt, update_seq),
    {ok, NewSt5} = couch_ngen:set(NewSt4, update_seq, FinalUpdateSeq),
    commit_compaction_data(NewSt5).


enum_docs_fun(FDI, _Offset, {CompSt, CAcc}) ->
    #comp_st{
        old_st = OldSt,
        new_st = NewSt
    } = CompSt,

    Seq = FDI#full_doc_info.update_seq,
    NewBatch = [FDI | CAcc#cacc.batch],
    CurrBatchSize = CAcc#cacc.curr_batch + ?term_size(DocInfo),

    AccOut = case CurrBatchSize >= CAcc#cacc.max_batch of
        true ->
            NewSt1 = copy_docs(OldSt, NewSt, NewBatch),
            {ok, NewSt2} = couch_ngen:set(NewSt1, update_seq, Seq),
            NewCAcc1 = CAcc#cacc{batch = [], curr_batch = 0},

            SinceCommit = CAcc#cacc.since_commit + CurrBatchSize,
            case SinceCommit >= CAcc#cacc.commit_after of
                true ->
                    NewSt3 = commit_compaction_data(NewSt2),
                    NewCompSt = CompSt#comp_st{new_st = NewSt3},
                    NewCAcc2 = CAcc#cacc{since_commit = 0},
                    {NewCompSt, NewCAcc2};
                false ->
                    NewCompSt = CompSt#comp_st{new_st = NewSt2},
                    {NewCompSt, NewCAcc1}
            end;
        false ->
            {CompSt, CAcc#cacc{batch = NewBatch, curr_batch = CurrBatchSize}}
    end,
    {ok, AccOut}.


copy_docs(OldSt, NewSt, FDIs) ->
    % COUCHDB-968, make sure we prune duplicates during compaction
    NewInfos0 = lists:usort(fun(#full_doc_info{id=A}, #full_doc_info{id=B}) ->
        A =< B
    end, FDIs),

    NewInfos1 = lists:map(fun(Info) ->
        {NewRevTree, FinalAcc} = couch_key_tree:mapfold(fun
            (_Rev, #leaf{ptr=Sp}=Leaf, leaf, SizesAcc) ->
                {Body, AttInfos} = copy_doc_attachments(St, Sp, NewSt),
                SummaryChunk = couch_bt_engine:make_doc_summary(
                        NewSt, {Body, AttInfos}),
                ExternalSize = ?term_size(SummaryChunk),
                {ok, Pos, SummarySize} = couch_file:append_raw_chunk(
                    NewSt#st.fd, SummaryChunk),
                AttSizes = [{element(3,A), element(4,A)} || A <- AttInfos],
                NewLeaf = Leaf#leaf{
                    ptr = Pos,
                    sizes = #size_info{
                        active = SummarySize,
                        external = ExternalSize
                    },
                    atts = AttSizes
                },
                {NewLeaf, couch_db_updater:add_sizes(leaf, NewLeaf, SizesAcc)};
            (_Rev, _Leaf, branch, SizesAcc) ->
                {?REV_MISSING, SizesAcc}
        end, {0, 0, []}, Info#full_doc_info.rev_tree),
        {FinalAS, FinalES, FinalAtts} = FinalAcc,
        TotalAttSize = lists:foldl(fun({_, S}, A) -> S + A end, 0, FinalAtts),
        NewActiveSize = FinalAS + TotalAttSize,
        NewExternalSize = FinalES + TotalAttSize,
        Info#full_doc_info{
            rev_tree = NewRevTree,
            sizes = #size_info{
                active = NewActiveSize,
                external = NewExternalSize
            }
        }
    end, NewInfos0),

    Limit = couch_bt_engine:get(St, revs_limit),
    NewInfos = lists:map(fun(FDI) ->
        FDI#full_doc_info{
            rev_tree = couch_key_tree:stem(FDI#full_doc_info.rev_tree, Limit)
        }
    end, NewInfos1),

    RemoveSeqs =
    case Retry of
    nil ->
        [];
    OldDocIdTree ->
        % Compaction is being rerun to catch up to writes during the
        % first pass. This means we may have docs that already exist
        % in the seq_tree in the .data file. Here we lookup any old
        % update_seqs so that they can be removed.
        Ids = [Id || #full_doc_info{id=Id} <- NewInfos],
        Existing = couch_btree:lookup(OldDocIdTree, Ids),
        [Seq || {ok, #full_doc_info{update_seq=Seq}} <- Existing]
    end,

    {ok, SeqTree} = couch_btree:add_remove(
            NewSt#st.seq_tree, NewInfos, RemoveSeqs),

    FDIKVs = lists:map(fun(#full_doc_info{id=Id, update_seq=Seq}=FDI) ->
        {{Id, Seq}, FDI}
    end, NewInfos),
    {ok, IdEms} = couch_emsort:add(NewSt#st.id_tree, FDIKVs),
    update_compact_task(length(NewInfos)),
    NewSt#st{id_tree=IdEms, seq_tree=SeqTree}.


copy_doc_attachments(#st{} = SrcSt, SrcSp, DstSt) ->
    {ok, {BodyData, BinInfos0}} = couch_bt_engine:read_doc(SrcSt, SrcSp),
    BinInfos = case BinInfos0 of
    _ when is_binary(BinInfos0) ->
        couch_compress:decompress(BinInfos0);
    _ when is_list(BinInfos0) ->
        % pre 1.2 file format
        BinInfos0
    end,
    % copy the bin values
    NewBinInfos = lists:map(
        fun({Name, Type, BinSp, AttLen, RevPos, ExpectedMd5}) ->
            % 010 UPGRADE CODE
            SrcStream = couch_bt_engine:open_read_stream(SrcSt, BinSp),
            DstStream = couch_bt_engine:open_write_stream(DstSt, []),
            ok = couch_stream:copy(SrcStream, DstStream),
            {NewStream, AttLen, AttLen, ActualMd5, _IdentityMd5} =
                couch_stream:close(DstStream),
            NewBinSp = couch_stream:to_disk_term(NewStream),
            couch_util:check_md5(ExpectedMd5, ActualMd5),
            {Name, Type, NewBinSp, AttLen, AttLen, RevPos, ExpectedMd5, identity};
        ({Name, Type, BinSp, AttLen, DiskLen, RevPos, ExpectedMd5, Enc1}) ->
            SrcStream = couch_bt_engine:open_read_stream(SrcSt, BinSp),
            DstStream = couch_bt_engine:open_write_stream(DstSt, []),
            ok = couch_stream:copy(SrcStream, DstStream),
            {NewStream, AttLen, _, ActualMd5, _IdentityMd5} =
                couch_stream:close(DstStream),
            NewBinSp = couch_stream:to_disk_term(NewStream),
            couch_util:check_md5(ExpectedMd5, ActualMd5),
            Enc = case Enc1 of
            true ->
                % 0110 UPGRADE CODE
                gzip;
            false ->
                % 0110 UPGRADE CODE
                identity;
            _ ->
                Enc1
            end,
            {Name, Type, NewBinSp, AttLen, DiskLen, RevPos, ExpectedMd5, Enc}
        end, BinInfos),
    {BodyData, NewBinInfos}.


sort_meta_data(St0) ->
    {ok, Ems} = couch_emsort:merge(St0#st.id_tree),
    St0#st{id_tree=Ems}.


copy_meta_data(#st{} = St) ->
    #st{
        fd = Fd,
        header = Header,
        id_tree = Src
    } = St,
    DstState = couch_bt_engine_header:id_tree_state(Header),
    {ok, IdTree0} = couch_btree:open(DstState, Fd, [
        {split, fun couch_bt_engine:id_tree_split/1},
        {join, fun couch_bt_engine:id_tree_join/2},
        {reduce, fun couch_bt_engine:id_tree_reduce/2}
    ]),
    {ok, Iter} = couch_emsort:iter(Src),
    Acc0 = #merge_st{
        id_tree=IdTree0,
        seq_tree=St#st.seq_tree,
        rem_seqs=[],
        infos=[]
    },
    Acc = merge_docids(Iter, Acc0),
    {ok, IdTree} = couch_btree:add(Acc#merge_st.id_tree, Acc#merge_st.infos),
    {ok, SeqTree} = couch_btree:add_remove(
        Acc#merge_st.seq_tree, [], Acc#merge_st.rem_seqs
    ),
    St#st{id_tree=IdTree, seq_tree=SeqTree}.


open_compaction_file(FilePath, Opts) ->
    case couch_ngen_file:open(FilePath, Opts) of
        {error, enoent} ->
            couch_ngen_file:open(FilePath, [create] ++ Opts);
        Else ->
            Else
    end.


reset_compaction_file(Fd, Header) ->
    ok = couch_file:truncate(Fd, 0),
    ok = couch_file:write_header(Fd, Header).


commit_compaction_data(#st{}=St) ->
    % Compaction needs to write headers to both the data file
    % and the meta file so if we need to restart we can pick
    % back up from where we left off.
    commit_compaction_data(St, couch_emsort:get_fd(St#st.id_tree)),
    commit_compaction_data(St, St#st.fd).


commit_compaction_data(#st{header = OldHeader} = St0, Fd) ->
    DataState = couch_bt_engine_header:id_tree_state(OldHeader),
    MetaFd = couch_emsort:get_fd(St0#st.id_tree),
    MetaState = couch_emsort:get_state(St0#st.id_tree),
    St1 = bind_id_tree(St0, St0#st.fd, DataState),
    Header = St1#st.header,
    CompHeader = #comp_header{
        db_header = Header,
        meta_state = MetaState
    },
    ok = couch_file:sync(Fd),
    ok = couch_file:write_header(Fd, CompHeader),
    St2 = St1#st{
        header = Header
    },
    bind_emsort(St2, MetaFd, MetaState).


bind_emsort(St, Fd, nil) ->
    {ok, Ems} = couch_emsort:open(Fd),
    St#st{id_tree=Ems};
bind_emsort(St, Fd, State) ->
    {ok, Ems} = couch_emsort:open(Fd, [{root, State}]),
    St#st{id_tree=Ems}.


bind_id_tree(St, Fd, State) ->
    {ok, IdBtree} = couch_btree:open(State, Fd, [
        {split, fun couch_bt_engine:id_tree_split/1},
        {join, fun couch_bt_engine:id_tree_join/2},
        {reduce, fun couch_bt_engine:id_tree_reduce/2}
    ]),
    St#st{id_tree=IdBtree}.


merge_docids(Iter, #merge_st{infos=Infos}=Acc) when length(Infos) > 1000 ->
    #merge_st{
        id_tree=IdTree0,
        seq_tree=SeqTree0,
        rem_seqs=RemSeqs
    } = Acc,
    {ok, IdTree1} = couch_btree:add(IdTree0, Infos),
    {ok, SeqTree1} = couch_btree:add_remove(SeqTree0, [], RemSeqs),
    Acc1 = Acc#merge_st{
        id_tree=IdTree1,
        seq_tree=SeqTree1,
        rem_seqs=[],
        infos=[]
    },
    merge_docids(Iter, Acc1);
merge_docids(Iter, #merge_st{curr=Curr}=Acc) ->
    case next_info(Iter, Curr, []) of
        {NextIter, NewCurr, FDI, Seqs} ->
            Acc1 = Acc#merge_st{
                infos = [FDI | Acc#merge_st.infos],
                rem_seqs = Seqs ++ Acc#merge_st.rem_seqs,
                curr = NewCurr
            },
            merge_docids(NextIter, Acc1);
        {finished, FDI, Seqs} ->
            Acc#merge_st{
                infos = [FDI | Acc#merge_st.infos],
                rem_seqs = Seqs ++ Acc#merge_st.rem_seqs,
                curr = undefined
            };
        empty ->
            Acc
    end.


next_info(Iter, undefined, []) ->
    case couch_emsort:next(Iter) of
        {ok, {{Id, Seq}, FDI}, NextIter} ->
            next_info(NextIter, {Id, Seq, FDI}, []);
        finished ->
            empty
    end;
next_info(Iter, {Id, Seq, FDI}, Seqs) ->
    case couch_emsort:next(Iter) of
        {ok, {{Id, NSeq}, NFDI}, NextIter} ->
            next_info(NextIter, {Id, NSeq, NFDI}, [Seq | Seqs]);
        {ok, {{NId, NSeq}, NFDI}, NextIter} ->
            {NextIter, {NId, NSeq, NFDI}, FDI, Seqs};
        finished ->
            {finished, FDI, Seqs}
    end.


get_config_int(Key, Default) ->
    config:get_integer("database_compaction", Key, Default).


add_compact_task(CompSt) ->
    #comp_st{
        old_st = OldSt,
        new_st = NewSt
    } = CompSt,
    NewUpdateSeq = couch_ngen:get(NewSt, update_seq),
    TotalChanges = couch_ngen:count_changes_since(OldSt, NewUpdateSeq),
    TaskProps0 = [
        {type, database_compaction},
        {database, DbName},
        {progress, 0},
        {changes_done, 0},
        {total_changes, TotalChanges}
    ],
    IsRetry = couch_ngen_btree:get_state(NewSt#st.id_tree) == nil,
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
    Progress = case Total of
    0 ->
        0;
    _ ->
        (Changes2 * 100) div Total
    end,
    couch_task_status:update([{changes_done, Changes2}, {progress, Progress}]).

