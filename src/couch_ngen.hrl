

-record(st, {
    dirpath,
    cp_fd,
    idx_fd,
    data_fd,
    fd_monitors,
    fsync_options,
    header,
    needs_commit,
    id_tree,
    seq_tree,
    local_tree,
    compression
}).

