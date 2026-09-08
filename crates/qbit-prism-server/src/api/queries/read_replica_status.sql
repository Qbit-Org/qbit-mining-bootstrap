SELECT jsonb_build_object(
    'in_recovery',pg_is_in_recovery(),
    'replay_lag_seconds',extract(epoch FROM(clock_timestamp()-pg_last_xact_replay_timestamp())),
    'receiver_heartbeat_age_seconds',extract(epoch FROM(clock_timestamp()-(SELECT last_msg_receipt_time FROM pg_stat_wal_receiver))),
    'apply_backlog_bytes',pg_wal_lsn_diff(pg_last_wal_receive_lsn(),pg_last_wal_replay_lsn())
);
