# zfs recv stuff

class ZfsMgmt::ZfsMgr::Recv < Thor
  extend ZfsMgmt::ZfsMgr::TransferOptions

  class_option :filter, :type => :string, :default => '.+',
               :desc => 'only act on zfs matching this regexp'
  desc "all", "receive all zfs configured via user properties"
  define_transfer_options
  def all()
    ZfsMgmt.set_log_level(options[:loglevel])
    ZfsMgmt.global_options = options
    ZfsMgmt.load_config
    lock = ZfsMgmt.lock(options)

    ZfsMgmt.zfs_recv_all(options)
    ZfsMgmt.unlock(lock)
  end

  desc "state [SUBCOMMAND ...ARGS]", "manage zfs send/recv state database"
  subcommand "state", ZfsMgmt::ZfsMgr::Send::State
end
