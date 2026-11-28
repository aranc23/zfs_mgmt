# zfs send stuff

class ZfsMgmt::ZfsMgr::Send < Thor
  extend ZfsMgmt::ZfsMgr::TransferOptions

  class_option :filter, :type => :string, :default => '.+',
               :desc => 'only act on zfs matching this regexp'
  desc "all", "send all zfs configured via user properties"
  method_option :remote, :type => :string,
                :desc => 'DEPRECATED: remote specification like root@otherhost or localhost (specify in destination property or config instead)'
  define_transfer_options
  def all()
    ZfsMgmt.set_log_level(options[:loglevel])
    ZfsMgmt.global_options = options
    ZfsMgmt.load_config
    lock = ZfsMgmt.lock(options)

    ZfsMgmt.zfs_send_all(options)
    ZfsMgmt.unlock(lock)
  end

  class State < Thor
    class_option :filter, :type => :string, :default => '.+',
                 :desc => 'only act on zfs matching this regexp'
    default_command :show

    desc "show", "print a table of the values in the state db"
    method_option :check, :type => :boolean, :default => false,
                  :desc => 'print a check_mk local style check output, one line for each entry in the db'
    def show()
      ZfsMgmt.set_log_level(options[:loglevel])
      ZfsMgmt.global_options = options
      ZfsMgmt.send_state_show(check: options[:check], filter: options[:filter])
    end

    desc "delete SOURCE [DESTINATION]", "delete a key (or all entries for a source) from the state db"
    def delete(zfs, destination = nil)
      ZfsMgmt.set_log_level(options[:loglevel])
      ZfsMgmt.global_options = options
      if destination
        ZfsMgmt.send_state_delete(zfs, destination)
      else
        ZfsMgmt.send_state_delete(zfs)
      end
    end

    desc "wipe", "remove all keys in state db"
    def wipe()
      ZfsMgmt.set_log_level(options[:loglevel])
      ZfsMgmt.global_options = options
      ZfsMgmt.send_state_wipe
    end
  end

  desc "state [SUBCOMMAND ...ARGS]", "manage zfs send/recv state database"
  subcommand "state", State
end
