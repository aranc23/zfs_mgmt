# zfs send stuff

class ZfsMgmt::ZfsMgr::Send < Thor
  class_option :filter, :type => :string, :default => '.+',
               :desc => 'only act on zfs matching this regexp'
  desc "all", "send all zfs configured via user properties"
  method_option :remote, :type => :string,
                :desc => 'remote specification like root@otherhost or localhost'
  method_option :destination, :type => :string,
                :desc => 'destination path like otherpool/ourpool'
  method_option :verbose, :type => :string, :aliases => :'-v', :enum => ['send','receive','recv','mbuffer','pv'],
                :desc => 'enable verbose output on the specified element of the pipe'
  method_option :initial_snapshot, :type => :string, :enum => ['oldest','newest'], :default => 'oldest',
                :desc => 'when sending the initial snapshot use the oldest or most recent snapshot'

  method_option :intermediary, :aliases => :'-I', :desc => "pass -I option to zfs send", :type => :boolean
  method_option :backup, :aliases => :'-p', :desc => "pass -b (--backup) option to zfs send", :type => :boolean
  method_option :compressed, :aliases => :'-c', :desc => "pass -c (compressed) option to zfs send", :type => :boolean
  method_option :embed, :aliases => :'-e', :desc => "pass -e (--embed) option to zfs send", :type => :boolean
  method_option :holds, :aliases => :'-h', :desc => "pass the -h (--holds) option to zfs send", :type => :boolean
  method_option :large_block, :aliases => :'-L', :desc => "pass -L (--large-block) option to zfs send", :type => :boolean
  method_option :props, :aliases => :'-p', :desc => "pass -p (--props) option to zfs send", :type => :boolean
  method_option :raw, :aliases => :'-w', :desc => "pass -w (--raw) option to zfs send", :type => :boolean
  method_option :replicate, :aliases => :'-R', :desc => "pass -R (--replicate) option to zfs send", :type => :boolean

  method_option :noop, :aliases => :'-n', :desc => "pass -n (noop) option to zfs send", :type => :boolean
  method_option :unmount, :aliases => :'-u', :desc => "pass -u (unmount) option to zfs receive", :type => :boolean
  method_option :exclude, :aliases => :'-x', :desc => "passed to -x option of receive side", :type => :array
  method_option :option, :aliases => :'-o', :desc => "passed to -o option of receive side", :type => :array
  method_option :drop_holds, :desc => "pass the -h option to zfs recv, indicating holds should be ignored", :type => :boolean

  method_option :mbuffer, :desc => "insert mbuffer between send and recv", :default => true, :type => :boolean
  method_option :mbuffer_size, :desc => "passed to mbuffer -s option", :type => :string
  def all()
    ZfsMgmt.global_options = options

    [
      { 'zfsmgmt:send' => 'true' },
      # {
      #   'zfsmgmt:send' => 'replicate',
      #   'zfsmgmt:send@source' => 'local'
      # },
    ].each do |match|
      ZfsMgmt.zfs_managed_list(filter: options[:filter],
                               property_match: match).each do |zfs,props,snaps|
        if props['zfsmgmt:send@source'] == 'received'
          $logger.debug("skipping received filesystem: #{zfs}")
          next
        end
        if props.has_key?('zfsmgmt:send_replicate') and props['zfsmgmt:send_replicate'] == 'true' and props['zfsmgmt:send_replicate@source'] != 'local'
          $logger.debug("skipping descendant of replicated filesystems: #{zfs}")
          next
        end
        unless props['zfsmgmt:destination']
          $logger.error("#{zfs}: you must specify a destination zfs path via the user property zfsmgmt:destination, even if using --destination on the command line, skipping")
          next
        end
        ZfsMgmt.zfs_send(options,zfs,props,snaps)
      end
    end
  end
end
