# Shared Thor options for zfsmgr send all and zfsmgr recv all

module ZfsMgmt::ZfsMgr::TransferOptions
  def define_transfer_options
    method_option :verbose, :type => :string, :aliases => :'-v', :enum => ['send', 'receive', 'recv', 'mbuffer', 'mbuffer_send', 'mbuffer_recv', 'pv'],
                  :desc => 'enable verbose output on the specified element of the pipe (mbuffer enables the final mbuffer; mbuffer_send/mbuffer_recv target a specific side)'
    method_option :initial_snapshot, :type => :string, :enum => ['oldest', 'newest'], :default => 'oldest',
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
    method_option :force, :aliases => :'-F', :desc => "pass -F (force) option to zfs receive", :type => :boolean
    method_option :exclude, :aliases => :'-x', :desc => "passed to -x option of receive side (merged with config and zfsmgmt:excludes; prefix with ** to knock out)", :type => :string, repeatable: true
    method_option :option, :aliases => :'-o', :desc => "passed to -o option of receive side (merged with config and zfsmgmt:overrides; use key=** or ** to knock out)", :type => :string, repeatable: true
    method_option :drop_holds, :desc => "pass the -h option to zfs recv, indicating holds should be ignored", :type => :boolean
    method_option :discard_first, :desc => "pass the -d option to zfs recv, discarding the first element of the send snapshot's file system name", :type => :boolean
    method_option :recv_state, :aliases => :'-s', :desc => "pass the -s option to zfs recv to save partial receive state for resuming (defaults to true unless disabled)", :type => :boolean

    method_option :mbuffer, :desc => "insert mbuffer between send and recv (defaults to true unless disabled)", :type => :boolean
    method_option :mbuffer_size, :desc => "passed to mbuffer -m option", :type => :string
  end
end
