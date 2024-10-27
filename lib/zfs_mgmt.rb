# coding: utf-8
require "zfs_mgmt/version"
require "zfs_mgmt/restic"
require "zfs_mgmt/zfs_mgr"
require 'pp'
require 'date'
require 'logger'
require 'text-table'
require 'open3'
require 'filesize'
require 'timeout'

$logger = Logger.new(STDERR, progname: 'zfs_mgmt')

$date_patterns = {
  'hourly' => '%F Hour %H',
  'daily' => '%F',
  'weekly' => '%Y Week %U', # week, starting on sunday
  'monthly' => '%Y-%m',
  'yearly' => '%Y',
}

$time_pattern_map = {}
$date_patterns.keys.each do |tf|
  $time_pattern_map[tf[0]] = tf
end

$time_specs = {
  's' => 1,
  'm' => 60,
  'h' => 60*60,
  'd' => 24*60*60,
  'w' => 7*24*60*60,
}

$properties_xlate = {
  'userrefs' => ->(x) { x.to_i },
  'creation' => ->(x) { x.to_i },
}

$lock = nil

module ZfsMgmt
  class << self
    attr_accessor :global_options
  end
  class ZfsGetError < StandardError
  end
  def self.custom_properties()
    return [
      'policy',
      'manage',
      'strategy',
      'minage',
      'matchsnaps',
      'ignoresnaps',
      'prefersnaps',
      'snapshot',
      'snap_prefix',
      'snap_timestamp',
      'send',
      'remote',
      'destination',
    ].map do |p|
      ['zfsmgmt',p].join(':')
    end
  end
  def self.timespec_to_seconds(spec)
    md = /^(\d+)([smhdw]?)/i.match(spec)
    unless md.length == 3
      raise 'SpecParseError'
    end
    if md[2] and md[2].length > 0
      return md[1].to_i * $time_specs[md[2].downcase]
    else
      return md[1].to_i
    end
  end

  def self.zfs_holds(snapshot)
    com = [global_options['zfs_binary'], 'holds', '-H', snapshot]
    $logger.debug("#{com.join(' ')}")
    out = %x(#{com.join(' ')})
    unless $?.success?
      errstr = "unable to retrieves holds for snapshot: #{snapshot}"
      $logger.error(errstr)
      raise errstr
    end
    a = []
    out.split("\n").each do |ln|
      a.push(ln.split("\t")[1])
    end
    a
  end

  def self.zfs_hold(hold,snapshot)
    com = [global_options['zfs_binary'], 'hold', hold, snapshot]
    system_com(com)
    unless $?.success?
      errstr = "unable to set hold: #{hold} for snapshot: #{snapshot}"
      $logger.error(errstr)
      raise errstr
    end
  end

  def self.zfs_release(hold,snapshot)
    com = [@global_options['zfs_binary'], 'release', hold, snapshot]
    system_com(com)
    unless $?.success?
      errstr = "unable to release hold: #{hold} for snapshot: #{snapshot}"
      $logger.error(errstr)
      raise errstr
    end
  end

  def self.zfsget(properties: ['all'],types: ['filesystem','volume'],zfs: '', command_prefix: [])
    results={}
    com = [ZfsMgmt.global_options[:zfs_binary], 'get', '-Hp', properties.join(','), '-t', types.join(','), zfs]
    $logger.debug((command_prefix+com).join(' '))
    so,se,status = Open3.capture3((command_prefix+com).join(' '))
    if status.signaled?
      $logger.error("process was signalled \"#{com.join(' ')}\", termsig #{status.termsig}")
      raise ZfsGetError, "process was signalled \"#{com.join(' ')}\", termsig #{status.termsig}"
    end
    unless status.success?
      $logger.error("failed to execute \"#{com.join(' ')}\", exit status #{status.exitstatus}")
      so.split("\n").each { |l| $logger.debug("stdout: #{l}") }
      se.split("\n").each { |l| $logger.error("stderr: #{l}") }
      raise ZfsGetError, "failed to execute \"#{com.join(' ')}\", exit status #{status.exitstatus}"
    end
    so.split("\n").each do |line|
      params = line.split("\t")
      unless results.has_key?(params[0])
        results[params[0]] = {}
      end
      if params[2] != '-'
        if $properties_xlate.has_key?(params[1])
          results[params[0]][params[1]] = $properties_xlate[params[1]].call(params[2])
        else
          results[params[0]][params[1]] = params[2]
        end
      end
      if params[3] != '-'
        results[params[0]]["#{params[1]}@source"] = params[3]
      end
    end
    return results
  end
  def self.local_epoch_to_datetime(e)
    return Time.at(e).to_datetime
  end
  def self.find_saved_reason(saved,snap)
    results = {}
    $date_patterns.each do |d,dk|
      if saved.has_key?(d)
        saved[d].each do |k,s|
          if snap == s
            results[d]=k
            break
          end
        end
      end
    end
    return [results['hourly'],results['daily'],results['weekly'],results['monthly'],results['yearly']]
  end
  def self.snapshot_destroy_policy(zfs,props,snaps)
    minage = 0
    if props.has_key?('zfsmgmt:minage')
      minage = timespec_to_seconds(props['zfsmgmt:minage'])
    end
    strategy = 'oldest'
    if props.has_key?('zfsmgmt:strategy') and props['zfsmgmt:strategy'] == 'youngest'
      strategy = 'youngest'
    end
    sorted = snaps.keys.sort { |a,b| snaps[b]['creation'] <=> snaps[a]['creation'] }
    
    counters = policy_parser(props['zfsmgmt:policy'])
    $logger.debug(counters)
    saved = {}

    # set the counters variable to track the number of saved daily/hourly/etc. snapshots
    $date_patterns.each do |d,p|
      saved[d] = {}
    end

    sorted.each do |snap_name|
      if  props.has_key?('zfsmgmt:ignoresnaps') and /#{props['zfsmgmt:ignoresnaps']}/ =~ snap_name.split('@')[1]
        $logger.debug("skipping #{snap_name} because it matches ignoresnaps pattern: #{props['zfsmgmt:ignoresnaps']}")
        next
      end
      if  props.has_key?('zfsmgmt:matchsnaps') and not /#{props['zfsmgmt:matchsnaps']}/ =~ snap_name.split('@')[1]
        $logger.debug("skipping #{snap_name} because it does not match matchsnaps pattern: #{props['zfsmgmt:matchsnaps']}")
        next
      end
      snaptime = local_epoch_to_datetime(snaps[snap_name]['creation'])
      $date_patterns.each do |d,p|
        pat = snaptime.strftime(p)
        if saved[d].has_key?(pat)
          #pp props['zfsmgmt:prefersnaps'],snap_name.split('@')[1], saved[d][pat].split('@')[1]
          if props.has_key?('zfsmgmt:prefersnaps') and /#{props['zfsmgmt:prefersnaps']}/ !~ saved[d][pat].split('@')[1] and /#{props['zfsmgmt:prefersnaps']}/ =~ snap_name.split('@')[1]
            $logger.debug("updating the saved snapshot, we prefer this one: \"#{pat}\" to #{snap_name} at #{snaptime}")
            saved[d][pat] = snap_name
          elsif strategy == 'oldest' and ( not props.has_key?('zfsmgmt:prefersnaps') or /#{props['zfsmgmt:prefersnaps']}/ =~ snap_name.split('@')[1] )
            # update the existing current save snapshot for this timeframe
            $logger.debug("updating the saved snapshot for \"#{pat}\" to #{snap_name} at #{snaptime}")
            saved[d][pat] = snap_name
          else
            $logger.debug("not updating the saved snapshot for \"#{pat}\" to #{snap_name} at #{snaptime}, we have a younger snap")
          end
        elsif counters[d] > 0
          # new pattern, and we want to save more snaps of this type
          $logger.debug("new pattern \"#{pat}\" n#{counters[d]} #{d} snapshot, saving #{snap_name} at #{snaptime}")
          counters[d] -= 1
          saved[d][pat] = snap_name
        end
      end
    end
    
    # create a list of unique saved snap shots
    saved_snaps = []
    saved.each do |d,saved|
      saved_snaps += saved.values()
    end
    saved_snaps = saved_snaps.sort.uniq
    
    # delete everything not in the list of saved snapshots
    deleteme = sorted - saved_snaps
    deleteme = deleteme.select { |snap|
      if props.has_key?('zfsmgmt:ignoresnaps') and /#{props['zfsmgmt:ignoresnaps']}/ =~ snap.split('@')[1]
        $logger.debug("skipping #{snap} because it matches ignoresnaps pattern: #{props['zfsmgmt:ignoresnaps']}")
        false
      elsif minage > 0 and Time.at(snaps[snap]['creation'] + minage) > Time.now()
        $logger.debug("skipping due to minage: #{snap} #{local_epoch_to_datetime(snaps[snap]['creation']).strftime('%F %T')}")
        false
      elsif snap == sorted[0] # the very newest snap
        $logger.debug("skipping due to newest: #{snap} #{local_epoch_to_datetime(snaps[snap]['creation']).strftime('%F %T')}")
        false
      else
        true
      end
    }
    return saved,saved_snaps,deleteme
  end
  def self.zfs_managed_list(filter: '.+', properties: ['all'], property_match: { 'zfsmgmt:manage' => method(:prop_on?) } )
    zfss = [] # array of arrays
    zfsget(properties: properties).each do |zfs,props|
      unless /#{filter}/ =~ zfs
        next
      end
      managed = true
      property_match.each do |k,v|
        unless key_comp?(props,k,v)
          managed = false
          break
        end
      end
      next unless managed
      snaps = self.zfsget(properties: ['name','creation','userrefs','used','written','referenced'],types: ['snapshot'], zfs: zfs)
      if snaps.length == 0
        $logger.warn("unable to process this zfs, no snapshots at all: #{zfs}")
        next
      end
      zfss.push([zfs,props,snaps])
    end
    return zfss
  end
  def self.snapshot_policy(filter: '.+')
    zfs_managed_list(filter: filter).each do |zfs,props,snaps|
      unless props.has_key?('zfsmgmt:policy')
        $logger.error("zfs_mgmt is configured to manage #{zfs}, but there is no policy configuration in zfsmgmt:policy, skipping")
        next # zfs
      end

      begin
        # call the function that decides who to save and who to delete
        (saved,saved_snaps,deleteme) = snapshot_destroy_policy(zfs,props,snaps)
      rescue ArgumentError
        $logger.error("zfs_mgmt is configured to manage #{zfs}, but there is no valid policy configuration, skipping")
        next
      end

      if saved_snaps.length == 0
        $logger.info("no snapshots marked as saved by policy for #{zfs}")
        next
      end
      # print a table of saved snapshots with the reasons it is being saved
      table = Text::Table.new
      table.head = [zfs,'creation','hourly','daily','weekly','monthly','yearly']
      table.rows = []
      saved_snaps.sort { |a,b| snaps[b]['creation'] <=> snaps[a]['creation'] }.each do |snap|
        table.rows << [snap.split('@')[1],local_epoch_to_datetime(snaps[snap]['creation'])] + find_saved_reason(saved,snap)
      end
      print table.to_s
    end
  end
  def self.snapshot_destroy(noop: false, verbose: false, filter: '.+')
    unless lock(options)
      exit(1)
    end
    zfs_managed_list(filter: filter).each do |zfs,props,snaps|
      unless props.has_key?('zfsmgmt:policy')
        $logger.error("zfs_mgmt is configured to manage #{zfs}, but there is no policy configuration in zfsmgmt:policy, skipping")
        next # zfs
      end

      begin
        # call the function that decides who to save and who to delete
        (saved,saved_snaps,deleteme) = snapshot_destroy_policy(zfs,props,snaps)
      rescue ArgumentError
        $logger.error("zfs_mgmt is configured to manage #{zfs}, but there is no valid policy configuration, skipping")
        next
      end
    
      $logger.info("deleting #{deleteme.length} snapshots for #{zfs}")
      deleteme.reverse! # oldest first for removal
      deleteme.each do |snap_name|
        $logger.debug("delete: #{snap_name} #{local_epoch_to_datetime(snaps[snap_name]['creation']).strftime('%F %T')}")
      end

      com_base = [ZfsMgmt.global_options[:zfs_binary], 'destroy']
      com_base.push('-d') if deleteme.length > 0 # why?
      com_base.push('-n') if noop
      com_base.push('-v') if verbose
      while deleteme.length > 0
        for i in 0..(deleteme.length - 1) do
          max = deleteme.length - 1 - i
          $logger.debug("attempting to remove snaps 0 through #{max} out of #{deleteme.length} snapshots")
          bigarg = "#{zfs}@#{deleteme[0..max].map { |s| s.split('@')[1] }.join(',')}"
          com = com_base + [bigarg]
          $logger.debug("size of bigarg: #{bigarg.length} size of com: #{com.length}")
          if bigarg.length >= 131072 or com.length >= (2097152-10000)
            next
          end
          deleteme = deleteme - deleteme[0..max]
          system_com(com) # pass -n, always run the command though
          break
        end
      end
    end
    unlock()  
  end
  # parse a policy string into a hash of integers
  def self.policy_parser(str)
    res = {}
    $date_patterns.keys.each do |tf|
      res[tf]=0
    end
    p = str.scan(/\d+[#{$time_pattern_map.keys.join('')}]/i)
    unless p.length > 0
      raise ArgumentError.new("unable to parse the policy configuration #{str}")
    end
    p.each do |pi|
      scn = /(\d+)([#{$time_pattern_map.keys.join('')}])/i.match(pi)
      res[$time_pattern_map[scn[2].downcase]] = scn[1].to_i
    end
    res
  end
  # snapshot all filesystems configured for snapshotting
  def self.snapshot_create(noop: false, filter: '.+')
    unless lock(options)
      exit(1)
    end

    dt = DateTime.now
    zfsget.select { |zfs,props|
      # must match filter
      match_filter?(zfs, filter) and
        # snapshot must be on or true
        (
          key_comp?(props,'zfsmgmt:snapshot') or
          # or snapshot can be recursive and local, but only if the source is local or received
          ( key_comp?(props,'zfsmgmt:snapshot',['recursive','local']) and key_comp?(props,'zfsmgmt:snapshot@source',['local','received']) )
        )
    }.each do |zfs,props|
      prefix = ( props.has_key?('zfsmgmt:snap_prefix') ? props['zfsmgmt:snap_prefix'] : 'zfsmgmt' )
      ts = ( props.has_key?('zfsmgmt:snap_timestamp') ? props['zfsmgmt:snap_timestamp'] : '%FT%T%z' )
      com = [global_options['zfs_binary'],'snapshot']
      if key_comp?(props,'zfsmgmt:snapshot','recursive') and key_comp?(props,'zfsmgmt:snapshot@source',['local','received'])
        com.push('-r')
      end
      com.push("#{zfs}@#{[prefix,dt.strftime(ts)].join('-')}")
      system_com(com,noop)
    end
    unlock()
  end
  def self.system_com(com, noop = false)
    comstr = com.join(' ')
    $logger.info(comstr)
    unless noop
      system(comstr)
      unless $?.success?
        $logger.error("command failed: #{$?.exitstatus}")
      end
    end
  end
  def self.zfs_send(options,zfs,props,snaps)
    sorted = snaps.keys.sort { |a,b| snaps[a]['creation'] <=> snaps[b]['creation'] }
    # compute the zfs "path"
    # ternary operator 4eva
    destination_path = ( options[:destination] ? options[:destination] : props['zfsmgmt:destination'] )
    if props['zfsmgmt:destination@source'] == 'local'
      destination_path = File.join( destination_path,
                                    File.basename(zfs)
                                  )
    elsif m = /inherited from (.+)/.match(props['zfsmgmt:destination@source'])
      destination_path = File.join( destination_path,
                                    File.basename(m[1]),
                                    zfs.sub(m[1],'')
                                  )
    else
      $logger.error("fatal error: #{props['zfsmgmt:destination']} source: #{props['zfsmgmt:destination@source']}")
      exit(1)
    end
    # does the destination zfs already exist?
    remote_zfs_state = ''
    begin
      recv_zfs = zfsget(zfs: destination_path,
                        command_prefix: recv_command_prefix(options,props),
                        #properties: ['receive_resume_token'],
                       )
    rescue ZfsGetError
      $logger.debug("recv filesystem doesn't exist: #{destination_path}")
      remote_zfs_state = 'missing'
    else
      if recv_zfs[destination_path].has_key?('receive_resume_token')
        remote_zfs_state = recv_zfs[destination_path]['receive_resume_token']
      else
        remote_zfs_state = 'present'
      end
    end

    if remote_zfs_state == 'missing'
      # the zfs does not exist, send initial (oldest?) snapshot
      com = []
      source = sorted[0]
      if options[:initial_snapshot] == 'newest' or
        key_comp?(options, 'replicate') or
        key_comp?(props, 'zfsmgmt:send_replicate')
        source = sorted[-1]
      end
      com += zfs_send_com(options,
                          props,
                          [],
                          source,
                         )
      e = zfs_send_estimate(com) if options[:verbose] == 'pv'
      com += mbuffer_command(options) if options[:mbuffer]
      com += pv_command(options,e) if options[:verbose] == 'pv'
      com += zfs_recv_com(options,[],props,destination_path)
 
      system_com(com)
      unless $?.success?
        return
      end

    elsif remote_zfs_state != 'present'
      # should be resumable!
      com = [ ]
      com.push( ZfsMgmt.global_options[:zfs_binary], 'send', '-t', remote_zfs_state )
      com.push('-v','-P') if key_comp?(options, 'verbose', 'send')
      com.push('|')
      e = zfs_send_estimate(com) if options[:verbose] == 'pv'
      com += mbuffer_command(options) if options[:mbuffer]
      com += pv_command(options,e) if options[:verbose] == 'pv'

      recv = [ ZfsMgmt.global_options[:zfs_binary], 'recv', '-s' ]
      recv.push('-n') if options[:noop]
      recv.push('-u') if options[:unmount]
      recv.push('-v') if options[:verbose] and ( options[:verbose] == 'receive' or options[:verbose] == 'recv' )
      recv.push(dq(destination_path))

      if options[:remote] or props['zfsmgmt:remote']
        if options[:mbuffer]
          recv = mbuffer_command(options) + recv
        end
        recv = recv_command_prefix(options,props) + [ sq(recv.join(' ')) ]
      end

      com += recv

      system_com(com)
      unless $?.success?
        return
      end
    end
    
    # the zfs already exists, so update with incremental?
    begin
      remote_snaps = zfsget(zfs: destination_path,
                            types: ['snapshot'],
                            command_prefix: recv_command_prefix(options,props),
                            properties: ['creation','userrefs'],
                           )
    rescue ZfsGetError
      $logger.error("unable to get remote snapshot information for #{destination_path}")
      return
    end
    unless remote_snaps and remote_snaps.keys.length > 0
      $logger.error("receiving filesystem has NO snapshots, it must be destroyed: #{destination_path}")
      return
    end
    if remote_snaps.has_key?(sorted[-1].sub(zfs,destination_path))
      $logger.info("the most recent local snapshot (#{sorted[-1]}) already exists on the remote side (#{sorted[-1].sub(zfs,destination_path)})")
      return
    end
    remote_snaps.sort_by { |k,v| -v['creation'] }.each do |rsnap,v|
      # oldest first
      #pp rsnap,rsnap.sub(destination_path,zfs)
      #pp snaps
      if snaps.has_key?(rsnap.sub(destination_path,zfs))
        $logger.debug("process #{rsnap} to #{sorted[-1]}")
        com = []
        i_opt = '-i'
        # allow the command line option for intermediary to override the property
        if key_comp?(options,'intermediary',[true,false])
          i_opt = '-I' if key_comp?(options, 'intermediary', true)
        elsif key_comp?(props, 'zfsmgmt:send_intermediary')
          i_opt = '-I'
        end

        com += zfs_send_com(options,props,[i_opt, dq('@' + rsnap.split('@')[1])], sorted[-1])
        e = zfs_send_estimate(com) if options[:verbose] == 'pv'
        com += mbuffer_command(options) if options[:mbuffer]
        com += pv_command(options,e) if options[:verbose] == 'pv'
        com += zfs_recv_com(options,[],props,destination_path)

        system_com(com)
        return
      end
      $logger.debug("skipping remote snapshot #{rsnap} because the same snapshot doesn't exist locally #{rsnap.sub(destination_path,zfs)}")
    end
    $logger.error("receiving filesystem has no snapshots that still exists on the sending side, it must be destroyed: #{destination_path}")
    
  end
  def self.mbuffer_command(options)
    mbuffer_command = [ ZfsMgmt.global_options[:mbuffer_binary] ]
    mbuffer_command.push('-q') unless options[:verbose] == 'mbuffer'
    mbuffer_command.push('-m',options[:mbuffer_size]) if options[:mbuffer_size]
    mbuffer_command.push('|')
    mbuffer_command
  end
  def self.zfs_send_com(options,props,extra_opts,target)
    zfs_send_com = [ ZfsMgmt.global_options[:zfs_binary], 'send' ]
    zfs_send_com.push('-v','-P') if key_comp?(options,'verbose','send')
    send_opts = {
      'backup'      => '-b',
      'compressed'  => '-c',
      'embed'       => '-e',
      'holds'       => '-h',
      'large_block' => '-L',
      'props'       => '-p',
      'raw'         => '-w',
      'replicate'   => '-R',
    }
    send_opts.each do |p,o|
      # allow the command line options to override the properties value
      if key_comp?(options,p,[true,false])
        zfs_send_com.push(o) if key_comp?(options,p,true)
      elsif key_comp?(props,"zfsmgmt:send_#{p}")
        zfs_send_com.push(o)
      end
    end
    zfs_send_com + extra_opts + [dq(target),'|']
  end
  def self.zfs_recv_com(options,extra_opts,props,target)
    zfs_recv_com = [ ZfsMgmt.global_options[:zfs_binary], 'recv', '-F', '-s' ]
    recv_opts = {
      'noop'          => '-n',
      'drop_holds'    => '-h',
      'unmount'       => '-u',
      #'discard_last'  => '-e',
      #'discard_first' => '-d',
    }
    recv_opts.each do |p,o|
      # allow the command line options to override the properties value
      if key_comp?(options,p,[true,false])
        zfs_recv_com.push(o) if key_comp?(options,p,true)
      elsif key_comp?(props,"zfsmgmt:recv_#{p}")
        zfs_recv_com.push(o)
      end
    end
    zfs_recv_com.push('-v') if key_comp?(options, 'verbose', ['receive', 'recv'])
    if options[:exclude]
      options[:exclude].each do |x|
        zfs_recv_com.push('-x',x)
      end
    end
    if options[:option]
      options[:option].each do |x|
        zfs_recv_com.push('-o',x)
      end
    end
    zfs_recv_com += extra_opts
    zfs_recv_com.push(dq(target))

    if options[:remote] or props['zfsmgmt:remote']
      if options[:mbuffer]
        zfs_recv_com = mbuffer_command(options) + zfs_recv_com
      end
      zfs_recv_com = recv_command_prefix(options,props) + [ sq(zfs_recv_com.join(' ')) ]
    end
    zfs_recv_com
  end
  def self.recv_command_prefix(options,props)
    ( (options[:remote] or props['zfsmgmt:remote']) ?
        [ 'ssh', ( options[:remote] ? options[:remote] : props['zfsmgmt:remote'] ) ] :
        [] )
  end
  def self.zfs_send_estimate(com)
    lcom = com.dup
    lcom.pop() # remove the pipe symbol
    precom = [ lcom.shift, lcom.shift ]
    lcom.unshift('-P') unless lcom.include?('-P')
    lcom.unshift('-n')
    lcom.push('2>&1')
    lcom = precom + lcom
    $logger.debug(lcom.join(' '))
    total = 0
    %x[#{lcom.join(' ')}].each_line do |l|
      if m = /^size\s+(\d+)$/.match(l)
        return m[1].to_i
      elsif m = /^incremental\s+.+?\s+.+?\s+(\d+)$/.match(l)
        total = total + m[1].to_i
      end
    end
    if total > 0
      return total
    end
    $logger.error("no estimate available")
    return nil
  end
  def self.pv_command(options,estimate)
    a = []
    a += [options[:pv_binary], '-prb' ]
    if estimate
      a += ['-e', '-s', estimate ]
    end
    a.push('|')
    a
  end
    
  def self.sq(s)
    "'#{s}'"
  end
  def self.dq(s)
    "\"#{s}\""
  end
  def self.prop_on?(v)
    ['true','on'].include?(v)
  end
  def self.match_filter?(zfs, filter)
    /#{filter}/ =~ zfs
  end
  def self.key_comp?(h,p,v = method(:prop_on?))
    #$logger.debug("p:#{p}\th[p]:#{h[p]}\tv:#{v}")
    return false unless h.has_key?(p)
    if v.kind_of?(Array)
      return v.include?(h[p])
    elsif v.kind_of?(Hash)
      return v.keys.include?(h[p])
    elsif v.kind_of?(Method)
      return v.call(h[p])
    elsif v.kind_of?(Regexp)
      return v =~ h[p]
    else
      # string, boolean, numbers?
      return h[p] == v
    end
  end
  def self.set_log_level(sev)
    case sev
    when 'debug'
      $logger.level = Logger::DEBUG
    when 'info'
      $logger.level = Logger::INFO
    when 'warn'
      $logger.level = Logger::WARN
    when 'error'
      $logger.level = Logger::ERROR
    when 'fatal'
      $logger.level = Logger::FATAL
    end
  end
  def self.zfs_send_all(options)
    unless lock(options)
      exit(1)
    end
    zfs_managed_list(filter: options[:filter],
                     property_match: { 'zfsmgmt:send' => method(:prop_on?) }).each do |zfs,props,snaps|
      
      if props['zfsmgmt:send@source'] == 'received'
        $logger.debug("skipping received filesystem: #{zfs}")
        next
      end
      if key_comp?(props,'zfsmgmt:send_replicate') and props['zfsmgmt:send_replicate@source'] != 'local'
        $logger.debug("skipping descendant of replicated filesystems: #{zfs}")
        next
      end
      unless props['zfsmgmt:destination']
        $logger.error("#{zfs}: you must specify a destination zfs path via the user property zfsmgmt:destination, even if using --destination on the command line, skipping")
        next
      end
      zfs_send(options,zfs,props,snaps)
    end
    unlock()
  end
  def self.lock(options)
    # open lock file, try to lock file until lock_wait has expired or
    # lock is obtained, write pid?
    return true unless options[:lock]
    $lock = File.open(options[:lock_file], File::RDWR|File::CREAT, 0644)
    if options[:lock_wait] > 0
      status = Timeout::timeout(options[:lock_wait]) do
        if $lock.flock(File::LOCK_EX)
          return true
        end
      end
    else
      # zero is wait forever!
      if $lock.flock(File::LOCK_EX)
        return true
      end
    end
    $logger.error("unable to obtain lock")
    return false
  end
  def self.unlock()
    unless $lock.nil?
      $lock.flock(File::LOCK_UN)
      $lock.close()
    end
  end
end
