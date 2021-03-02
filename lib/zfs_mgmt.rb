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

$logger = Logger.new(STDERR)

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
    $logger.debug("#{com.join(' ')}")
    system(com.join(' '))
    unless $?.success?
      errstr = "unable to set hold: #{hold} for snapshot: #{snapshot}"
      $logger.error(errstr)
      raise errstr
    end
  end

  def self.zfs_release(hold,snapshot)
    com = [@global_options['zfs_binary'], 'release', hold, snapshot]
    $logger.debug("#{com.join(' ')}")
    system(com.join(' '))
    unless $?.success?
      errstr = "unable to release hold: #{hold} for snapshot: #{snapshot}"
      $logger.error(errstr)
      raise errstr
    end
  end

  def self.zfsget(properties: ['name'],types: ['filesystem','volume'],zfs: '', command_prefix: [])
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
  def self.zfs_managed_list(filter: '.+', properties: custom_properties(), property_match: { 'zfsmgmt:manage' => 'true' } )
    zfss = [] # array of arrays
    zfsget(properties: properties).each do |zfs,props|
      unless /#{filter}/ =~ zfs
        next
      end
      managed = true
      property_match.each do |k,v|
        unless props.has_key?(k) and props[k] == v
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
  def self.snapshot_policy(verbopt: false, debugopt: false, filter: '.+')
    if debugopt
      $logger.level = Logger::DEBUG
    else
      $logger.level = Logger::INFO
    end
    zfs_managed_list(filter: filter).each do |zdata|
      (zfs,props,snaps) = zdata
      unless props.has_key?('zfsmgmt:policy') and policy_parser(props['zfsmgmt:policy'])
        $logger.error("zfs_mgmt is configured to manage #{zfs}, but there is no valid policy configuration, skipping")
        next # zfs
      end
      # call the function that decides who to save and who to delete
      (saved,saved_snaps,deleteme) = snapshot_destroy_policy(zfs,props,snaps)

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
  def self.snapshot_destroy(noop: false, verbopt: false, debugopt: false, filter: '.+')
    if debugopt
      $logger.level = Logger::DEBUG
    else
      $logger.level = Logger::INFO
    end
    zfs_managed_list(filter: filter).each do |zdata|
      (zfs,props,snaps) = zdata
      unless props.has_key?('zfsmgmt:policy') and policy_parser(props['zfsmgmt:policy'])
        $logger.error("zfs_mgmt is configured to manage #{zfs}, but there is no valid policy configuration, skipping")
        next # zfs
      end

      # call the function that decides who to save and who to delete
      (saved,saved_snaps,deleteme) = snapshot_destroy_policy(zfs,props,snaps)
    
      $logger.info("deleting #{deleteme.length} snapshots for #{zfs}")
      deleteme.reverse! # oldest first for removal
      deleteme.each do |snap_name|
        $logger.debug("delete: #{snap_name} #{local_epoch_to_datetime(snaps[snap_name]['creation']).strftime('%F %T')}")
      end

      com_base = "zfs destroy -p"
      if deleteme.length > 0
        com_base = "#{com_base}d"
      end
      if noop
        com_base = "#{com_base}n"
      end
      if verbopt
        com_base = "#{com_base}v"
      end
      while deleteme.length > 0
        for i in 0..(deleteme.length - 1) do
          max = deleteme.length - 1 - i
          $logger.debug("attempting to remove snaps 0 through #{max} out of #{deleteme.length} snapshots")
          bigarg = "#{zfs}@#{deleteme[0..max].map { |s| s.split('@')[1] }.join(',')}"
          com = "#{com_base} #{bigarg}"
          $logger.debug("size of bigarg: #{bigarg.length} size of com: #{com.length}")
          if bigarg.length >= 131072 or com.length >= (2097152-10000)
            next
          end
          $logger.info(com)
          deleteme = deleteme - deleteme[0..max]
          system(com)
          if $?.exitstatus != 0
            $logger.error("zfs exited with non-zero status: #{$?.exitstatus}")
          end
          break
        end
      end
    end
  end
  # parse a policy string into a hash of integers
  def self.policy_parser(str)
    res = {}
    $date_patterns.keys.each do |tf|
      res[tf]=0
    end
    p = str.scan(/\d+[#{$time_pattern_map.keys.join('')}]/i)
    unless p.length > 0
      raise "unable to parse the policy configuration #{str}"
    end
    p.each do |pi|
      scn = /(\d+)([#{$time_pattern_map.keys.join('')}])/i.match(pi)
      res[$time_pattern_map[scn[2].downcase]] = scn[1].to_i
    end
    res
  end
  def self.snapshot_create(noop: false, verbopt: false, debugopt: false, filter: '.+')
    if debugopt
      $logger.level = Logger::DEBUG
    else
      $logger.level = Logger::INFO
    end
    dt = DateTime.now
    zfsget(properties: custom_properties()).each do |zfs,props|
      unless /#{filter}/ =~ zfs
        next
      end
      # zfs must have snapshot set to true or recursive
      if props.has_key?('zfsmgmt:snapshot') and
        props['zfsmgmt:snapshot'] == 'true' or
        ( props['zfsmgmt:snapshot'] == 'recursive' and props['zfsmgmt:snapshot@source'] == 'local' ) or
        ( props['zfsmgmt:snapshot'] == 'local' and props['zfsmgmt:snapshot@source'] == 'local' )
        
        prefix = ( props.has_key?('zfsmgmt:snap_prefix') ? props['zfsmgmt:snap_prefix'] : 'zfsmgmt' )
        ts = ( props.has_key?('zfsmgmt:snap_timestamp') ? props['zfsmgmt:snap_timestamp'] : '%FT%T%z' )
        com = [global_options['zfs_binary'],'snapshot']
        if props['zfsmgmt:snapshot'] == 'recursive' and props['zfsmgmt:snapshot@source'] == 'local'
          com.push('-r')
        end
        com.push("#{zfs}@#{[prefix,dt.strftime(ts)].join('-')}")
        $logger.info(com)
        system(com.join(' '))
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
    recv_command_prefix = ( (options[:remote] or props['zfsmgmt:remote']) ?
                              [ 'ssh', ( options[:remote] ? options[:remote] : props['zfsmgmt:remote'] ) ] :
                              [] )
    # does the destination zfs already exist?
    remote_zfs_state = ''
    begin
      recv_zfs = zfsget(zfs: destination_path,
                        command_prefix: recv_command_prefix,
                        properties: ['receive_resume_token'],
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
    if options[:mbuffer]
      mbuffer_command = [ ZfsMgmt.global_options[:mbuffer_binary] ]
      mbuffer_command.push('-q') unless options[:verbose] == 'mbuffer'
      mbuffer_command.push('-m',options[:mbuffer_size]) if options[:mbuffer_size]
      mbuffer_command.push('|')
    end
    zfs_send_com = [ ZfsMgmt.global_options[:zfs_binary], 'send' ]
    zfs_send_com.push('-v','-P') if options[:verbose] and options[:verbose] == 'send'
    zfs_send_com.push('-p') if options[:properties]
    zfs_send_com.push('-w') if options[:raw]
    zfs_send_com.push('-L') if options[:large_block]
    zfs_send_com.push('-e') if options[:embed]
    zfs_send_com.push('-c') if options[:compressed]

    zfs_recv_com = [ ZfsMgmt.global_options[:zfs_binary], 'recv', '-F', '-s' ]
    zfs_recv_com.push('-n') if options[:noop]
    zfs_recv_com.push('-u') if options[:unmount]
    zfs_recv_com.push('-v') if options[:verbose] and ( options[:verbose] == 'receive' or options[:verbose] == 'recv' )
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
    zfs_recv_com.push("\"#{destination_path}\"")

    if options[:remote] or props['zfsmgmt:remote']
      if options[:mbuffer]
        zfs_recv_com = mbuffer_command + zfs_recv_com
      end
      zfs_recv_com = recv_command_prefix + [ "'#{zfs_recv_com.join(' ')}'" ]
    end


    if remote_zfs_state == 'missing'
      # the zfs does not exist, send initial (oldest?) snapshot
      com = []
      com += zfs_send_com
      com.push("\"#{sorted[0]}\"",'|')
      com += mbuffer_command if options[:mbuffer]
      com += zfs_recv_com
 
      $logger.debug(com.join(' '))
      system(com.join(' '))
      unless $?.success?
        $logger.error("initial send failed: #{$?.exitstatus}")
        return
      end

    elsif remote_zfs_state != 'present'
      # should be resumable!
      com = [ ]
      com.push( ZfsMgmt.global_options[:zfs_binary], 'send', '-t', remote_zfs_state )
      com.push('-v','-P') if options[:verbose] and options[:verbose] == 'send'
      com.push('|')
      com += mbuffer_command if options[:mbuffer]
      com += recv_command_prefix if recv_command_prefix.length > 0
      com.push(ZfsMgmt.global_options[:zfs_binary], 'recv', '-s' )
      com.push('-n') if options[:noop]
      com.push('-u') if options[:unmount]
      com.push('-v') if options[:verbose] and ( options[:verbose] == 'receive' or options[:verbose] == 'recv' )
      com.push("\"#{destination_path}\"")
      
      $logger.debug(com.join(' '))
      system(com.join(' '))
      unless $?.success?
        $logger.error("resume failed: #{$?.exitstatus}")
        return
      end
    end
    
    # the zfs already exists, so update with incremental?
    begin
      remote_snaps = zfsget(zfs: destination_path,
                            types: ['snapshot'],
                            command_prefix: recv_command_prefix,
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
        $logger.debug("process #{rsnap} to #{sorted[0]}")
        com = []
        com += zfs_send_com
        com.push(options[:intermediary] ? '-I' : '-i')
        com.push("\"@#{rsnap.split('@')[1]}\"")
        com.push("\"#{sorted[-1]}\"",'|')
        com += mbuffer_command if options[:mbuffer]
        com += zfs_recv_com
 
        $logger.debug(com.join(' '))
        system(com.join(' '))
        return
      end
    end
    $logger.error("receiving filesystem has no snapshots that still exists on the sending side, it must be destroyed: #{destination_path}")
    
  end
end
