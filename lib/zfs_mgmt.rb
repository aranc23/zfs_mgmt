# coding: utf-8
require "zfs_mgmt/version"
require "thor"
require "zfs_mgmt/zfs"
require "zfs_mgmt/config"
require "zfs_mgmt/restic"
require "zfs_mgmt/zfs_mgr"
require 'pp'
require 'date'
require 'logger'
require 'text-table'
require 'open3'
require 'filesize'
require 'timeout'
require 'pstore'
require 'json'
require 'fileutils'

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
  KNOCKOUT_PREFIX = '**'.freeze
  DESTINATION_REMOTE_RE = /\A((?:[^@:\/\s]+@)?[^:\/\s]+):(.+)\z/

  class << self
    attr_accessor :global_options, :config
  end

  def self.parse_destination(dest)
    return [nil, nil] if dest.nil?

    str = dest.to_s
    if (m = DESTINATION_REMOTE_RE.match(str))
      [m[1], m[2]]
    else
      [nil, str]
    end
  end

  def self.format_destination(remote, path)
    return path if remote.nil? || remote.to_s.empty? || path.nil? || path.to_s.empty?

    "#{remote}:#{path}"
  end

  def self.parse_destinations(value)
    case value
    when nil
      []
    when String
      value.split(',').map(&:strip).reject(&:empty?).uniq
    when Array
      value.flat_map { |v| v.is_a?(String) ? v.split(',').map(&:strip) : [] }.reject(&:empty?).uniq
    else
      []
    end
  end

  def self.warn_deprecated_remote(context = nil)
    suffix = context ? " (#{context})" : ''
    $logger&.warn("remote option is deprecated#{suffix}; include the remote host in destination instead (e.g. user@host:pool/dataset)")
  end

  def self.parse_excludes(value)
    case value
    when nil
      []
    when String
      value.split(',').map(&:strip).reject(&:empty?)
    when Array
      value.map { |v| v.to_s.strip }.reject(&:empty?)
    else
      []
    end
  end

  DEFAULT_SSH_CONFIG = { 'BatchMode' => 'yes' }.freeze

  def self.parse_kv_entry(entry, context: nil, label: 'override')
    str = entry.to_s.strip
    return nil if str.empty?
    return [KNOCKOUT_PREFIX, KNOCKOUT_PREFIX] if str == KNOCKOUT_PREFIX

    if str.include?('=')
      k, v = str.split('=', 2)
      k = k.strip
      v = v.strip
      if k.empty?
        target = context ? " on #{context}" : ''
        $logger&.warn("ignoring malformed #{label} '#{str}'#{target}")
        return nil
      end
      return [k, v]
    end

    if str.start_with?(KNOCKOUT_PREFIX)
      k = str[KNOCKOUT_PREFIX.length..-1].strip
      return [KNOCKOUT_PREFIX, KNOCKOUT_PREFIX] if k.empty?
      return [k, KNOCKOUT_PREFIX]
    end

    target = context ? " on #{context}" : ''
    $logger&.warn("ignoring malformed #{label} '#{str}'#{target}")
    nil
  end

  def self.parse_kv_map(value, context: nil, label: 'override')
    case value
    when nil
      {}
    when Hash
      value.each_with_object({}) do |(k, v), h|
        key = k.to_s.strip
        next if key.empty?

        h[key] = v.to_s.strip
      end
    when String
      result = {}
      value.split(',').each do |part|
        parsed = parse_kv_entry(part, context: context, label: label)
        next unless parsed

        result[parsed[0]] = parsed[1]
      end
      result
    when Array
      result = {}
      value.each do |entry|
        parsed = parse_kv_entry(entry, context: context, label: label)
        next unless parsed

        result[parsed[0]] = parsed[1]
      end
      result
    else
      {}
    end
  end

  def self.parse_override_entry(entry, context: nil)
    parse_kv_entry(entry, context: context, label: 'override')
  end

  def self.parse_overrides(value, context: nil)
    parse_kv_map(value, context: context, label: 'override')
  end

  def self.parse_ssh_config(value, context: nil)
    parse_kv_map(value, context: context, label: 'ssh_config')
  end

  def self.merge_excludes(*layers, strip_knockouts: true)
    result = []
    layers.each do |layer|
      next if layer.nil?

      parse_excludes(layer).each do |item|
        if item == KNOCKOUT_PREFIX
          result.clear
          result << KNOCKOUT_PREFIX unless strip_knockouts
        elsif item.start_with?(KNOCKOUT_PREFIX)
          target = item[KNOCKOUT_PREFIX.length..-1].strip
          next if target.empty?

          result.delete(target)
          unless strip_knockouts
            knockout_token = "#{KNOCKOUT_PREFIX}#{target}"
            result << knockout_token unless result.include?(knockout_token)
          end
        else
          result.delete("#{KNOCKOUT_PREFIX}#{item}")
          result << item unless result.include?(item)
        end
      end
    end
    result
  end

  def self.merge_overrides(*layers, strip_knockouts: true)
    result = {}
    layers.each do |layer|
      next if layer.nil?

      parse_overrides(layer).each do |k, v|
        if k == KNOCKOUT_PREFIX
          result.clear
          result[KNOCKOUT_PREFIX] = KNOCKOUT_PREFIX unless strip_knockouts
        elsif v == KNOCKOUT_PREFIX
          if strip_knockouts
            result.delete(k)
          else
            result[k] = KNOCKOUT_PREFIX
          end
        else
          result[k] = v
        end
      end
    end
    result
  end

  def self.merge_ssh_config(*layers, strip_knockouts: true)
    result = {}
    layers.each do |layer|
      next if layer.nil?

      parse_ssh_config(layer).each do |k, v|
        if k == KNOCKOUT_PREFIX
          result.clear
          result[KNOCKOUT_PREFIX] = KNOCKOUT_PREFIX unless strip_knockouts
        else
          result.delete_if { |existing_k, _| existing_k.downcase == k.downcase }
          if v == KNOCKOUT_PREFIX
            result[k] = KNOCKOUT_PREFIX unless strip_knockouts
          else
            result[k] = v
          end
        end
      end
    end
    result
  end

  def self.resolve_excludes(zfs_obj, cli_exclude = nil)
    layers = [zfs_obj.excludes]
    if config && !zfs_obj.config_merged?
      layers << config.excludes
      layers.concat(config.zfs_excludes_layers_for(zfs_obj.name))
    end
    layers << cli_exclude unless cli_exclude.nil?
    merge_excludes(*layers, strip_knockouts: true)
  end

  def self.resolve_overrides(zfs_obj, cli_option = nil)
    layers = [zfs_obj.overrides]
    if config && !zfs_obj.config_merged?
      layers << config.overrides
      layers.concat(config.zfs_overrides_layers_for(zfs_obj.name))
    end
    layers << cli_option unless cli_option.nil?
    merge_overrides(*layers, strip_knockouts: true)
  end

  def self.resolve_ssh_config(zfs_obj, extra_ssh_config = nil)
    layers = [DEFAULT_SSH_CONFIG, zfs_obj.ssh_config]
    if config && !zfs_obj.config_merged?
      layers << config.ssh_config
      layers.concat(config.zfs_ssh_config_layers_for(zfs_obj.name))
    end
    layers << extra_ssh_config unless extra_ssh_config.nil?
    merge_ssh_config(*layers, strip_knockouts: true)
  end

  def self.ssh_command_prefix(remote, ssh_config = DEFAULT_SSH_CONFIG)
    return [] if remote.nil? || remote.to_s.empty?

    com = ['ssh']
    opts = ssh_config.nil? ? DEFAULT_SSH_CONFIG : ssh_config
    opts.each do |k, v|
      next if v == KNOCKOUT_PREFIX

      opt_str = "#{k}=#{v}"
      com.push('-o', opt_str =~ /\s/ ? dq(opt_str) : opt_str)
    end
    com.push(remote)
    com
  end

  def self.build_zfs_from_props(zfs_name, props)
    ZfsMgmt::Zfs.from_zfs_props(zfs_name, merge_config_props(zfs_name, props), config_merged: !config.nil?)
  end

  def self.config_dir
    if global_options && global_options[:config_dir]
      return global_options[:config_dir]
    end
    if Process.euid == 0
      '/etc/zfsmgr'
    elsif ENV['XDG_CONFIG_HOME'] && !ENV['XDG_CONFIG_HOME'].empty?
      File.join(ENV['XDG_CONFIG_HOME'], 'zfsmgr')
    else
      File.join(Dir.home, '.config', 'zfsmgr')
    end
  end

  def self.load_config(dir = config_dir)
    self.config = ZfsMgmt::Config.load_dir(dir)
  end

  def self.merge_config_props(zfs_name, props)
    base = props || {}
    return base unless config && zfs_name && !zfs_name.empty?

    top_defaults = {}
    unless config.mbuffer.nil?
      top_defaults['zfsmgmt:mbuffer'] = config.mbuffer ? 'true' : 'false'
    end
    if config.mbuffer_size && !config.mbuffer_size.to_s.empty?
      top_defaults['zfsmgmt:mbuffer_size'] = config.mbuffer_size
    end

    merged = base.merge(top_defaults).merge(config.zfs_props_for(zfs_name))
    excludes = merge_excludes(base['zfsmgmt:excludes'], config.excludes, *config.zfs_excludes_layers_for(zfs_name), strip_knockouts: true)
    overrides = merge_overrides(base['zfsmgmt:overrides'], config.overrides, *config.zfs_overrides_layers_for(zfs_name), strip_knockouts: true)
    ssh_config = merge_ssh_config(base['zfsmgmt:ssh_config'], config.ssh_config, *config.zfs_ssh_config_layers_for(zfs_name), strip_knockouts: false)
    if excludes.empty?
      merged.delete('zfsmgmt:excludes')
    else
      merged['zfsmgmt:excludes'] = excludes
    end
    if overrides.empty?
      merged.delete('zfsmgmt:overrides')
    else
      merged['zfsmgmt:overrides'] = overrides
    end
    if ssh_config.empty?
      merged.delete('zfsmgmt:ssh_config')
    else
      merged['zfsmgmt:ssh_config'] = ssh_config
    end
    merged
  end
  class ZfsGetError < StandardError
  end
  class ZfsDatasetNotFoundError < ZfsGetError
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
      'recv_state',
      'mbuffer',
      'mbuffer_size',
      'excludes',
      'overrides',
      'ssh_config',
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
      if se =~ /cannot open '.*': dataset does not exist/i || se =~ /dataset does not exist/i
        $logger.debug("dataset does not exist when running \"#{com.join(' ')}\", exit status #{status.exitstatus}")
        so.split("\n").each { |l| $logger.debug("stdout: #{l}") }
        se.split("\n").each { |l| $logger.debug("stderr: #{l}") }
        raise ZfsDatasetNotFoundError, "dataset does not exist: #{zfs}"
      end
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
  def self.snapshot_destroy_policy(zfs, props = nil, snaps = nil)
    if zfs.is_a?(ZfsMgmt::Zfs)
      zfs_obj = zfs
      snaps ||= zfs_obj.snapshots
    else
      zfs_obj = build_zfs_from_props(zfs, props)
    end

    minage = 0
    if zfs_obj.minage
      minage = timespec_to_seconds(zfs_obj.minage)
    end
    strategy = 'oldest'
    if zfs_obj.strategy == 'youngest'
      strategy = 'youngest'
    end
    sorted = snaps.keys.sort { |a,b| snaps[b]['creation'] <=> snaps[a]['creation'] }
    
    counters = policy_parser(zfs_obj.policy)
    $logger.debug(counters)
    saved = {}

    # set the counters variable to track the number of saved daily/hourly/etc. snapshots
    $date_patterns.each do |d,p|
      saved[d] = {}
    end

    sorted.each do |snap_name|
      if zfs_obj.ignoresnaps and /#{zfs_obj.ignoresnaps}/ =~ snap_name.split('@')[1]
        $logger.debug("skipping #{snap_name} because it matches ignoresnaps pattern: #{zfs_obj.ignoresnaps}")
        next
      end
      if zfs_obj.matchsnaps and not /#{zfs_obj.matchsnaps}/ =~ snap_name.split('@')[1]
        $logger.debug("skipping #{snap_name} because it does not match matchsnaps pattern: #{zfs_obj.matchsnaps}")
        next
      end
      snaptime = local_epoch_to_datetime(snaps[snap_name]['creation'])
      $date_patterns.each do |d,p|
        pat = snaptime.strftime(p)
        if saved[d].has_key?(pat)
          if zfs_obj.prefersnaps and /#{zfs_obj.prefersnaps}/ !~ saved[d][pat].split('@')[1] and /#{zfs_obj.prefersnaps}/ =~ snap_name.split('@')[1]
            $logger.debug("updating the saved snapshot, we prefer this one: \"#{pat}\" to #{snap_name} at #{snaptime}")
            saved[d][pat] = snap_name
          elsif strategy == 'oldest' and ( not zfs_obj.prefersnaps or /#{zfs_obj.prefersnaps}/ =~ snap_name.split('@')[1] )
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
      if zfs_obj.ignoresnaps and /#{zfs_obj.ignoresnaps}/ =~ snap.split('@')[1]
        $logger.debug("skipping #{snap} because it matches ignoresnaps pattern: #{zfs_obj.ignoresnaps}")
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
  def self.zfs_managed_list(filter: '.+', properties: ['all'], property_match: { manage: true } )
    zfss = []
    zfsget(properties: properties).each do |zfs_name,props|
      unless match_filter?(zfs_name, filter)
        next
      end
      merged_props = merge_config_props(zfs_name, props)
      zfs = ZfsMgmt::Zfs.from_zfs_props(zfs_name, merged_props, config_merged: !config.nil?)
      managed = true
      property_match.each do |k,v|
        matched = if k.to_s.start_with?('zfsmgmt:')
                    key_comp?(merged_props, k, v)
                  else
                    attr_name = (k.to_sym == :send ? :send_enabled : k.to_sym)
                    match_value?(zfs.public_send(attr_name), v)
                  end
        unless matched
          managed = false
          break
        end
      end
      next unless managed
      zfss.push(zfs)
    end
    return zfss
  end
  def self.snapshot_policy(filter: '.+', show_destroy: false)
    zfs_managed_list(filter: filter).each do |zfs|
      unless zfs.policy
        $logger.error("zfs_mgmt is configured to manage #{zfs.name}, but there is no policy configuration in zfsmgmt:policy, skipping")
        next # zfs
      end

      snaps = zfs.snapshots
      if snaps.empty?
        $logger.debug("not processing #{zfs.name} as there are no snapshots")
        next
      end

      begin
        # call the function that decides who to save and who to delete
        (saved,saved_snaps,deleteme) = snapshot_destroy_policy(zfs)
      rescue ArgumentError
        $logger.error("zfs_mgmt is configured to manage #{zfs.name}, but there is no valid policy configuration, skipping")
        next
      end

      snaps_to_show = show_destroy ? (saved_snaps + deleteme).uniq : saved_snaps

      if snaps_to_show.length == 0
        $logger.info("no snapshots marked as saved by policy for #{zfs.name}")
        next
      end
      # print a table of saved snapshots with the reasons it is being saved
      table = Text::Table.new
      table.head = [zfs.name,'creation','hourly','daily','weekly','monthly','yearly']
      table.rows = []
      snaps_to_show.sort { |a,b| snaps[b]['creation'] <=> snaps[a]['creation'] }.each do |snap|
        table.rows << [snap.split('@')[1],local_epoch_to_datetime(snaps[snap]['creation'])] + find_saved_reason(saved,snap)
      end
      print table.to_s
    end
  end
  def self.snapshot_destroy(noop: false, verbose: false, filter: '.+')
    zfs_managed_list(filter: filter).each do |zfs|
      unless zfs.policy
        $logger.error("zfs_mgmt is configured to manage #{zfs.name}, but there is no policy configuration in zfsmgmt:policy, skipping")
        next # zfs
      end

      snaps = zfs.snapshots
      if snaps.empty?
        $logger.debug("not processing #{zfs.name} as there are no snapshots")
        next
      end

      begin
        # call the function that decides who to save and who to delete
        (saved,saved_snaps,deleteme) = snapshot_destroy_policy(zfs)
      rescue ArgumentError
        $logger.error("zfs_mgmt is configured to manage #{zfs.name}, but there is no valid policy configuration, skipping")
        next
      end
    
      $logger.info("deleting #{deleteme.length} snapshots for #{zfs.name}")
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
          bigarg = "#{zfs.name}@#{deleteme[0..max].map { |s| s.split('@')[1] }.join(',')}"
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
    dt = DateTime.now
    zfs_managed_list(filter: filter, property_match: { snapshot: true }).each do |zfs|
      next unless zfs.snapshot

      prefix = zfs.snap_prefix || 'zfsmgmt'
      ts = zfs.snap_timestamp || '%FT%T%z'
      com = [global_options['zfs_binary'] || global_options[:zfs_binary], 'snapshot']
      com.push('-r') if zfs.snapshot_recursive
      com.push("#{zfs.name}@#{[prefix,dt.strftime(ts)].join('-')}")
      system_com(com,noop)
    end
  end
  def self.system_com(com, noop = false)
    comstr = com.join(' ')
    $logger.info(comstr)
    unless noop
      system(comstr)
      unless $?.success?
        $logger.error("command failed: #{$?.exitstatus}")
      end
      return $?
    end
  end
  def self.zfs_send(options, zfs, props = nil, snaps = nil)
    if zfs.is_a?(ZfsMgmt::Zfs)
      zfs_obj = zfs
      snaps ||= zfs_obj.snapshots
    else
      zfs_obj = build_zfs_from_props(zfs, props)
    end
    zfs_name = zfs_obj.name
    sorted = snaps.keys.sort { |a,b| snaps[a]['creation'] <=> snaps[b]['creation'] }
    discard_first = recv_discard_first?(options, zfs_obj)
    entries = zfs_obj.destination_entries
    if entries.empty?
      $logger.error("fatal error: unable to determine destination path for #{zfs_name}")
      exit(1)
    end
    opt_remote = options.key?(:remote) ? options[:remote] : options['remote']
    opt_remote = nil if opt_remote.respond_to?(:empty?) && opt_remote.empty?
    warn_deprecated_remote('CLI --remote') if opt_remote

    entries.each do |entry|
      zfs_send_to_destination(options, zfs_obj, snaps, sorted, entry, discard_first: discard_first, opt_remote: opt_remote)
    end
  end

  def self.zfs_send_to_destination(options, zfs_obj, snaps, sorted, entry, discard_first:, opt_remote:)
    zfs_name = zfs_obj.name
    raw_destination = discard_first ? zfs_obj.discard_first_for(entry[:base]) : entry[:resolved]
    raw_recv_target = discard_first ? entry[:base] : raw_destination
    _, destination_path = parse_destination(raw_destination)
    _, recv_target = parse_destination(raw_recv_target)
    if destination_path.nil? || destination_path.empty?
      $logger.error("fatal error: unable to determine destination path for #{zfs_name}")
      return
    end
    entry_remote = entry[:explicit_remote] || opt_remote || zfs_obj.remote
    entry_remote = nil if entry_remote.respond_to?(:empty?) && entry_remote.empty?
    cmd_prefix = entry_remote ? ssh_command_prefix(entry_remote, resolve_ssh_config(zfs_obj)) : []
    state_destination = format_destination(entry_remote, destination_path)

    # does the destination zfs already exist?
    remote_zfs_state = ''
    begin
      recv_zfs = zfsget(zfs: destination_path,
                        command_prefix: cmd_prefix,
                        #properties: ['receive_resume_token'],
                       )
    rescue ZfsDatasetNotFoundError
      $logger.debug("recv filesystem doesn't exist: #{destination_path}")
      remote_zfs_state = 'missing'
    rescue ZfsGetError
      $logger.error("unable to check remote filesystem #{destination_path}")
      return
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
        zfs_obj.send_replicate
        source = sorted[-1]
      end
      com += zfs_send_com(options,
                          zfs_obj,
                          [],
                          source,
                         )
      e = zfs_send_estimate(com) if options[:verbose] == 'pv'
      com += mbuffer_command(options, zfs_obj) if mbuffer_enabled?(options, zfs_obj)
      com += pv_command(options,e) if options[:verbose] == 'pv'
      com += zfs_recv_com(options,[],zfs_obj,recv_target, warn_deprecated: false, command_prefix: cmd_prefix)
 
      status = system_com(com)
      record_send_state(zfs_name, status, destination: state_destination) unless options[:noop]
      unless status && status.success?
        return
      end

    elsif remote_zfs_state != 'present'
      # should be resumable!
      com = [ ]
      com.push( ZfsMgmt.global_options[:zfs_binary], 'send', '-t', remote_zfs_state )
      com.push('-v','-P') if key_comp?(options, 'verbose', 'send')
      com.push('|')
      e = zfs_send_estimate(com) if options[:verbose] == 'pv'
      com += mbuffer_command(options, zfs_obj) if mbuffer_enabled?(options, zfs_obj)
      com += pv_command(options,e) if options[:verbose] == 'pv'

      com += zfs_recv_com(options,[],zfs_obj,recv_target, warn_deprecated: false, command_prefix: cmd_prefix)

      status = system_com(com)
      record_send_state(zfs_name, status, destination: state_destination) unless options[:noop]
      unless status && status.success?
        return
      end
    end
    
    # the zfs already exists, so update with incremental?
    begin
      remote_snaps = zfsget(zfs: destination_path,
                            types: ['snapshot'],
                            command_prefix: cmd_prefix,
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
    if remote_snaps.has_key?(sorted[-1].sub(zfs_name,destination_path))
      $logger.info("the most recent local snapshot (#{sorted[-1]}) already exists on the remote side (#{sorted[-1].sub(zfs_name,destination_path)})")
      return
    end
    remote_snaps.sort_by { |k,v| -v['creation'] }.each do |rsnap,v|
      # oldest first
      if snaps.has_key?(rsnap.sub(destination_path,zfs_name))
        $logger.debug("process #{rsnap} to #{sorted[-1]}")
        com = []
        i_opt = '-i'
        # allow the command line option for intermediary to override the property
        if key_comp?(options,'intermediary',[true,false])
          i_opt = '-I' if key_comp?(options, 'intermediary', true)
        elsif zfs_obj.send_intermediary
          i_opt = '-I'
        end

        com += zfs_send_com(options,zfs_obj,[i_opt, dq('@' + rsnap.split('@')[1])], sorted[-1])
        e = zfs_send_estimate(com) if options[:verbose] == 'pv'
        com += mbuffer_command(options, zfs_obj) if mbuffer_enabled?(options, zfs_obj)
        com += pv_command(options,e) if options[:verbose] == 'pv'
        com += zfs_recv_com(options,[],zfs_obj,recv_target, warn_deprecated: false, command_prefix: cmd_prefix)

        status = system_com(com)
        record_send_state(zfs_name, status, destination: state_destination) unless options[:noop]
        return
      end
      $logger.debug("skipping remote snapshot #{rsnap} because the same snapshot doesn't exist locally #{rsnap.sub(destination_path,zfs_name)}")
    end
    $logger.error("receiving filesystem has no snapshots that still exists on the sending side, it must be destroyed: #{destination_path}")
    
  end
  def self.mbuffer_enabled?(options, props = nil)
    opts = options || {}
    ['mbuffer', :mbuffer].each do |k|
      return opts[k] if key_comp?(opts, k, [true, false])
    end
    if props.is_a?(ZfsMgmt::Zfs)
      return props.mbuffer if props.config_merged?
      if config
        cfg_props = config.zfs_props_for(props.name)
        return !prop_off?(cfg_props['zfsmgmt:mbuffer']) if cfg_props.key?('zfsmgmt:mbuffer')
        return config.mbuffer unless config.mbuffer.nil?
      end
      return props.mbuffer
    end
    if config && !config.mbuffer.nil?
      return config.mbuffer
    end
    return false if props.is_a?(Hash) && key_comp?(props, 'zfsmgmt:mbuffer', method(:prop_off?))
    true
  end

  def self.resolve_mbuffer_size(options, props = nil)
    opts = options || {}
    ['mbuffer_size', :mbuffer_size].each do |k|
      val = opts[k]
      return val if val && !val.to_s.empty?
    end
    if props.is_a?(ZfsMgmt::Zfs)
      return props.mbuffer_size if props.config_merged?
      if config
        cfg_props = config.zfs_props_for(props.name)
        return cfg_props['zfsmgmt:mbuffer_size'] if cfg_props.key?('zfsmgmt:mbuffer_size') && !cfg_props['zfsmgmt:mbuffer_size'].to_s.empty?
        return config.mbuffer_size if config.mbuffer_size && !config.mbuffer_size.to_s.empty?
      end
      return props.mbuffer_size
    end
    if config && config.mbuffer_size && !config.mbuffer_size.to_s.empty?
      return config.mbuffer_size
    end
    return props['zfsmgmt:mbuffer_size'] if props.is_a?(Hash) && props['zfsmgmt:mbuffer_size'] && !props['zfsmgmt:mbuffer_size'].to_s.empty?
    nil
  end

  def self.mbuffer_command(options, props = nil)
    opts = options || {}
    mbuffer_bin = (ZfsMgmt.global_options && (ZfsMgmt.global_options[:mbuffer_binary] || ZfsMgmt.global_options['mbuffer_binary'])) || 'mbuffer'
    mbuffer_command = [ mbuffer_bin ]
    mbuffer_command.push('-q') unless opts[:verbose] == 'mbuffer'
    size = resolve_mbuffer_size(opts, props)
    mbuffer_command.push('-m', size) if size && !size.to_s.empty?
    mbuffer_command.push('|')
    mbuffer_command
  end
  def self.zfs_send_com(options,props,extra_opts,target)
    zfs_obj = props.is_a?(ZfsMgmt::Zfs) ? props : ZfsMgmt::Zfs.from_zfs_props('', props)
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
      elsif zfs_obj.public_send("send_#{p}")
        zfs_send_com.push(o)
      end
    end
    zfs_send_com + extra_opts + [dq(target),'|']
  end
  def self.prop_off?(v)
    ['false','off'].include?(v)
  end
  def self.recv_state_enabled?(options, props)
    ['state', :state, 'recv_state', :recv_state].each do |k|
      return options[k] if key_comp?(options, k, [true, false])
    end
    return props.recv_state if props.is_a?(ZfsMgmt::Zfs)
    return false if key_comp?(props, 'zfsmgmt:recv_state', method(:prop_off?))
    true
  end
  def self.recv_discard_first?(options, zfs_obj)
    if key_comp?(options, 'discard_first', [true, false])
      key_comp?(options, 'discard_first', true)
    else
      zfs_obj.recv_discard_first
    end
  end
  VOLUME_UNSUPPORTED_RECV_PROPERTIES = %w[
    canmount
    mountpoint
    quota
  ].freeze

  def self.unsupported_volume_recv_prop?(prop_spec)
    prop_name = prop_spec.to_s.split('=', 2).first.to_s.strip.downcase
    VOLUME_UNSUPPORTED_RECV_PROPERTIES.include?(prop_name)
  end

  def self.zfs_recv_com(options,extra_opts,props,target, warn_deprecated: true, command_prefix: nil)
    zfs_obj = props.is_a?(ZfsMgmt::Zfs) ? props : ZfsMgmt::Zfs.from_zfs_props('', props)
    zfs_recv_com = [ ZfsMgmt.global_options[:zfs_binary], 'recv' ]
    zfs_recv_com.push('-s') if recv_state_enabled?(options, zfs_obj)
    recv_opts = {
      'noop'          => '-n',
      'drop_holds'    => '-h',
      'unmount'       => '-u',
      'discard_first' => '-d',
      'force'         => '-F'
    }
    recv_opts.each do |p,o|
      # allow the command line options to override the properties value
      if key_comp?(options,p,[true,false])
        zfs_recv_com.push(o) if key_comp?(options,p,true)
      elsif zfs_obj.public_send("recv_#{p}")
        zfs_recv_com.push(o)
      end
    end
    zfs_recv_com.push('-v') if key_comp?(options, 'verbose', ['receive', 'recv'])
    volume = zfs_obj.volume?
    cli_exclude = options.key?(:exclude) ? options[:exclude] : options['exclude']
    cli_option  = options.key?(:option)  ? options[:option]  : options['option']
    excludes  = resolve_excludes(zfs_obj, cli_exclude)
    overrides = resolve_overrides(zfs_obj, cli_option).map { |k, v| "#{k}=#{v}" }
    [['-x', excludes], ['-o', overrides]].each do |flag, values|
      next unless values

      values.each do |val|
        if volume && unsupported_volume_recv_prop?(val)
          $logger.debug("skipping #{flag} #{val} for volume #{zfs_obj.name}")
          next
        end
        zfs_recv_com.push(flag, val)
      end
    end
    zfs_recv_com += extra_opts
    _, target_path = parse_destination(target)
    zfs_recv_com.push(dq(target_path))

    prefix = command_prefix.nil? ? recv_command_prefix(options, zfs_obj, target, warn_deprecated: warn_deprecated) : command_prefix
    unless prefix.empty?
      if mbuffer_enabled?(options, zfs_obj)
        zfs_recv_com = mbuffer_command(options, zfs_obj) + zfs_recv_com
      end
      zfs_recv_com = prefix + [ sq(zfs_recv_com.join(' ')) ]
    end
    zfs_recv_com
  end
  def self.effective_remote(options, props, target = nil, warn_deprecated: true)
    opts = options || {}
    opt_remote = opts.key?(:remote) ? opts[:remote] : opts['remote']
    opt_remote = nil if opt_remote.respond_to?(:empty?) && opt_remote.empty?

    if warn_deprecated && opt_remote
      warn_deprecated_remote('CLI --remote')
    end

    if target
      target_remote, = parse_destination(target)
      return target_remote if target_remote
    end

    if props.is_a?(ZfsMgmt::Zfs)
      return props.destination_explicit_remote if props.destinations.length <= 1 && props.destination_explicit_remote
      return opt_remote if opt_remote
      return props.remote
    elsif props.is_a?(Hash)
      raw_dest = props['zfsmgmt:destination']
      if raw_dest && !raw_dest.is_a?(Array) && !raw_dest.to_s.include?(',')
        prop_dest_remote, = parse_destination(raw_dest)
        return prop_dest_remote if prop_dest_remote
      end
      return opt_remote if opt_remote
      return props['zfsmgmt:remote']
    end

    opt_remote
  end
  def self.recv_command_prefix(options, props, target = nil, warn_deprecated: true)
    remote = effective_remote(options, props, target, warn_deprecated: warn_deprecated)
    return [] unless remote && !remote.empty?

    zfs_obj = props.is_a?(ZfsMgmt::Zfs) ? props : ZfsMgmt::Zfs.from_zfs_props('', props || {})
    ssh_command_prefix(remote, resolve_ssh_config(zfs_obj))
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
  def self.match_value?(actual, expected)
    if expected.kind_of?(Array)
      return expected.include?(actual)
    elsif expected.kind_of?(Hash)
      return expected.keys.include?(actual)
    elsif expected.kind_of?(Method) || expected.kind_of?(Proc)
      return expected.call(actual)
    elsif expected.kind_of?(Regexp)
      return expected =~ actual
    else
      # string, boolean, numbers?
      return actual == expected
    end
  end
  def self.key_comp?(h,p,v = method(:prop_on?))
    #$logger.debug("p:#{p}\th[p]:#{h[p]}\tv:#{v}")
    return false unless h.has_key?(p)
    match_value?(h[p], v)
  end
  def self.set_log_level(sev = nil)
    sev = ENV['ZFS_MGMT_LOGLEVEL'] if sev.nil? || sev.to_s.strip.empty?
    return if sev.nil? || sev.to_s.strip.empty?
    case sev.to_s.strip.downcase
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
    when 'unknown', 'off', 'none', 'silent'
      $logger.level = Logger::UNKNOWN
    when /\A\d+\z/
      $logger.level = sev.to_i
    end
  end
  set_log_level
  def self.zfs_send_all(options)
    zfs_managed_list(filter: options[:filter],
                     property_match: { send_enabled: true }).each do |zfs|
      unless zfs.send_enabled
        $logger.debug("skipping filesystem with send disabled: #{zfs.name}")
        next
      end
      if zfs.destinations.empty?
        $logger.error("#{zfs.name}: you must specify a destination zfs path via the user property zfsmgmt:destination, skipping")
        next
      end
      if zfs.snapshots.empty?
        $logger.debug("not processing #{zfs.name} as there are no snapshots")
        next
      end
      zfs_send(options, zfs)
    end
  end
  def self.lock(options)
    # open lock file, try to lock file until lock_wait has expired or
    # lock is obtained, write pid?
    return nil unless options[:lock]
    begin
      lock = File.open(options[:lock_file], File::RDWR|File::CREAT, 0644)
    rescue Errno::ENOENT => error
      $logger.error("unable to open lock file (#{options[:lock_file]}), possibly the directory doesn't exist")
      raise
    end
    if lock.flock(File::LOCK_EX|File::LOCK_NB)
      lock.flock(File::LOCK_UN)
    else
      # we failed to lock so locked without NB is likely to block for some amount of time
      $logger.info("attempting to lock control file, this may block")
    end

    if options[:lock_wait] > 0
      begin
        status = Timeout::timeout(options[:lock_wait]) do
          return lock if lock.flock(File::LOCK_EX)
          $logger.error("flock of #{options[:lock_file]} failed")
          raise "flock failed"
        end
      rescue Timeout::Error => error
        $logger.error("timed out waiting to lock")
        raise
      end
    else
      # zero is wait forever!
      return lock if lock.flock(File::LOCK_EX)
      $logger.error("flock of #{options[:lock_file]} failed")
      raise "flock failed"
    end
    $logger.error("unable to obtain lock")
    raise "unknown failure in locking file"
  end
  def self.unlock(lock)
    unless lock.nil?
      lock.flock(File::LOCK_UN)
      lock.close()
    end
  end

  SEND_WARN_SECONDS = 36 * 60 * 60
  SEND_CRIT_SECONDS = 72 * 60 * 60
  SEND_STATE_KEY_SEPARATOR = ' -> '.freeze

  def self.send_state_db_path
    if global_options && global_options[:state_db]
      return global_options[:state_db]
    end
    dir = if Process.euid == 0
            '/var/lib/zfs_mgmt'
          elsif ENV['XDG_STATE_HOME'] && !ENV['XDG_STATE_HOME'].empty?
            File.join(ENV['XDG_STATE_HOME'], 'zfs_mgmt')
          else
            File.join(Dir.home, '.local', 'state', 'zfs_mgmt')
          end
    File.join(dir, 'send_state.pstore')
  end

  def self.send_state_store(path = send_state_db_path)
    FileUtils.mkdir_p(File.dirname(path))
    PStore.new(path)
  end

  def self.send_state_key(source, destination = nil)
    return source.to_s if destination.nil? || destination.to_s.empty?

    "#{source}#{SEND_STATE_KEY_SEPARATOR}#{destination}"
  end

  def self.record_send_state(zfs, status, destination: nil, timestamp: Time.now.to_i)
    exit_code = if status.nil?
                  1
                elsif status.respond_to?(:exitstatus)
                  status.exitstatus || (status.respond_to?(:termsig) && status.termsig ? 128 + status.termsig : 1)
                else
                  status.to_i
                end
    key = send_state_key(zfs, destination)
    payload = {
      'source' => zfs.to_s,
      'destination' => destination,
      'timestamp' => timestamp.to_i,
      'exit' => exit_code
    }
    store = send_state_store
    store.transaction do
      store[key] = JSON.generate(payload)
    end
  end

  def self.send_state_entries(filter: '.+')
    path = send_state_db_path
    return {} unless File.exist?(path)
    store = send_state_store(path)
    entries = {}
    store.transaction(true) do
      store.roots.sort.each do |key|
        key_str = key.to_s
        next unless match_filter?(key_str, filter)
        raw = store[key]
        data = raw.is_a?(String) ? JSON.parse(raw) : raw.dup
        unless data.key?('source')
          src, dst = key_str.split(SEND_STATE_KEY_SEPARATOR, 2)
          data['source'] = src
          data['destination'] = dst if dst && !data.key?('destination')
        end
        entries[key_str] = data
      end
    end
    entries
  end

  def self.send_state_delete(zfs, destination = nil)
    store = send_state_store
    store.transaction do
      if destination && !destination.to_s.empty?
        store.delete(send_state_key(zfs, destination))
      else
        store.delete(zfs)
        prefix = "#{zfs}#{SEND_STATE_KEY_SEPARATOR}"
        store.roots.each do |k|
          store.delete(k) if k.to_s.start_with?(prefix)
        end
      end
    end
  end

  def self.send_state_wipe
    store = send_state_store
    store.transaction do
      store.roots.each do |k|
        store.delete(k)
      end
    end
  end

  def self.send_state_show(check: false, filter: '.+')
    entries = send_state_entries(filter: filter)
    if check
      now = Time.now.to_i
      entries.each do |key, data|
        ts = data['timestamp'].to_i
        exit_code = data['exit'].to_i
        age = [now - ts, 0].max
        time_str = local_epoch_to_datetime(ts).to_s
        state = if exit_code != 0 || age > SEND_CRIT_SECONDS
                  2
                elsif age > SEND_WARN_SECONDS
                  1
                else
                  0
                end
        label = send_state_key(data['source'] || key, data['destination'])
        puts "#{state} \"zfs_send #{label}\" age=#{age};#{SEND_WARN_SECONDS};#{SEND_CRIT_SECONDS} last send #{time_str} exit #{exit_code}"
      end
    else
      return if entries.empty?
      table = Text::Table.new
      table.head = ['source', 'destination', 'timestamp', 'exit']
      table.rows = entries.map do |key, data|
        [data['source'] || key, data['destination'].to_s, local_epoch_to_datetime(data['timestamp'].to_i), data['exit']]
      end
      print table.to_s
    end
  end
end
