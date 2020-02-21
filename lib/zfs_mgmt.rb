# coding: utf-8
require "zfs_mgmt/version"
require 'pp'
require 'date'
require 'logger'

$logger = Logger.new(STDERR)
$logger.level = Logger::DEBUG

$date_patterns = {
  'hourly' => '%F Hour %H',
  'daily' => '%F',
  'weekly' => '%Y Week %U', # week, starting on sunday
  'monthly' => '%Y-%m',
  'yearly' => '%Y',
}

module ZfsMgmt
  def self.custom_properties()
    return [
      'weekly',
      'daily',
      'hourly',
      'minage',
      'monthly',
      'yearly',
      'manage',
    ].map do |p|
      ['zfsmgmt',p].join(':')
    end
  end
  def self.readsnaps()
    a=[]
    File.open('/etc/zfs-list-snapshots.txt',mode='r') do |s|
      while l = s.gets
        a << l
      end
    end
    a
  end

  def self.zfsget(properties: ['name'],types: ['filesystem','volume'],fs: '')
    results={}
    com = ['zfs', 'get', '-Hp', properties.join(','), '-t', types.join(','), fs]
    %x| #{com.join(' ')} |.split("\n").each do |line|
      params = line.split("\t")
      unless results.has_key?(params[0])
        results[params[0]] = {}
      end
      unless params[2] == '-'
        results[params[0]][params[1]] = params[2]
      end
      unless params[3] == '-'
        results[params[0]]["#{params[1]}@source"] = params[3]
      end
    end
    return results
  end
  def self.local_epoch_to_datetime(e)
    return Time.at(e).to_datetime
  end
  def self.snapshot_destroy(noop: false, verbopt: false, debugopt: false)
    self.zfsget(properties: custom_properties()).each do |zfs,props|
      if props.has_key?('zfsmgmt:manage') and props['zfsmgmt:manage'] == 'true'
        # in order to process the filesystem we must have some guidelines as to what to keep
        # otherwise we would simply delete everything
        # maybe we could just refuse to delete anything unless we have saved at least one thing?
        # might be simpler
        # $date_patterns.keys.each do |spec|
        #   if props.has_key?("zfsmgmt:#{spec}") and props.has_key?("zfsmgmt:#{spec}") =~ /^\d+$/
        #     print "found something\n"
        #     break
        #   end
        # end

        snaps = self.zfsget(properties: ['name','creation','userrefs'],types: ['snapshot'], fs: zfs)
        if snaps.length == 0
          $logger.warn("unable to process this zfs, no snapshots at all: #{zfs}")
          next
        end
        snaps.each do |s,h|
          ['creation','userrefs'].each do |p|
            if h.has_key?(p)
              snaps[s][p] = snaps[s][p].to_i
            end
          end
        end
        sorted = snaps.keys.sort { |a,b| snaps[b]['creation'] <=> snaps[a]['creation'] }
        newest_snapshot_name = sorted.shift
        # set the current patterns
        newest_date = local_epoch_to_datetime(snaps[newest_snapshot_name]['creation'])
        #pp newest_snapshot_name,newest_date
        patterns = {}
        counters = {}
        saved = {}
        $date_patterns.each do |d,p|
          patterns[d] = newest_date.strftime(p)
          if props.has_key?("zfsmgmt:#{d}")
            counters[d] = props["zfsmgmt:#{d}"].to_i
          else
            counters[d] = 0
          end
        end
        #pp patterns,counters
        sorted.each do |snap_name|
          snaptime = local_epoch_to_datetime(snaps[snap_name]['creation'])
          $date_patterns.each do |d,p|
            unless counters[d] > 0
              next
            end
            pat = snaptime.strftime(p)
            #pp snap_name,snaptime,patterns[d],pat
            #system("date -d @#{snaps[snap_name]['creation']}")
            if patterns[d] != pat
              patterns[d] = pat
              unless saved.has_key?(snap_name)
                saved[snap_name]=[]
              end
              saved[snap_name].push("#{d} \##{props["zfsmgmt:#{d}"].to_i-counters[d]+1}: #{pat}")
              counters[d] -= 1
            end
          end
        end
        deleteme = sorted - saved.keys
        $logger.info("deleting #{deleteme.length} snapshots for #{zfs}")
        if deleteme.length > 0
          deleteme.each do |snap_name|
            $logger.debug("delete: #{snap_name} #{local_epoch_to_datetime(snaps[snap_name]['creation']).strftime('%F %T')}")
          end
          saved.each do |snap_name,info|
            $logger.debug("saved: #{snap_name} #{info.join(',')} creation #{local_epoch_to_datetime(snaps[snap_name]['creation']).strftime('%F %T')}")
          end
          bigarg = "#{zfs}@#{deleteme.map { |s| s.split('@')[1] }.join(',')}"
          com_base = "zfs destroy -p"
          if noop
            com_base = "#{com_base}n"
          end
          if verbopt
            com_base = "#{com_base}v"
          end
          com = "#{com_base} #{bigarg}"
          # this is just a guess about how big things can be before running zfs will fail
          if bigarg.length >= 131072 or com.length >= (2097152-10000) 
            deleteme.each do |snap_name|
              minicom="#{com_base} #{snap_name}"
              $logger.info(minicom)
              system(minicom)
            end
          else
            $logger.info(com)
            system(com)
          end
        end
      end
    end
  end
end
