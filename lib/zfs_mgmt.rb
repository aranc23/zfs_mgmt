require "zfs_mgmt/version"
require 'pp'

module ZfsMgmt
  def self.readsnaps()
    a=[]
    File.open('/etc/zfs-list-snapshots.txt',mode='r') do |s|
      while l = s.gets
        a << l
      end
    end
    a
  end
  def self.zfsget(properties=['name'],types=['filesystem','volume'],fs)
    results={}
    com = ['zfs', 'get', '-Hp', properties.join(','), '-t', types.join(','), fs]
    print(com,"\n")
    %x| #{com.join(' ')} |.split("\n").each do |line|
      params = line.split("\t")
      unless results.has_key?(params[0])
        results[params[0]] = {}
      end
      results[params[0]][params[1]] = params[2]
      results[params[0]]["#{params[1]}@source"] = params[3]
    end
    return results
  end
  def self.snapshot_purge(noop=True)
    pp self.zfsget(properties=['all'],types=['snapshot'],fs='')
  end
end
