
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "zfs_mgmt/version"

Gem::Specification.new do |spec|
  spec.name          = "zfs_mgmt"
  spec.version       = ZfsMgmt::VERSION
  spec.licenses      = ['GPL-3.0-or-later']
  spec.authors       = ["Aran Cox"]
  spec.email         = ["arancox@gmail.com"]

  spec.summary       = %q{Misc. helpers regarding snapshots and send/recv.}
  #spec.description   = %q{TODO: Write a longer description or delete this line.}
  spec.homepage      = 'https://github.com/aranc23/zfs_mgmt'

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    #spec.metadata["allowed_push_host"] = "TODO: Set to 'http://mygemserver.com'"

    spec.metadata["homepage_uri"] = spec.homepage
    spec.metadata["source_code_uri"] = spec.homepage
    spec.metadata["changelog_uri"] = "#{spec.homepage}/commits/"
  else
    raise "RubyGems 2.0 or newer is required to protect against " \
      "public gem pushes."
  end

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = "bin"
  spec.executables   = ['readsnaps','zfssendman','zfssnapman','zfsrecvman','zfs-list-snapshots','zfsmgr']
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.16"
  spec.add_development_dependency "rake", ">= 12.3.3"
  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency "thor", "~> 1.0"
  spec.add_development_dependency "text-table", "~> 1.2"
  spec.add_development_dependency "filesize", "~> 0.2"
  
end
