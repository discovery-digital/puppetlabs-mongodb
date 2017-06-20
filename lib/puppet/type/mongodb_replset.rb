#
# Author: François Charlier <francois.charlier@enovance.com>
#

Puppet::Type.newtype(:mongodb_replset) do
  @doc = "Manage a MongoDB replicaSet"

  ensurable do
    defaultto :present

    newvalue(:present) do
      provider.create
    end
  end

  newparam(:name) do
    desc "The name of the replicaSet"
  end

  newparam(:arbiter) do
    desc "The replicaSet arbiter"
  end

  newparam(:initialize_host) do
    desc "Host to use for Replicaset initialization"
    defaultto '127.0.0.1'
  end

  newproperty(:members, :array_matching => :all) do
    desc "The replicaSet members"

    munge do |v|
      # If it's a string, we only have hostname
      if v.is_a? String
        v = { 'host' => v }
      end

      # Convert resource definition to standard hash representation
      {
        'host' => v['host'],
        'arbiterOnly' => v.fetch("arbiter_only", false),
        'buildIndexes' => v.fetch("build_indexes", true),
        'hidden' => v.fetch("hidden", false),
        'priority' => v.fetch("priority", 1),
        'tags' => v.fetch("tags", {}),
        'slaveDelay' => v.fetch('slave_delay', 0),
        'votes' => v.fetch('votes', 1)
      }
    end

    validate do |v|
      if v.is_a? String
        raise ArgumentError, "Hostname must be a non-empty string" if v.empty?
      elsif v.is_a? Hash
        raise ArgumentError, "Host field is required for a replSet member" unless v.key? 'host'
        valid_keys = ['host', 'arbiter_only', 'build_indexes', 'hidden', 'priority', 'tags', 'slave_delay', 'votes']
        v.keys.reject{|k| valid_keys.include? k}.each{|k| raise ArgumentError, "Invalid key in member definition: %s" % k}
      else
        raise ArgumentError, "Invalid member definition. Must either be a hostname string or a replSet member configuration hash."
      end
    end
  end

  autorequire(:package) do
    'mongodb_client'
  end

  autorequire(:service) do
    'mongodb'
  end
end
