#
# Author: François Charlier <francois.charlier@enovance.com>
#

require File.expand_path(File.join(File.dirname(__FILE__), '..', 'mongodb'))
Puppet::Type.type(:mongodb_replset).provide(:mongo, :parent => Puppet::Provider::Mongodb) do

  desc "Manage MongoDB replicaset configuration."

  confine :true =>
    begin
      require 'json'
      true
    rescue LoadError
      false
    end

  mk_resource_methods

  def initialize(resource={})
    super(resource)
    @property_flush = {}
  end

  def members=(members)
    @property_flush[:members] = members
  end

  def self.instances
    instance = get_replset_properties
    if instance
      # There can only be one replset per node
      [new(instance)]
    else
      []
    end
  end

  def self.prefetch(resources)
    instances.each do |prov|
      if resource = resources[prov.name]
        resource.provider = prov
      end
    end
  end

  def exists?
    @property_hash[:ensure] == :present
  end

  def create
    @property_flush[:ensure] = :present
    @property_flush[:members] = resource.should(:members)
  end

  def destroy
    @property_flush[:ensure] = :absent
  end

  def flush
    set_members
    @property_hash = self.class.get_replset_properties
  end

  private

  def db_ismaster(host)
    mongo_command('db.isMaster()', host)
  end

  def rs_initiate(conf, master)
    host = auth_enabled && auth_enabled != 'disabled' ? initialize_host : master
    mongo_command("rs.initiate(#{conf})", host)
  end

  def rs_reconfig(conf, master)
    host = auth_enabled && auth_enabled != 'disabled' ? initialize_host : master
    mongo_command("rs.reconfig(#{conf})", host)
  end

  def rs_status(host)
    mongo_command('rs.status()', host)
  end

  def rs_arbiter
    @resource[:arbiter]
  end

  def auth_enabled
    self.class.auth_enabled
  end

  def initialize_host
    @resource[:initialize_host]
  end

  def master_host(members)
    members.each do |member|
      host = member['host']
      status = db_ismaster(host)
      if status.has_key?('primary')
        return status['primary']
      end
    end
    false
  end

  def self.get_replset_properties
    conn_string = get_conn_string
    begin
      output = mongo_command('rs.conf()', conn_string)
    rescue Puppet::ExecutionFailure => e
      output = {}
    end
    if output['members']
      members = output['members'].collect do |val|
        # Convert mongodb replset config object to standard hash representation
        {
          'host' => val['host'],
          'arbiterOnly' => val['arbiterOnly'],
          'buildIndexes' => val['buildIndexes'],
          'hidden' => val['hidden'],
          'priority' => val['priority'],
          'tags' => val['tags'],
          'slaveDelay' => val['slaveDelay'],
          'votes' => val['votes']
        }
      end
      props = {
        :name     => output['_id'],
        :ensure   => :present,
        :members  => members,
        :provider => :mongo,
      }
    else
      props = nil
    end
    Puppet.debug("MongoDB replset properties: #{props.inspect}")
    props
  end

  def alive_members(members)
    alive = []
    members.select do |member|
      host = member['host']
      begin
        Puppet.debug "Checking replicaset member #{host} ..."
        status = rs_status(host)
        if status.has_key?('errmsg') and status['errmsg'] == 'not running with --replSet'
          raise Puppet::Error, "Can't configure replicaset #{self.name}, host #{host} is not supposed to be part of a replicaset."
        end

        if auth_enabled and status.has_key?('errmsg') and (status['errmsg'].include? "unauthorized" or status['errmsg'].include? "not authorized")
          Puppet.warning "Host #{host} is available, but you are unauthorized because of authentication is enabled: #{auth_enabled}"
          alive.push(member)
        end

        if status.has_key?('set')
          if status['set'] != self.name
            raise Puppet::Error, "Can't configure replicaset #{self.name}, host #{host} is already part of another replicaset."
          end

          # This node is alive and supposed to be a member of our set
          Puppet.debug "Host #{host} is available for replset #{status['set']}"
          alive.push(member)
        elsif status.has_key?('info')
          Puppet.debug "Host #{host} is alive but unconfigured: #{status['info']}"
          alive.push(member)
        end
      rescue Puppet::ExecutionFailure
        Puppet.warning "Can't connect to replicaset member #{host}."
      end
    end
    return alive
  end

  def generate_replset_conf(members)
      members = members.each_with_index.map do |member, id|
        member['_id'] = id
        if rs_arbiter == member['host']
          member['arbiterOnly'] = true
        end
        member.reject{|k,v| v.nil?}
      end

      conf = {
        '_id' => self.name,
        'members' => members
      }

      conf.to_json
  end

  def set_members
    if @property_flush[:ensure] == :absent
      # TODO: I don't know how to remove a node from a replset; unimplemented
      #Puppet.debug "Removing all members from replset #{self.name}"
      #@property_hash[:members].collect do |member|
      #  rs_remove(member, master_host(@property_hash[:members]))
      #end
      return
    end

    if ! @property_flush[:members].empty?
      # Find the alive members so we don't try to add dead members to the replset
      alive = alive_members(@property_flush[:members])
      dead  = @property_flush[:members] - alive
      Puppet.debug "Alive members: #{alive.inspect}"
      Puppet.debug "Dead members: #{dead.inspect}" unless dead.empty?
      raise Puppet::Error, "Can't connect to any member of replicaset #{self.name}." if alive.empty?
    else
      alive = []
    end

    if @property_flush[:ensure] == :present and @property_hash[:ensure] != :present and !master_host(alive)
      Puppet.debug "Initializing the replset #{self.name}"

      conf = generate_replset_conf(alive)

      # Set replset members with the first host as the master
      output = rs_initiate(conf, alive[0]['host'])
      if output['ok'] == 0
        raise Puppet::Error, "rs.initiate() failed for replicaset #{self.name}: #{output['errmsg']}"
      end

      # Check that the replicaset has finished initialization
      retry_limit = 10
      retry_sleep = 3

      retry_limit.times do |n|
        begin
          if db_ismaster(alive[0]['host'])['ismaster']
            Puppet.debug 'Replica set initialization has successfully ended'
            return
          else
            Puppet.debug "Wainting for replica initialization. Retry: #{n}"
            sleep retry_sleep
            next
          end
        end
      end
      raise Puppet::Error, "rs.initiate() failed for replicaset #{self.name}: host #{alive[0]} didn't become master"

    else
      # Update exsting replet config
      Puppet.notice "Updating existing replset #{self.name}"
      if master = master_host(alive)
        conf = generate_replset_conf(alive)
        output = rs_reconfig(conf, master)
        if output['ok'] == 0
          raise Puppet::Error, "rs.reconfig() failed for replicaset #{self.name}: #{output['errmsg']}"
        end
      else
        raise Puppet::Error, "Can't find master host for replicaset #{self.name}."
      end
    end
  end

  def mongo_command(command, host, retries=4)
    self.class.mongo_command(command, host, retries)
  end

  def self.mongo_command(command, host=nil, retries=4)
    begin
      output = mongo_eval("printjson(#{command})", 'admin', retries, host)
    rescue Puppet::ExecutionFailure => e
      Puppet.debug "Got an exception: #{e}"
      raise
    end

    # Dirty hack to remove JavaScript objects
    output.gsub!(/\w+\((\d+).+?\)/, '\1')  # Remove extra parameters from 'Timestamp(1462971623, 1)' Objects
    output.gsub!(/\w+\((.+?)\)/, '\1')

    #Hack to avoid non-json empty sets
    output = "{}" if output == "null\n"

    # Parse the JSON output and return
    JSON.parse(output)

  end

end
