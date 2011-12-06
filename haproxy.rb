#!/usr/bin/ruby -w

require 'fileutils'

$:.unshift File.join(File.dirname(__FILE__))
require 'helperfunctions'
require 'load_balancer'
require 'monitoring'

# A class to wrap all the interactions with the haproxy load balacer
class HAProxy
  HAPROXY_PATH = File.join("/", "etc", "haproxy")
  SITES_ENABLED_PATH = File.join(HAPROXY_PATH, "sites-enabled")

  CONFIG_EXTENSION = "cfg"

  # The configuration file haproxy reads from
  MAIN_CONFIG_FILE = File.join(HAPROXY_PATH, "haproxy.#{CONFIG_EXTENSION}")
  # Provides a set of default configurations 
  BASE_CONFIG_FILE = File.join(HAPROXY_PATH, "base.#{CONFIG_EXTENSION}")

  # Options to used to configure servers.
  # For more information see http://haproxy.1wt.eu/download/1.3/doc/configuration.txt
  SERVER_OPTIONS = "maxconn 1 check inter 20000 fastinter 1000 fall 1"

  # The first port to listen on
  START_PORT = 10000
 
  TIME_PERIOD = 10
  SCALE_UP = 1
  SCALE_DOWN = 2
  NO_CHANGE = 0
  SRV_NAME = 1
  QUEUE_CURR = 2
  REQ_RATE = 46
  CURR_RATE = 33
  @@initialized = {}
  @@req_rate =   {}   # Request rate coming in over last 20 seconds
  @@queue_curr = {} # currently Queued requests      
  @@threshold_req_rate = {}
  @@threshold_queue_curr = {}
  @@scale_down_req_rate = {}

  def self.stop
    `service haproxy stop`
  end

  def self.restart
    `service haproxy restart`
  end

  def self.reload
    `service haproxy reload`
  end

  def self.is_running?
    processes = `ps ax | grep haproxy | grep -v grep | wc -l`.chomp
    if processes == "0"
      return false
    else
      return true
    end
  end

  def self.initialize(app_name)
	if @@initialized[app_name].nil?
        	index = 0
        	@@req_rate[app_name] = []
       		@@queue_curr[app_name] = []
		# Assigning a random value now , have to change it later to pick real values
		@@threshold_req_rate[app_name] = 5
		@@scale_down_req_rate[app_name] = 2
		@@threshold_queue_curr[app_name] = 5 
        	while index < TIME_PERIOD
                	@@req_rate[app_name][index] = 0
                	@@queue_curr[app_name][index] = 0
                	index += 1 ;
        	end
		#puts 'Step 4 initialize'
		@@initialized[app_name]=1 
	end
  end

  # The port that the load balancer will be listening on for the given app number
  def self.app_listen_port(app_number)
    START_PORT + app_number
  end

  # Create the configuration file for the AppLoadBalancer Rails application
  def self.create_app_load_balancer_config(my_ip, listen_port)
    self.create_app_config(my_ip, listen_port, LoadBalancer.server_ports, LoadBalancer.name)
  end

  # Create the configuration file for the AppMonitoring Rails application
  def self.create_app_monitoring_config(my_ip, listen_port)
    self.create_app_config(my_ip, listen_port, Monitoring.server_ports, Monitoring.name)
  end

  # Create the config file for PBServer applications
  def self.create_pbserver_config(my_ip, listen_port, table)
    self.create_app_config(my_ip, listen_port, PbServer.server_ports(table), PbServer.name)
  end

  # A generic function for creating haproxy config files used by appscale services
  def self.create_app_config(my_ip, listen_port, server_ports, name)
    servers = []
    server_ports.each_with_index do |port, index|
      servers << HAProxy.server_config(name, index, my_ip, port)
    end

    config = "# Create a load balancer for the #{name} application \n"
    config << "listen #{name} #{my_ip}:#{listen_port} \n"
    config << servers.join("\n")

    config_path = File.join(SITES_ENABLED_PATH, "#{name}.#{CONFIG_EXTENSION}")
    File.open(config_path, "w+") { |dest_file| dest_file.write(config) }

    HAProxy.regenerate_config
  end

  # Generates a load balancer configuration file. Since haproxy doesn't provide
  # an file include option we emulate that functionality here.
  def self.regenerate_config
    conf = File.open(MAIN_CONFIG_FILE,"w+")
    
    # Start by writing in the base file
    File.open(BASE_CONFIG_FILE, "r") do |base|
      conf.write(base.read())
    end

    sites = Dir.entries(SITES_ENABLED_PATH)
    # Remove any files that are not configs
    sites.delete_if { |site| !site.end_with?(CONFIG_EXTENSION) }

    sites.sort!

    # Append each one of the configs into the main one
    sites.each do |site|
      conf.write("\n")
      File.open(File.join(SITES_ENABLED_PATH, site), "r") do |site_config|
        conf.write(site_config.read())
      end
      conf.write("\n")
    end

    conf.close()
    
    # Restart haproxy since we have changed the config
    HAProxy.restart
  end
  
  # Generate the server configuration line for the provided inputs
  def self.server_config app_name, index, ip, port
    "  server #{app_name}-#{index} #{ip}:#{port} #{SERVER_OPTIONS}"
  end

  def self.write_app_config(app_name, app_number, num_of_servers, ip)
    # Add a prefix to the app name to avoid possible conflicts
    full_app_name = "gae_#{app_name}"
    index = 0
    servers = []

    num_of_servers.times do |index|
      port = HelperFunctions.application_port(app_number, index, num_of_servers)
      server = HAProxy.server_config(full_app_name, index, ip, port)
      servers << server
    end
    
    #port_apps[app_name].each{ |port|
    #  server = HAProxy.server_config(full_app_name, index, ip, port)
    #  index+=1
    #  servers << server
    #}

    listen_port = HAProxy.app_listen_port(app_number)
    config = "# Create a load balancer for the app #{app_name} \n"
    config << "listen #{full_app_name} #{ip}:#{listen_port} \n"
    config << servers.join("\n")

    config_path = File.join(SITES_ENABLED_PATH, "#{full_app_name}.#{CONFIG_EXTENSION}")
    File.open(config_path, "w+") { |dest_file| dest_file.write(config) }

    HAProxy.regenerate_config
  end

  def self.add_app_config(app_name, app_number, port_apps,ip)
    # Add a prefix to the app name to avoid possible conflicts
    full_app_name = "gae_#{app_name}"
    index=0
    servers = []
    port_apps[app_name].each { |port|
      server = HAProxy.server_config(full_app_name, index, ip, port)
      index+=1
      servers << server
    }

    listen_port = HAProxy.app_listen_port(app_number)
    config = "# Create a load balancer for the app #{app_name} \n"
    config << "listen #{full_app_name} #{ip}:#{listen_port} \n"
    config << servers.join("\n")

    config_path = File.join(SITES_ENABLED_PATH, "#{full_app_name}.#{CONFIG_EXTENSION}")
    File.open(config_path, "w+") { |dest_file| dest_file.write(config) }
	
    HAProxy.regenerate_config
  end

  def self.remove_app(app_name)
    config_name = "gae_#{app_name}.#{CONFIG_EXTENSION}"
    FileUtils.rm(File.join(SITES_ENABLED_PATH, config_name))
    HAProxy.regenerate_config
  end

  def self.auto_scale(app_name)

    # Checks the autoscaling for each app name 

    # Average Request rates and queued requests set to 0
    avg_req_rate = 0
    avg_queue_curr = 0

    # Get the average of req_rate and time periods and  
    # Get the current request rate  and the currently queued requests  
    # And store the req rate for last 20 seconds 

    # Now maintain the Request rate and Queued requests for last 20 secs

    #puts @@req_rate , @@queue_curr , @@scale_down_req_rate


    index = 0
    while index < ( TIME_PERIOD - 1 )
        @@req_rate[app_name][index] = @@req_rate[app_name][index+1]
        @@queue_curr[app_name][index] = @@queue_curr[app_name][index+1]
        #puts @req_rate[app_name][index]
        avg_req_rate += @@req_rate[app_name][index+1].to_i
        avg_queue_curr += @@queue_curr[app_name][index+1].to_i
        index += 1
    end

    # Run this cmmand for each app and get the queued request and request rate of requests coming in 
    monitor_cmd=`echo \"show info;show stat\" | socat stdio unix-connect:/etc/haproxy/stats | grep #{app_name} `
    # puts monitor_cmd
    

    monitor_cmd.each{ |line_output|
        puts line_output
        array = line_output.split(',')
        #puts array.length
	if array.length < REQ_RATE
		next
	end
        service_name = array[SRV_NAME]
        queue_curr_present = array[QUEUE_CURR]
        req_rate_present = array[REQ_RATE]
        # Not using req rate  as of know 
        rate_last_sec = array[CURR_RATE]
	
	if(service_name=="FRONTEND")
        	puts "#{service_name} #{req_rate_present}"
                req_rate_present = array[REQ_RATE]
                avg_req_rate += req_rate_present.to_i
                @@req_rate[app_name][index]=req_rate_present
        end

        if(service_name=="BACKEND")
        	puts "#{service_name} #{queue_curr_present}"
                queue_curr_present = array[QUEUE_CURR]
                avg_queue_curr += queue_curr_present.to_i
                @@queue_curr[app_name][index] = queue_curr_present
        end
    }

    # Average Request rates and queued requests currently contain the aggregated  sum over last TIME_PERIOD till this timea
    # So we will make a decsion here before 
    total_queue_curr = avg_queue_curr 

    avg_req_rate /= TIME_PERIOD
    avg_queue_curr /= TIME_PERIOD


    if( avg_req_rate <= @@scale_down_req_rate[app_name] && total_queue_curr ==0 )
	#We wish to scaly UP , signal 1 says SCALE_UP
	return SCALE_DOWN
    end
    
    
    puts "#{avg_req_rate} #{avg_queue_curr}";

    if(avg_req_rate > @@threshold_req_rate[app_name] && avg_queue_curr > @@threshold_queue_curr[app_name] )
	 # We wish to scaly UP , signal 1 says SCALE_UP
	 return SCALE_UP
    end

    return NO_CHANGE		

  end

  # Removes all the enabled sites
  def self.clear_sites_enabled
    if File.exists?(SITES_ENABLED_PATH)
      sites = Dir.entries(SITES_ENABLED_PATH)
      # Remove any files that are not configs
      sites.delete_if { |site| !site.end_with?(CONFIG_EXTENSION) }
      full_path_sites = sites.map { |site| File.join(SITES_ENABLED_PATH, site) }
      FileUtils.rm_f full_path_sites

      HAProxy.regenerate_config
    end
  end

  # Set up the folder structure and creates the configuration files necessary for haproxy
  def self.initialize_config
    base_config = <<CONFIG	
global
  maxconn 64000
  ulimit-n 200000

  # log incoming requests - may need to tell syslog to accept these requests
  # http://kevin.vanzonneveld.net/techblog/article/haproxy_logging/
  log             127.0.0.1       local0
  log             127.0.0.1       local1 notice

  # Distribute the health checks with a bit of randomness
  spread-checks 5

  # Bind socket for haproxy stats
  stats socket /etc/haproxy/stats

# Settings in the defaults section apply to all services (unless overridden in a specific config)
defaults

  # apply log settings from the global section above to services
  log global

  # Proxy incoming traffic as HTTP requests
  mode http

  # Use round robin load balancing, however since we will use maxconn that will take precedence
  balance roundrobin

  maxconn 64000

  # Log details about HTTP requests
  #option httplog

  # Abort request if client closes its output channel while waiting for the 
  # request. HAProxy documentation has a long explanation for this option.
  option abortonclose

  # Check if a "Connection: close" header is already set in each direction,
  # and will add one if missing.
  option httpclose

  # If sending a request fails, try to send it to another, 3 times
  # before aborting the request
  retries 3

  # Do not enforce session affinity (i.e., an HTTP session can be served by 
  # any Mongrel, not just the one that started the session
  option redispatch

  # Timeout a request if the client did not read any data for 120 seconds
  timeout client 30000

  # Timeout a request if Mongrel does not accept a connection for 30 seconds
  timeout connect 30000

  # Timeout a request if Mongrel does not accept the data on the connection,
  # or does not send a response back in 120 seconds
  timeout server 30000
  
  # Enable the statistics page 
  stats enable
  stats uri     /haproxy?stats
  stats realm   Haproxy\ Statistics
  stats auth    haproxy:stats

  # Create a monitorable URI which returns a 200 if haproxy is up
  # monitor-uri /haproxy?monitor

  # Amount of time after which a health check is considered to have timed out
  timeout check 5000

  # Enabling the socket
  #stats socket /tmp/haproxy
CONFIG

    # Create the sites enabled folder
    unless File.exists? SITES_ENABLED_PATH
      FileUtils.mkdir_p SITES_ENABLED_PATH
    end
    
    # Write the base configuration file which sets default configuration parameters
    File.open(BASE_CONFIG_FILE, "w+") { |dest_file| dest_file.write(base_config) }
  end
end
