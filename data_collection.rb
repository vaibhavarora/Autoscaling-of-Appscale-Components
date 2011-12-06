#!/usr/bin/ruby -w
SRV_NAME = 1
QUEUE_CURR = 2
REQ_RATE = 46
CURR_RATE = 33
time_arg = ARGV[0].to_i
app_name = ARGV[1]
arg_3 = ARGV[2]
puts time_arg
time = 1 

mem_cmd = "sar -r 2 #{time_arg} > appserver_scale_mem_#{arg_3}.txt &"
cpu_cmd = "sar 2 #{time_arg} > appserver_scale_cpu_#{arg_3}.txt & "

puts mem_cmd , cpu_cmd
exec_cmd = `#{mem_cmd}`
exec_cpu_cmd =`#{cpu_cmd}`

while time <= time_arg
    # Run this cmmand for each app and get the queued request and request rate of requests coming in 
    monitor_cmd=`echo \"show info;show stat\" | socat stdio unix-connect:/etc/haproxy/stats | grep #{app_name} `
    # puts monitor_cmd


    monitor_cmd.each{ |line_output|
        #puts line_output
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
                #puts "#{service_name} #{req_rate_present}"
                req_rate_present = array[REQ_RATE]
        end

        if(service_name =="BACKEND")
                str="#{time} #{queue_curr_present}"
		write_cmd=`echo #{str} >> queue-size_#{arg_3}`
                queue_curr_present = array[QUEUE_CURR]
        end
    }
    out2 =` ps aux | grep #{app_name} | grep -v grep | wc -l ` 
    ans = out2.to_i / 2 -1 ;
    `echo #{time} #{ans} >> devappservers_#{arg_3}`
    time+=1
    sleep(2)
end



