module Enumerable
  PARALLELIZE_DEFAULTS = {
    :host => nil,
    :username => nil,
    :queue => nil,
    :project => nil,
    :output => nil,
    :error => nil
  }

  # Enumerable#parallelize applies a block to a list and submits it as a 
  # job array to an LSF cluster. The method optionally takes a number of 
  # arguments.
  # --
  # *Arguments*:
  # * _host_: Farm host to be used to submit the job. If not provided it is 
  # assumed that the script is run from the host itself. Default = nil.
  # * _username_: the username to log into that host. Required if host is
  # provided. Default = nil.
  # * _queue_: Name of the LSF queue (*-q*). If not provided the environment
  # variable LSB_DEFAULTQUEUE is used. Default = nil.
  # * _project_: Name of the project that resources will be charged to (*-P*). If not
  # provided the environment variable LSB_DEFAULTPROJECT is used. Default = nil.
  # * _output_: Name seed for the file where STDOUT will be sent to (*-o*). If not
  # specified the output will be sent by email. Default = nil.
  # * _error_: Name seed for the file where STDERR will be sent to (*-e*). If not 
  # specified the same output sink will be used as for STDOUT. Default = nil.
  # * _jobname_: Job name. Required.
  # * _max_concurrent_: Maximum number of concurrent jobs to be run. Default is
  # the number of elements in the list if not exceeding 1000, else 1000.
  def parallelize(opts = {}, &block)
    opts = {
      :host => PARALLELIZE_DEFAULTS[:host],
      :username => PARALLELIZE_DEFAULTS[:username],
      :queue => PARALLELIZE_DEFAULTS[:queue],
      :project => PARALLELIZE_DEFAULTS[:project],
      :output => PARALLELIZE_DEFAULTS[:output],
      :error => PARALLELIZE_DEFAULTS[:error]
    }.merge(opts)
    
    counter = 1
    self.each do |a|
      tmp_file = nil
      if opts[:host.nil?]
        tmp_file = File.open(opts[:working_dir] + '/' + opts[:jobname] + '-' + counter.to_s + '.sh','w')
      else
        tmp_file = File.open(opts[:jobname] + '-' + counter.to_s + '.sh','w')
      end
      
      tmp_file.puts('puts("' + opts[:queue] + ' ' + opts[:project] + ' ' + block.call(a).to_s + '")')
      tmp_file.chmod(0755)
      tmp_file.close
      counter += 1
    end
    
    max_concurrent = [10, self.length].min
    max_concurrent = [max_concurrent, opts[:max_concurrent]].min unless opts[:max_concurrent].nil?
    
    bsub_arguments = Array.new
    bsub_arguments.push('-P ' + opts[:project]) unless opts[:project].nil?
    bsub_arguments.push('-q ' + opts[:queue]) unless opts[:queue].nil?
    bsub_arguments.push("-J'#{opts[:jobname]}[1-" + self.length.to_s + ']%' + max_concurrent.to_s + "'")
    bsub_arguments.push("-o #{opts[:output]}.%J.%I") unless opts[:output].nil?
    bsub_arguments.push("-e #{opts[:error]}.%J.%I") unless opts[:error].nil?
    
    bsub_command = "bsub " + bsub_arguments.join(' ')
    full_command = "echo 'ruby #{opts[:working_dir]}/#{opts[:jobname]}-${LSB_JOBINDEX}.sh' | " + bsub_command
    
    if opts[:host].nil?
      system(full_command)
    else
      command_file = File.open('command_file.sh','w')
      command_file.puts(full_command)
      command_file.close
      
      system("scp #{opts[:jobname]}-*.sh #{opts[:username]}@#{opts[:host]}:#{opts[:working_dir]}/")
      system("scp command_file.sh #{opts[:username]}@#{opts[:host]}:#{opts[:working_dir]}/")
      system("ssh #{opts[:username]}@#{opts[:host]} 'cd #{opts[:working_dir]}; source #{opts[:working_dir]}/command_file.sh'")
    end
    
  end
end
 
if __FILE__ == $0
  [1,2,7].parallelize( :host => 'my_server',
                       :username => 'my_username',
                       :working_dir => '/farm/users/me/',
                       :queue => 'normal',
                       :project => 'my_project',
                       :output => 'output_para',
                       :jobname => 'para'
                     ) do |n|
    [n, "times", n, "is", n*n].join(' ')
  end
end
