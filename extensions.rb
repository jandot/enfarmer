module Enumerable
  def parallelize(opts = {}, &block)
    counter = 1
    self.each do |a|
      tmp_file = File.open('/users/ja8/parallelize-' + counter.to_s + '.sh','w')
      tmp_file.puts('puts("' + opts[:queue] + ' ' + opts[:project] + ' ' + block.call(a).to_s + '")')
      tmp_file.chmod(0755)
      tmp_file.close
      counter += 1
    end
    
    system("echo 'ruby /users/ja8/parallelize-${LSB_JOBINDEX}.sh' |  bsub -J'parallel[1-" + self.length.to_s + ']%' +     self.length.to_s + "' -Pmy_project -o x.%J.%I")
  end
end
 
if __FILE__ == $0
  [1,2,7].parallelize( :queue => 'normal',
                       :project => 'my_project') do |n|
    [n, "times", n, "is", n*n].join(' ')
  end
end
