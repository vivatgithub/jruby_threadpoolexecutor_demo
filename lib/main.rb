require 'java'

java_import 'java.util.concurrent.ThreadPoolExecutor'
java_import 'java.util.concurrent.TimeUnit'
java_import 'java.util.concurrent.LinkedBlockingQueue'
java_import 'java.util.concurrent.FutureTask'
java_import 'java.util.concurrent.Callable'

class Task
  include Java::JavaUtilConcurrent::Callable 
  
  def initialize(name)
    @taskname = name
  end
  
  def call
    puts "#{Time.now} : starting task #{@taskname}"
    sleep (rand() * 10).to_i + 5 #simulating processing time
    puts "#{Time.now} : completed task #{@taskname}"    
  end
end

def fetch_next_slice(source_arr, slice_num, size)
  puts "#{Time.now} : >==>==>==>==>==>==>==> fetching next slice #{slice_num}"
  sleep 15 #simulating delay
  temp_arr = source_arr.slice(slice_num * size, size)
  puts "#{Time.now} : <==<==<==<==<==<==<== obtained next slice #{slice_num}"
  temp_arr
end

ids = [*0..99]
fetch_size = 10

core_pool_size = 3
maximum_pool_size = 6
keep_alive_time_in_pool = 30

slice_num = 0
cur_slice = fetch_next_slice(ids, slice_num, fetch_size)

while (true)
  slice_num += 1
  
  next_fetch_thread = Thread.new {
    Thread.current['batch'] = fetch_next_slice(ids, slice_num, fetch_size)
  }
  
  puts "#{Time.now} : ------ starting slice #{slice_num} = #{cur_slice.inspect}"
  executor = ThreadPoolExecutor.new(
    core_pool_size, 
    maximum_pool_size, 
    keep_alive_time_in_pool, 
    TimeUnit::SECONDS, 
    LinkedBlockingQueue.new(fetch_size)
  )
  
  cur_slice.each {|s|
    ft = FutureTask.new(Task.new(s)) 
    executor.execute(ft)
  }
      
  puts "#{Time.now} : ------- awaiting termination of slice #{slice_num}"
  executor.shutdown()
  executor.await_termination(100, TimeUnit::SECONDS) 
  puts "#{Time.now} : ------- completed slice #{slice_num}"
  
  next_fetch_thread.join
  cur_slice = next_fetch_thread['batch']
  
  sleep 2  #give cpu some breathing in case all tasks get over
end

