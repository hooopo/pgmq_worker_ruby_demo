require 'socket'
require 'dotenv/load'
require 'pry'
require 'pg'
require 'mini_sql'
require 'virtus'

class Worker
  include Virtus.model
  attribute :id, Integer
  attribute :pid, Integer
  attribute :hostname, String
  attribute :v, String
  attribute :labels, Array[String]
  attribute :started_at, DateTime
  attribute :last_active_at, DateTime
end

db = PG.connect(ENV['PGMQ_URL'])
db.exec 'set search_path to pgmq'
# only use mimi_sql for type_map
mini_sql = MiniSql::Connection.get(db)

db.type_map_for_results = mini_sql.type_map

pid            = Process.pid
hostname       = Socket.gethostname
v              = '0.0.1'
labels         = '{ruby, pgmq_worker_ruby_demo}'
started_at     = Time.now.utc
last_active_at = Time.now.utc

db.prepare("create_worker", %Q{
  insert into workers (hostname, pid, v, labels, started_at, last_active_at) 
       values ($1, $2, $3, $4, $5, $6) 
  on conflict (pid)
              do update set last_active_at = EXCLUDED.last_active_at, 
                            hostname = EXCLUDED.hostname, 
                            v = EXCLUDED.v,
                            labels = EXCLUDED.labels,
                            started_at = EXCLUDED.started_at
    returning *
})
result = db.exec_prepared("create_worker", [hostname, pid, v, labels, started_at, last_active_at])[0]
current_worker = Worker.new(result)

puts "current worker: #{current_worker.attributes.inspect}"

binding.pry

puts 'done'