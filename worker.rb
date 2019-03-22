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

db.prepare("fetch_jobs", %Q{
  UPDATE ONLY jobs 
     SET state = 'working'
   WHERE jid IN (
                SELECT jid
                  FROM  ONLY jobs
                 WHERE state = 'scheduled' AND at <= now()
              ORDER BY at DESC, priority DESC
                       FOR UPDATE SKIP LOCKED
                 LIMIT $1
    )
  RETURNING *
})

db.prepare("complete_jobs", %Q{
  UPDATE ONLY jobs
     SET state = 'done',
         completed_at = now(),
         worker_id = $1
   WHERE jid = $2
})

loop do 
  db.transaction do |conn|
    jobs = conn.exec_prepared("fetch_jobs", [10])
    jobs.each do |job|
      done_job = conn.exec_prepared("complete_jobs", [current_worker.id, job['jid']])
      puts "Done Job: #{job['jid']}"
    end
  end
end

puts 'done'