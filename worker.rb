require 'socket'
require 'dotenv/load'
require 'pry'
require 'pg'
require 'mini_sql'

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

db.prepare("create_worker", "insert into workers (hostname, pid, v, labels, started_at, last_active_at) values ($1, $2, $3, $4, $5, $6 returning *")
current_worker = db.exec_prepared("create_worker", [hostname, pid, v, labels, started_at, last_active_at])[0]


binding.pry

puts 'done'