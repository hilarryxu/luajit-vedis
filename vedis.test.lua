local cwtest = require'cwtest'
local vedis = require'vedis'

local T = cwtest.new()

T:start('GET SET sample'); do
    local db = vedis.open()
    db:exec('SET name jim')
    db:exec('GET name')
    local val = db:exec_result()
    T:eq(val:to_string(), 'jim')
    db:close()
end; T:done()
