# lari

# lariserver.js (Entrypoint)


## lariapi.js (handle_api2)
Requires:

- laricontainermanager.js
- lariprocesscache.js
- docstorclient.js   


## lariclient.js


## docstor.js

## Container Stats
Supports **get-stats** api:
```
.platform   # return os/arch
.sysUptime  # returns system uptime in secs (uses os module)

.cpuCount   # return num cpus (uses os module)
.cpuFree    # fraction cpu free (uses os module)
.cpuUsage   # 1 - cpuFree (uses os module)

.freemem    # return free memory (GB) (uses os module)
.totalmem   # return total memory (GB) (uses os module)
.freememPercentage # return % memory free
.freeCommand       # return used memory (as reported by `free` - linux only)

.harddrive     # return total, free and used memory on HDD as reported by `df -k`
.getProcesses  # get all processes and their cpu, memory usage + uptime + arguments (as reported by `ps`)
```
