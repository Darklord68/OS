# CampusCompute
Prerequisites: Ubuntu/WSL Ubuntu, gcc, make.
Build: make        Clean: make clean
Run: ./campus help | sched fcfs|rr|prio|all [file] | logger | ipc | banker safe|unsafe | worker normal|fail | rlimit | demo
Job file format: one job per line: id arrival burst priority
Cleanup check: ls /dev/shm | grep campus   (should print nothing); ps aux | grep campus
Notes: worker failures are SIMULATED. IPC objects are named with the program PID and removed by the program.
