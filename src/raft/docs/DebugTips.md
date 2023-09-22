# You can just pipe the go test output into the script
$ VERBOSE=1 go test -run InitialElection | python3 dslogs
# ... colored output will be printed

# We can ignore verbose topics like timers or log changes
$ VERBOSE=1 go test -run Backup | python3 dslogs -c 5 -i TIMR,DROP,LOG2
# ... colored output in 5 columns

# Dumping output to a file can be handy to iteratively
# filter topics and when failures are hard to reproduce
$ VERBOSE=1 go test -run Figure8Unreliable > output.log
# Print from a file, selecting just two topics
$ python3 dslogs output.log -j CMIT,PERS
# ... colored output

# Run test 2A 10 times with VERBOSE setting to 1
$ python3 dstest.py 2A -n 10 -v 1