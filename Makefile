CC=gcc
CFLAGS=-Wall -Wextra -O2 -pthread
all: campus
campus: campus.c
	$(CC) $(CFLAGS) -o campus campus.c -lrt
clean:
	rm -f campus campus.log
