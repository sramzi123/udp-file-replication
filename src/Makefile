# Makefile for CSE156/L Programming Lab 4
# Reliable File Transfer with UDP

# Compiler and flags
CC = gcc
CFLAGS = -Wall -Wextra -std=gnu99 -pthread -g
LDFLAGS = -pthread

# Directories
SRCDIR = src
BINDIR = bin
DOCDIR = doc

# Source files
CLIENT_SRC = $(SRCDIR)/myclient.c
SERVER_SRC = $(SRCDIR)/myserver.c

# Object files
CLIENT_OBJ = $(CLIENT_SRC:.c=.o)
SERVER_OBJ = $(SERVER_SRC:.c=.o)

# Executables
CLIENT_EXEC = $(BINDIR)/myclient
SERVER_EXEC = $(BINDIR)/myserver

# Default target
all: $(CLIENT_EXEC) $(SERVER_EXEC)

# Build client
$(CLIENT_EXEC): $(CLIENT_OBJ)
	$(CC) $(CLIENT_OBJ) -o $@ $(LDFLAGS)

# Build server
$(SERVER_EXEC): $(SERVER_OBJ)
	$(CC) $(SERVER_OBJ) -o $@ $(LDFLAGS)

# Compile source files
$(SRCDIR)/%.o: $(SRCDIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

# Clean build artifacts
clean:
	rm -f $(SRCDIR)/*.o
	rm -f $(CLIENT_EXEC) $(SERVER_EXEC)

# Phony targets
.PHONY: all clean