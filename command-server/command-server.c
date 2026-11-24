// The command server of a sandbox. The client library connects to it straight over the pod
// network and runs one command per connection, instead of asking the Kubernetes API to exec.
//
// Both directions carry frames: one byte of type, four bytes of payload length (big-endian), then
// the payload. The client sends AUTH (the pod's token), ARGV (the arguments, each ended by a zero
// byte), any number of STDIN frames and one CLOSE. The server answers the token with ACCEPTED,
// before anything runs, so a client that reached the wrong pod knows nothing ran. It then sends
// STDOUT and STDERR frames as the command prints, and one EXIT whose payload is the exit code in
// decimal.
//
// A client that goes away does not stop its command, as with a Kubernetes exec: the command loses
// its output and is ended by whoever started it with a time limit.
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#define TOKEN_VARIABLE "GKE_SANDBOXES_COMMAND_TOKEN"
#define PORT_VARIABLE "GKE_SANDBOXES_COMMAND_PORT"

enum { STDIN_FRAME = 0, STDOUT_FRAME = 1, STDERR_FRAME = 2, EXIT_FRAME = 3, ACCEPTED_FRAME = 4 };
enum { AUTH_FRAME = 16, ARGV_FRAME = 17, CLOSE_FRAME = 255 };

// A client sends its stdin in pieces of 256 KiB.
#define MAX_PAYLOAD (1024 * 1024)
#define MAX_ARGUMENTS 256
#define OUTPUT_CHUNK (64 * 1024)
// How long a connection may take to say who it is and what to run.
#define HANDSHAKE_MS 10000

static int write_all(int fd, const unsigned char *data, size_t length) {
  while (length > 0) {
    ssize_t written = write(fd, data, length);
    if (written < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN) {
        struct pollfd ready = {.fd = fd, .events = POLLOUT};
        poll(&ready, 1, -1);
        continue;
      }
      return -1;
    }
    data += written;
    length -= (size_t)written;
  }
  return 0;
}

static int read_all(int fd, unsigned char *data, size_t length, int timeout_ms) {
  while (length > 0) {
    struct pollfd ready = {.fd = fd, .events = POLLIN};
    int polled = poll(&ready, 1, timeout_ms);
    if (polled < 0 && errno == EINTR) continue;
    if (polled <= 0) return -1;
    ssize_t count = read(fd, data, length);
    if (count < 0 && errno == EINTR) continue;
    if (count <= 0) return -1;
    data += count;
    length -= (size_t)count;
  }
  return 0;
}

static int send_frame(int fd, unsigned char type, const unsigned char *payload, uint32_t length) {
  unsigned char header[5] = {type, (unsigned char)(length >> 24), (unsigned char)(length >> 16),
                             (unsigned char)(length >> 8), (unsigned char)length};
  if (write_all(fd, header, sizeof header) < 0) return -1;
  return write_all(fd, payload, length);
}

// Reads one frame into `payload`, which holds MAX_PAYLOAD + 1 bytes. Returns its type, or -1.
static int read_frame(int fd, unsigned char *payload, uint32_t *length, int timeout_ms) {
  unsigned char header[5];
  if (read_all(fd, header, sizeof header, timeout_ms) < 0) return -1;
  *length = ((uint32_t)header[1] << 24) | ((uint32_t)header[2] << 16) | ((uint32_t)header[3] << 8) |
            (uint32_t)header[4];
  if (*length > MAX_PAYLOAD) return -1;
  if (read_all(fd, payload, *length, timeout_ms) < 0) return -1;
  return header[0];
}

// Takes the same time whatever the tokens share, so the time tells a guesser nothing.
static int same_token(const unsigned char *given, size_t given_length, const char *token) {
  size_t length = strlen(token);
  unsigned char difference = (unsigned char)(given_length != length);
  for (size_t index = 0; index < length; index++)
    difference |= (unsigned char)((index < given_length ? given[index] : 0) ^ (unsigned char)token[index]);
  return difference == 0;
}

static void serve(int client, const char *token) {
  static unsigned char payload[MAX_PAYLOAD + 1];
  static unsigned char output[OUTPUT_CHUNK];
  uint32_t length;

  if (read_frame(client, payload, &length, HANDSHAKE_MS) != AUTH_FRAME) return;
  if (!same_token(payload, length, token)) return;
  if (send_frame(client, ACCEPTED_FRAME, payload, 0) < 0) return;
  if (read_frame(client, payload, &length, HANDSHAKE_MS) != ARGV_FRAME) return;
  if (length == 0 || payload[length - 1] != 0) return;
  char *arguments[MAX_ARGUMENTS + 1];
  int count = 0;
  for (uint32_t offset = 0; offset < length; offset += (uint32_t)strlen((char *)payload + offset) + 1) {
    if (count == MAX_ARGUMENTS) return;
    arguments[count++] = (char *)payload + offset;
  }
  arguments[count] = NULL;

  int in[2], out[2], err[2];
  if (pipe(in) < 0 || pipe(out) < 0 || pipe(err) < 0) return;
  pid_t child = fork();
  if (child < 0) return;
  if (child == 0) {
    // A group of its own, as a command started by the container runtime has.
    setpgid(0, 0);
    dup2(in[0], 0);
    dup2(out[1], 1);
    dup2(err[1], 2);
    for (int fd = 3; fd < 64; fd++) close(fd);
    // Ignored signals stay ignored in the program that is started.
    signal(SIGPIPE, SIG_DFL);
    execvp(arguments[0], arguments);
    dprintf(2, "Could not run %s: %s\n", arguments[0], strerror(errno));
    _exit(127);
  }
  close(in[0]);
  close(out[1]);
  close(err[1]);
  // The command's stdin must never block this loop: while it does not read, its output still has
  // to be carried.
  fcntl(in[1], F_SETFL, fcntl(in[1], F_GETFL) | O_NONBLOCK);

  int stdin_fd = in[1];
  int streams[2] = {out[0], err[0]};
  // The part of the last STDIN frame the command has not taken yet.
  uint32_t pending = 0, taken = 0;
  while (streams[0] >= 0 || streams[1] >= 0) {
    struct pollfd fds[4];
    // Nothing more is read from the client while a frame waits for the command.
    fds[0] = (struct pollfd){.fd = stdin_fd >= 0 && !pending ? client : -1, .events = POLLIN};
    fds[1] = (struct pollfd){.fd = pending ? stdin_fd : -1, .events = POLLOUT};
    fds[2] = (struct pollfd){.fd = streams[0], .events = POLLIN};
    fds[3] = (struct pollfd){.fd = streams[1], .events = POLLIN};
    if (poll(fds, 4, -1) < 0) {
      if (errno == EINTR) continue;
      _exit(0);
    }
    if (fds[0].revents) {
      int type = read_frame(client, payload, &length, HANDSHAKE_MS);
      // The client left. Its command runs on without it.
      if (type < 0) _exit(0);
      if (type == STDIN_FRAME) {
        pending = length;
        taken = 0;
      } else if (type == CLOSE_FRAME) {
        close(stdin_fd);
        stdin_fd = -1;
      }
    }
    if (fds[1].revents) {
      ssize_t written = write(stdin_fd, payload + taken, pending - taken);
      if (written > 0) taken += (uint32_t)written;
      // A command that has closed its stdin gets no more of it.
      if (written < 0 && errno != EAGAIN && errno != EINTR) taken = pending;
      if (taken == pending) pending = 0;
    }
    for (int index = 0; index < 2; index++) {
      if (!fds[2 + index].revents) continue;
      ssize_t size = read(streams[index], output, sizeof output);
      if (size < 0 && (errno == EINTR || errno == EAGAIN)) continue;
      if (size <= 0) {
        close(streams[index]);
        streams[index] = -1;
        continue;
      }
      if (send_frame(client, index == 0 ? STDOUT_FRAME : STDERR_FRAME, output, (uint32_t)size) < 0)
        _exit(0);
    }
  }
  if (stdin_fd >= 0) close(stdin_fd);
  int status = 0;
  while (waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  char code[16];
  int written = snprintf(code, sizeof code, "%d",
                         WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status));
  send_frame(client, EXIT_FRAME, (unsigned char *)code, (uint32_t)written);
}

int main(void) {
  const char *configured = getenv(TOKEN_VARIABLE);
  const char *port = getenv(PORT_VARIABLE);
  if (!configured || strlen(configured) < 32 || !port) {
    dprintf(2, "%s (32 characters or more) and %s are required.\n", TOKEN_VARIABLE, PORT_VARIABLE);
    return 2;
  }
  char *token = strdup(configured);
  // The commands it runs have no use for the token.
  unsetenv(TOKEN_VARIABLE);
  unsetenv(PORT_VARIABLE);

  int listener = socket(AF_INET, SOCK_STREAM, 0);
  int on = 1;
  setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &on, sizeof on);
  struct sockaddr_in address = {.sin_family = AF_INET,
                                .sin_port = htons((uint16_t)atoi(port)),
                                .sin_addr = {.s_addr = htonl(INADDR_ANY)}};
  if (listener < 0 || bind(listener, (struct sockaddr *)&address, sizeof address) < 0 ||
      listen(listener, 128) < 0) {
    dprintf(2, "Could not listen on port %s: %s\n", port, strerror(errno));
    return 1;
  }
  // The kernel reaps the connection handlers.
  signal(SIGCHLD, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  while (1) {
    int client = accept(listener, NULL, NULL);
    if (client < 0) {
      if (errno == EINTR || errno == ECONNABORTED) continue;
      // Out of descriptors or memory for a moment.
      usleep(50000);
      continue;
    }
    pid_t handler = fork();
    if (handler == 0) {
      close(listener);
      // The handler waits for its own command.
      signal(SIGCHLD, SIG_DFL);
      setsockopt(client, IPPROTO_TCP, TCP_NODELAY, &on, sizeof on);
      serve(client, token);
      _exit(0);
    }
    close(client);
  }
}
