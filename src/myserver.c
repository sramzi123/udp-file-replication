#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <libgen.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>

#define MAX_PACKET_SIZE 32748  // required
#define MAX_CLIENTS 10
#define MAX_PATH_LEN  512
#define HEADER_SIZE 20

// Packet types
#define DATA_PACKET 1
#define ACK_PACKET 2
#define ERROR_FILE_BUSY 20

// protocol header which has to be 20 bytes
typedef struct {
    uint32_t seq_num;
    uint32_t packet_type;    
    uint32_t file_path_len;  
    uint32_t data_len;      
    uint32_t checksum;  
} packet_header_t;

typedef struct {
    packet_header_t header;
    char data[MAX_PACKET_SIZE];
} packet_t;

// there could be out of order packets
#define MAX_OOO_PACKETS 100000

typedef struct {
    struct sockaddr_in client_addr;
    char file_path[MAX_PATH_LEN];
    FILE *file_ptr;
    uint32_t expected_seq;
    time_t last_activity;
    int active;

    packet_t **received_packets;
    uint32_t   max_seq_seen;
} client_session_t;

static int    server_port;
static int    drop_percentage;
static char   root_folder[MAX_PATH_LEN];
static client_session_t clients[MAX_CLIENTS];

void print_usage(char *prog);
void get_timestamp(char *buf, size_t sz);
uint32_t calculate_checksum(const char *data, size_t len);
int should_drop_packet(void);
void print_data_log(struct sockaddr_in *addr, uint32_t seq);
void print_ack_log(struct sockaddr_in *addr, uint32_t seq);
void print_drop_log(struct sockaddr_in *addr, uint32_t seq, const char *type);
client_session_t* find_or_create_session(int sock, struct sockaddr_in *addr, const char *path);
int send_ack(int fd, struct sockaddr_in *cli, uint32_t seq);
int verify_checksum(packet_t *pkt);
void cleanup_inactive(void);

// main function that runs the server
int main(int argc, char *argv[]) {
    if (argc != 4) {
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }
    server_port     = atoi(argv[1]);
    drop_percentage = atoi(argv[2]);
    strncpy(root_folder, argv[3], sizeof(root_folder) - 1);
    root_folder[sizeof(root_folder) - 1] = '\0';

    // normal checks
    if (server_port <= 0 || server_port > 65535) {
        fprintf(stderr, "Invalid port\n");
        return EXIT_FAILURE;
    }
    if (drop_percentage < 0 || drop_percentage > 100) {
        fprintf(stderr, "Drop must be 0–100\n");
        return EXIT_FAILURE;
    }

    // ensures root_folder exists
    if (mkdir(root_folder, 0755) < 0 && errno != EEXIST) {
        perror("mkdir root_folder");
        return EXIT_FAILURE;
    }

    srand((unsigned)time(NULL));
    memset(clients, 0, sizeof(clients));

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }
    // sets non-blocking
    int flags = fcntl(sock, F_GETFL, 0);
    if (fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("fcntl");
        close(sock);
        return EXIT_FAILURE;
    }

    struct sockaddr_in srv = {0};
    srv.sin_family      = AF_INET;
    srv.sin_addr.s_addr = INADDR_ANY;
    srv.sin_port        = htons(server_port);
    if (bind(sock, (struct sockaddr *)&srv, sizeof(srv)) < 0) {
        perror("bind");
        close(sock);
        return EXIT_FAILURE;
    }

    printf("Server on port %d, drop %% = %d, root = %s\n",
           server_port, drop_percentage, root_folder);

    while (1) {
        packet_t pkt;
        struct sockaddr_in cli;
        socklen_t len = sizeof(cli);

        ssize_t r = recvfrom(sock, &pkt, sizeof(pkt), 0,
                             (struct sockaddr *)&cli, &len);
        if (r < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                cleanup_inactive();
                usleep(1000);
                continue;
            }
            perror("recvfrom");
            continue;
        }

        // simulates drop when received
        if (should_drop_packet()) {
            uint32_t seq = ntohl(pkt.header.seq_num);
            uint32_t t   = ntohl(pkt.header.packet_type);
            print_drop_log(&cli, seq, (t == DATA_PACKET ? "DATA" : "ACK"));
            continue;
        }

        // has to have a header
        if (r < (ssize_t)sizeof(packet_header_t)) {
            fprintf(stderr, "pkt too small (%zd < %zu)\n", r, sizeof(packet_header_t));
            continue;
        }

        // only handle DATA from client
        uint32_t type = ntohl(pkt.header.packet_type);
        if (type != DATA_PACKET) {
            continue;
        }

        // extracts fields seq number, file path length, and data length
        uint32_t seq   = ntohl(pkt.header.seq_num);
        uint32_t fplen = ntohl(pkt.header.file_path_len);
        uint32_t dlen  = ntohl(pkt.header.data_len);

        // checks to see if payload doesn't exceed buffer
        if (fplen >= MAX_PATH_LEN) {
            fprintf(stderr, "path-too-long: %u ≥ %d\n", fplen, MAX_PATH_LEN);
            continue;
        }
        if (fplen + dlen > MAX_PACKET_SIZE) {
            fprintf(stderr, "packet payload+path (%u+%u) > %d\n", fplen, dlen, MAX_PACKET_SIZE);
            continue;
        }
        // checks if we have enough bytes
        if (r < (ssize_t)(HEADER_SIZE + fplen + dlen)) {
            fprintf(stderr, "truncated packet (only %zd bytes, header claims %u+%u)\n", r, fplen, dlen);
            continue;
        }

        print_data_log(&cli, seq);

        // verify checksum
        if (!verify_checksum(&pkt)) {
            fprintf(stderr, "checksum fail %u\n", seq);
            continue;
        }

        // extracts path
        char path[MAX_PATH_LEN];
        memcpy(path, pkt.data, fplen);
        path[fplen - 1] = '\0';

        if (strstr(path, "..") != NULL) {
            fprintf(stderr, "Illegal path (\"..\" not allowed): %s\n", path);
            packet_header_t eh = {0};
            eh.packet_type = htonl(ERROR_FILE_BUSY);
            sendto(sock, &eh, sizeof(eh), 0, (struct sockaddr *)&cli, sizeof(cli));
            continue;
        }

        char *cleanpath = path;
        if (cleanpath[0] == '/') cleanpath++;
        // find or create session
        client_session_t *c = find_or_create_session(sock, &cli, cleanpath);
        if (!c) { // already logged
            continue;
        }
        c->last_activity = time(NULL);

        // makes sure its in-order
        if (seq == c->expected_seq) {
            if (dlen) {
                fwrite(pkt.data + fplen, 1, dlen, c->file_ptr);
                fflush(c->file_ptr);
            }
            c->expected_seq++;

            while (c->received_packets &&
                   c->expected_seq <= c->max_seq_seen &&
                   c->received_packets[c->expected_seq]) {
                packet_t *bp = c->received_packets[c->expected_seq];
                uint32_t bd = ntohl(bp->header.data_len);
                if (bd) {
                    fwrite(bp->data + fplen, 1, bd, c->file_ptr);
                    fflush(c->file_ptr);
                }
                free(bp);
                c->received_packets[c->expected_seq] = NULL;
                c->expected_seq++;
            }
        }
        // out-of-order
        else if (seq > c->expected_seq) {
            if (!c->received_packets) {
                c->received_packets = calloc(MAX_OOO_PACKETS, sizeof(packet_t *));
                if (!c->received_packets) {
                    perror("calloc received_packets");
                    packet_header_t eh = {0};
                    eh.packet_type = htonl(EXIT_FAILURE);
                    sendto(sock, &eh, sizeof(eh), 0, (struct sockaddr *)&cli, sizeof(cli));
                    continue;
                }
            }
            if (seq < MAX_OOO_PACKETS && !c->received_packets[seq]) {
                size_t sz = HEADER_SIZE + fplen + dlen;
                packet_t *buf = malloc(sz);
                if (!buf) {
                    perror("malloc ooo packet");
                } else {
                    memcpy(buf, &pkt, sz);
                    c->received_packets[seq] = buf;
                    if (seq > c->max_seq_seen) {
                        c->max_seq_seen = seq;
                    }
                }
            }
        }
        send_ack(sock, &cli, c->expected_seq - 1);
        cleanup_inactive();
    }

    close(sock);
    return EXIT_SUCCESS;
}

// finds an existing ession or creates a new session. checks if path is being used, if so send an ERROR
// if there is conflict, return NULL or pointer to the sessiion if successful
client_session_t* find_or_create_session(int sock, struct sockaddr_in *cli, const char *path)
{
    // 1) looks for existing path using port, ip, path
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active
         && clients[i].client_addr.sin_addr.s_addr == cli->sin_addr.s_addr
         && clients[i].client_addr.sin_port        == cli->sin_port
         && strcmp(clients[i].file_path, path) == 0)
        {
            return &clients[i];
        }
    }

    // check if there is already an active session on path
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (!clients[i].active) continue;
        // if from same IP, skip
        if (clients[i].client_addr.sin_addr.s_addr == cli->sin_addr.s_addr
         && clients[i].client_addr.sin_port        == cli->sin_port)
        {
            continue;
        }
        // different client uses same path, reject
        if (strcmp(clients[i].file_path, path) == 0) {
            fprintf(stderr, "File in progress by another client: %s\n", path);
            // Immediately send an ERROR to the new client so it can exit(20).
            packet_header_t eh = {0};
            eh.packet_type = htonl(ERROR_FILE_BUSY);
            sendto(sock, &eh, sizeof(eh), 0, (struct sockaddr *)cli, sizeof(*cli));
            return NULL;
        }
    }

    // find a free slot
    int slot = -1;
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (!clients[i].active) {
            slot = i;
            break;
        }
    }
    if (slot < 0) {
        fprintf(stderr, "max clients\n");
        packet_header_t eh = {0};
        eh.packet_type = htonl(EXIT_FAILURE);
        sendto(sock, &eh, sizeof(eh), 0, (struct sockaddr *)cli, sizeof(*cli));
        return NULL;
    }

    // initialize client
    client_session_t *c = &clients[slot];
    memset(c, 0, sizeof(*c));
    c->client_addr   = *cli;
    strncpy(c->file_path, path, MAX_PATH_LEN - 1);
    c->file_path[MAX_PATH_LEN - 1] = '\0';
    c->expected_seq  = 0;
    c->last_activity = time(NULL);
    c->active        = 1;
    c->max_seq_seen  = 0;
    c->received_packets = NULL;

    char full[MAX_PATH_LEN * 2];
    if (strlen(root_folder) + 1 + strlen(path) >= sizeof(full)) {
        fprintf(stderr, "combined path too long: %s/%s\n", root_folder, path);
        c->active = 0;
        return NULL;
    }
    snprintf(full, sizeof(full), "%s/%s", root_folder, path);

    // create directories if needed
    char dirpath[MAX_PATH_LEN * 2];
    strncpy(dirpath, full, sizeof(dirpath));
    dirpath[sizeof(dirpath) - 1] = '\0';

    size_t base_len = strlen(root_folder);
    for (char *p = dirpath + base_len + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(dirpath, 0755) < 0 && errno != EEXIST) {
                perror("mkdir intermediate");
                c->active = 0;
                return NULL;
            }
            *p = '/';
        }
    }
    // mkdir on the parent directory
    char *last_slash = strrchr(full, '/');
    if (last_slash) {
        *last_slash = '\0';
        if (mkdir(full, 0755) < 0 && errno != EEXIST) {
            perror("mkdir parent");
            c->active = 0;
            return NULL;
        }
        *last_slash = '/';
    }

    // open file for writing
    c->file_ptr = fopen(full, "wb");
    if (!c->file_ptr) {
        perror("fopen");
        packet_header_t eh = {0};
        eh.packet_type = htonl(EXIT_FAILURE);
        sendto(sock, &eh, sizeof(eh), 0, (struct sockaddr *)&c->client_addr, sizeof(c->client_addr));
        c->active = 0;
        return NULL;
    }

    printf("New session: %s\n", path);
    c->last_activity = time(NULL);
    c->active        = 1;
    c->expected_seq  = 0;
    return c;
}

// simulates dropped packages
int should_drop_packet(void) {
    if (drop_percentage == 0) {
        return 0;
    }
    if (drop_percentage == 100) {
        return 1;
    }
    return ((rand() % 100) < drop_percentage);
}

// send ACK for 'seq' to client and simulate drop %
int send_ack(int fd, struct sockaddr_in *cli, uint32_t seq) {
    if (should_drop_packet()) {
        print_drop_log(cli, seq, "ACK");
        return 0;
    }
    packet_header_t h = {0};
    h.seq_num     = htonl(seq);
    h.packet_type = htonl(ACK_PACKET);
    ssize_t s = sendto(fd, &h, sizeof(h), 0, (struct sockaddr *)cli, sizeof(*cli));
    if (s < 0) {
        perror("sendto ACK");
        return -1;
    } else if (s != sizeof(h)) {
        fprintf(stderr, "sendto(ACK) truncated\n");
        return -1;
    }
    print_ack_log(cli, seq);
    return 0;
}

// return 1 if checksum matches, else 0
int verify_checksum(packet_t *pkt) {
    uint32_t cs    = ntohl(pkt->header.checksum);
    uint32_t fplen = ntohl(pkt->header.file_path_len);
    uint32_t dlen  = ntohl(pkt->header.data_len);
    uint32_t calc  = calculate_checksum(pkt->data, fplen + dlen);
    return (cs == calc);
}

// closes session that have been there for more than 60 seconds
void cleanup_inactive(void) {
    time_t now = time(NULL);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active &&
            (now - clients[i].last_activity) > 60)
        {
            printf("Cleanup session %s\n", clients[i].file_path);
            fclose(clients[i].file_ptr);
            if (clients[i].received_packets) {
                for (int j = 0; j < MAX_OOO_PACKETS; j++) {
                    if (clients[i].received_packets[j]) {
                        free(clients[i].received_packets[j]);
                    }
                }
                free(clients[i].received_packets);
            }
            clients[i].active = 0;
        }
    }
}

// logs DATA packet arrival
void print_data_log(struct sockaddr_in *addr, uint32_t seq) {
    char t[64]; get_timestamp(t, sizeof(t));
    char ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &addr->sin_addr, ip, sizeof(ip));
    printf("%s, %d, %s, %d, DATA, %u\n", t, server_port, ip, ntohs(addr->sin_port), seq);
    fflush(stdout);
}

// logs ACK packet
void print_ack_log(struct sockaddr_in *addr, uint32_t seq) {
    char t[64]; get_timestamp(t, sizeof(t));
    char ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &addr->sin_addr, ip, sizeof(ip));
    printf("%s, %d, %s, %d, ACK, %u\n", t, server_port, ip, ntohs(addr->sin_port), seq);
    fflush(stdout);
}

// logs a packet drop
void print_drop_log(struct sockaddr_in *addr, uint32_t seq, const char *type) {
    char t[64]; get_timestamp(t, sizeof(t));
    char ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &addr->sin_addr, ip, sizeof(ip));
    printf("%s, %d, %s, %d, DROP %s, %u\n", t, server_port, ip, ntohs(addr->sin_port), type, seq);
    fflush(stdout);
}

// gets timestamp
void get_timestamp(char *buf, size_t sz) {
    struct timeval tv; gettimeofday(&tv, NULL);
    struct tm tm_utc; gmtime_r(&tv.tv_sec, &tm_utc);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%S", &tm_utc);
    snprintf(buf + strlen(buf), sz - strlen(buf), ".%03ldZ", tv.tv_usec / 1000);
}
  
// calculates checksum
uint32_t calculate_checksum(const char *data, size_t len) {
    uint32_t sum = 0;
    for (size_t i = 0; i < len; i++) {
        sum += (unsigned char)data[i];
    }
    return sum;
}

// prints usage if needed
void print_usage(char *prog) {
    fprintf(stderr, "Usage: %s <port> <drop%%> <root_folder>\n", prog);
}
