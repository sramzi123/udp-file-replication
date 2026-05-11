#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdint.h>
#include <inttypes.h>

#define MAX_SERVERS 10
#define MAX_RETRIES 5
#define TIMEOUT_SEC 3
#define SERVER_TIMEOUT_SEC 30
#define MAX_PACKET_SIZE 32768
#define HEADER_SIZE 20
#define MIN_MSS HEADER_SIZE

#define DATA_PACKET 1
#define ACK_PACKET 2

#define EXIT_SUCCESS 0
#define EXIT_MSS_TOO_SMALL 1
#define EXIT_SERVER_DOWN 3
#define EXIT_MAX_RETRIES 4
#define EXIT_FILE_IN_PROGRESS 20

// the packet header
typedef struct {
    uint32_t seq_num;
    uint32_t packet_type;
    uint32_t file_path_len;
    uint32_t data_len;
    uint32_t checksum;
} packet_header_t;

typedef struct {
    packet_header_t header;
    char data[];
} packet_t;

// sliding window state for each server
typedef struct {
    uint32_t base_seq;
    uint32_t next_seq;
    uint32_t window_size;
    packet_t **packets;
    struct timeval *send_times;
    int *retry_counts;
    pthread_mutex_t lock;
} window_t;

typedef struct {
    char ip[16];
    int port;
    int sock_fd;
    struct sockaddr_in addr;
} server_info_t;

typedef struct {
    server_info_t server;
    window_t window;
    char *file_data;
    size_t file_size;
    char *out_path;
    int mss;
    uint32_t total_packets;
    int local_port;
} thread_arg_t;

void print_usage(char *prog) {
    fprintf(stderr, "Usage: %s <servn> <servaddr.conf> <mss> <winsz> <in> <out>\n", prog);
}

// calculates checksum
uint32_t calculate_checksum(const char *data, size_t len) {
    uint32_t sum = 0;
    for (size_t i = 0; i < len; i++) {
        sum += (unsigned char)data[i];
    }
    return sum;
}

// writes the current time
void get_timestamp(char *buf, size_t sz) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    struct tm tm_info;
    gmtime_r(&tv.tv_sec, &tm_info);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%S", &tm_info);
    snprintf(buf + strlen(buf), sz - strlen(buf), ".%03ldZ", tv.tv_usec / 1000);
}

// logs a DATA packet to send
void print_data_log(int local_port, server_info_t *s, uint32_t seq, window_t *w) {
    char ts[64];
    get_timestamp(ts, sizeof(ts));
    printf("%s, %d, %s, %d, DATA, %u, %u, %u, %u\n",
           ts, local_port, s->ip, s->port,
           seq, w->base_seq, w->next_seq, w->base_seq + w->window_size);
    fflush(stdout);
}

// logs an ACK packet to receive
void print_ack_log(int local_port, server_info_t *s, uint32_t ack, window_t *w) {
    char ts[64];
    get_timestamp(ts, sizeof(ts));
    printf("%s, %d, %s, %d, ACK, %u, %u, %u, %u\n", ts , local_port, s->ip, s->port, ack, w->base_seq, w->next_seq, w->base_seq + w->window_size);
    fflush(stdout);
}

// create a DATA packet with sequence 'seq', pathname 'path', and 'dlen' bytes of 'data'
int create_packet(packet_t **out_pkt, uint32_t seq, const char *path, const char *data, size_t dlen) {
    uint32_t pathlen = strlen(path) + 1;
    size_t total = HEADER_SIZE + pathlen + dlen;
    packet_t *pkt = malloc(total);
    if (!pkt) {
        return -1;
    }
    memset(pkt, 0, total);
    
    // get network byte order of each parameter
    pkt->header.seq_num = htonl(seq);
    pkt->header.packet_type = htonl(DATA_PACKET);
    pkt->header.file_path_len = htonl(pathlen);
    pkt->header.data_len = htonl(dlen);
    memcpy(pkt->data, path, pathlen);
    if (dlen) {
        memcpy(pkt->data + pathlen, data, dlen);
    }
    uint32_t csum = calculate_checksum(pkt->data, pathlen + dlen);
    pkt->header.checksum = htonl(csum);

    *out_pkt = pkt;
    return 0;
}

// receive an ACK packet; returns 0 on success, -1 on failure
int recv_ack(int fd, uint32_t *ack) {
    packet_header_t hdr;
    ssize_t r = recvfrom(fd, &hdr, sizeof(hdr), 0, NULL, NULL);
    if (r < (ssize_t)sizeof(hdr)) {
        return -1;
    }
    if (ntohl(hdr.packet_type) != ACK_PACKET) {
        return -1;
    }
    *ack = ntohl(hdr.seq_num);
    return 0;
}

// initializes workers, also implementation of locking and unlocking mutexes
//
void *worker(void *arg) {
    thread_arg_t *t = (thread_arg_t *)arg;
    server_info_t *s = &t->server;
    window_t *w = &t->window;
    uint32_t N = t->total_packets;
    size_t payload_size = t->mss - HEADER_SIZE;
    time_t last_ack_time = time(NULL);

    pthread_mutex_lock(&w->lock);
    while (w->next_seq < N && w->next_seq < w->base_seq + w->window_size) {
        uint32_t seq = w->next_seq;
        size_t offset = seq * payload_size;
        size_t dlen = (offset + payload_size > t->file_size) ? (t->file_size - offset) : payload_size;

        if (create_packet(&w->packets[seq], seq, t->out_path, t->file_data + offset, dlen) < 0) {
            fprintf(stderr, "malloc() failed in create_packet\n");
            pthread_mutex_unlock(&w->lock);
            pthread_exit((void *)(intptr_t)EXIT_FAILURE);
        }

        uint32_t pathlen = ntohl(w->packets[seq]->header.file_path_len);
        size_t sendlen = HEADER_SIZE + pathlen + dlen;
        if (sendto(s->sock_fd, w->packets[seq], sendlen, 0,
                   (struct sockaddr *)&s->addr, sizeof(s->addr)) < 0) {
            perror("sendto");
            pthread_mutex_unlock(&w->lock);
            pthread_exit((void *)(intptr_t)EXIT_FAILURE);
        }

        gettimeofday(&w->send_times[seq], NULL);
        w->retry_counts[seq] = 0;
        print_data_log(t->local_port, s, seq, w);
        w->next_seq++;
    }
    pthread_mutex_unlock(&w->lock);

    fd_set rfds;
    struct timeval tv;

    while (1) {
        pthread_mutex_lock(&w->lock);
        if (w->base_seq >= N) {
            pthread_mutex_unlock(&w->lock);
            break;  // all packets are acked
        }
        pthread_mutex_unlock(&w->lock);

        // checks the server-down timeout
        if (time(NULL) - last_ack_time > SERVER_TIMEOUT_SEC) {
            fprintf(stderr, "Cannot detect server IP %s port %d\n", s->ip, s->port);
            pthread_exit((void *)(intptr_t)EXIT_SERVER_DOWN);
        }

        // waits 100ms for an ack
        FD_ZERO(&rfds);
        FD_SET(s->sock_fd, &rfds);
        tv.tv_sec = 0;
        tv.tv_usec = 100000;
        int rv = select(s->sock_fd + 1, &rfds, NULL, NULL, &tv);
        if (rv > 0 && FD_ISSET(s->sock_fd, &rfds)) {
            uint32_t ack;
            if (recv_ack(s->sock_fd, &ack) == 0) {
                last_ack_time = time(NULL);
                pthread_mutex_lock(&w->lock);
                if (ack >= w->base_seq && ack < w->next_seq) {
                    w->base_seq = ack + 1;
                    print_ack_log(t->local_port, s, ack, w);
                    // new packets if any remain
                    while (w->next_seq < N && w->next_seq < w->base_seq + w->window_size) {
                        uint32_t seq = w->next_seq;
                        size_t offset = seq * payload_size;
                        size_t dlen = (offset + payload_size > t->file_size)
                                          ? (t->file_size - offset)
                                          : payload_size;
                        if (!w->packets[seq]) {
                            if (create_packet(&w->packets[seq], seq, t->out_path, t->file_data + offset, dlen) < 0) {
                                fprintf(stderr, "malloc() failed in create_packet");
                                continue;
                            }
                        }
                        uint32_t pathlen = ntohl(w->packets[seq]->header.file_path_len);
                        size_t sendlen = HEADER_SIZE + pathlen + dlen;
                        if (sendto(s->sock_fd, w->packets[seq], sendlen, 0,
                                   (struct sockaddr *)&s->addr, sizeof(s->addr)) < 0) {
                            perror("sendto");
                        }

                        gettimeofday(&w->send_times[seq], NULL);
                        print_data_log(t->local_port, s, seq, w);
                        w->next_seq++;
                    }
                }
                pthread_mutex_unlock(&w->lock);
            }
        }

        // retransmission
        struct timeval now;
        gettimeofday(&now, NULL);
        pthread_mutex_lock(&w->lock);
        for (uint32_t seq = w->base_seq; seq < w->next_seq; seq++) {
            double elapsed = (now.tv_sec - w->send_times[seq].tv_sec) +
                             (now.tv_usec - w->send_times[seq].tv_usec) / 1e6;
            if (elapsed >= TIMEOUT_SEC) {
                if (w->retry_counts[seq] >= MAX_RETRIES) {
                    fprintf(stderr, "Reached max re-transmission limit IP %s\n", s->ip);
                    pthread_mutex_unlock(&w->lock);
                    pthread_exit((void *)(intptr_t)EXIT_MAX_RETRIES);
                }
                // retransmit packets here
                uint32_t pathlen = ntohl(w->packets[seq]->header.file_path_len);
                uint32_t dlen = ntohl(w->packets[seq]->header.data_len);
                size_t sendlen = HEADER_SIZE + pathlen + dlen;
                if (sendto(s->sock_fd, w->packets[seq], sendlen, 0,
                           (struct sockaddr *)&s->addr, sizeof(s->addr)) < 0) {
                    perror("sendto");
                    // continues on and counts as a retrnasmission attempt
                }
                gettimeofday(&w->send_times[seq], NULL);
                w->retry_counts[seq]++;
                fprintf(stderr, "Packet loss detected\n");
                print_data_log(t->local_port, s, seq, w);
            }
        }
        pthread_mutex_unlock(&w->lock);

        usleep(10000);
    }

    // cleanup
    pthread_mutex_lock(&w->lock);
    for (uint32_t i = 0; i < N; i++) {
        if (w->packets[i]) {
            free(w->packets[i]);
        }
    }
    pthread_mutex_unlock(&w->lock);
    free(w->packets);
    free(w->send_times);
    free(w->retry_counts);

    close(s->sock_fd);
    pthread_exit((void *)(intptr_t)EXIT_SUCCESS);
}

int main(int argc, char *argv[]) {
    if (argc != 7) {
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    int servn = atoi(argv[1]);
    char *conf = argv[2];
    int mss = atoi(argv[3]);
    int winsz = atoi(argv[4]);
    char *in = argv[5];
    char *out = argv[6];

    if (mss < MIN_MSS) {
        fprintf(stderr, "Required minimum MSS is %d\n", MIN_MSS);
        return EXIT_MSS_TOO_SMALL;
    }

    if (servn < 1 || servn > MAX_SERVERS) {
        fprintf(stderr, "servn must be between 1 and %d\n", MAX_SERVERS);
        return EXIT_FAILURE;
    }

    // Read server config
    server_info_t addrs[MAX_SERVERS];
    int count = 0;
    FILE *fp = fopen(conf, "r");
    if (!fp) {
        perror("config");
        return EXIT_FAILURE;
    }
    char line[256];
    while (fgets(line, sizeof(line), fp) && count < servn) {
        if (line[0] == '#' || line[0] == '\n') continue;
        if (sscanf(line, "%15s %d", addrs[count].ip, &addrs[count].port) != 2) {
            continue;
        }
        count++;
    }
    fclose(fp);
    if (count < servn) {
        fprintf(stderr, "Not enough servers in config\n");
        return EXIT_FAILURE;
    }

    // Load input file
    struct stat st;
    if (stat(in, &st) != 0) {
        perror("stat");
        return EXIT_FAILURE;
    }
    size_t fsz = st.st_size;
    char *data = NULL;
    if (fsz > 0) {
        data = malloc(fsz);
        if (!data) {
            perror("malloc");
            return EXIT_FAILURE;
        }
        FILE *f = fopen(in, "rb");
        if (!f) {
            perror("open");
            free(data);
            return EXIT_FAILURE;
        }
        size_t r = fread(data, 1, fsz, f);
        if (r != fsz) {
            perror("fread");
            fclose(f);
            free(data);
            return EXIT_FAILURE;
        }
        fclose(f);
    }

    uint32_t total_packets = 0;
    size_t payload_size = mss - HEADER_SIZE;
    if (fsz == 0) {
        total_packets = 1;
    } else {
        total_packets = (fsz + payload_size - 1) / payload_size;
    }

    // creates one thread per server
    pthread_t th[MAX_SERVERS];
    thread_arg_t *targs[MAX_SERVERS];

    for (int i = 0; i < servn; i++) {
        thread_arg_t *t = calloc(1, sizeof(thread_arg_t));
        if (!t) {
            perror("calloc");
            for (int j = 0; j < i; j++) {
                free(targs[j]);
            }
            free(data);
            return EXIT_FAILURE;
        }
        targs[i] = t;

        // copies server IP and port
        strncpy(t->server.ip, addrs[i].ip, sizeof(t->server.ip));
        t->server.port = addrs[i].port;

        t->file_data = data;
        t->file_size = fsz;
        t->out_path = out;
        t->mss = mss;
        t->total_packets = total_packets;

        // this initializes window state
        t->window.base_seq = 0;
        t->window.next_seq = 0;
        t->window.window_size = winsz;
        if (pthread_mutex_init(&t->window.lock, NULL) != 0) {
            perror("pthread_mutex_init");
            free(t);
            for (int j = 0; j < i; j++) {
                free(targs[j]);
            }
            free(data);
            return EXIT_FAILURE;
        }

        t->window.packets = calloc(total_packets, sizeof(packet_t *));
        t->window.send_times = calloc(total_packets, sizeof(struct timeval));
        t->window.retry_counts = calloc(total_packets, sizeof(int));
        if (!t->window.packets || !t->window.send_times || !t->window.retry_counts) {
            perror("calloc");
            free(t->window.packets);
            free(t->window.send_times);
            free(t->window.retry_counts);
            free(t);
            for (int j = 0; j < i; j++) {
                free(targs[j]);
            }
            free(data);
            return EXIT_FAILURE;
        }

        // creates UDP socket using socket()
        t->server.sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
        if (t->server.sock_fd < 0) {
            perror("socket");
            for (int j = 0; j <= i; j++) {
                free(targs[j]);
            }
            free(data);
            return EXIT_FAILURE;
        }

        // binds to a port using bind()
        struct sockaddr_in laddr;
        memset(&laddr, 0, sizeof(laddr));
        laddr.sin_family = AF_INET;
        laddr.sin_addr.s_addr = INADDR_ANY;
        laddr.sin_port = 0;
        if (bind(t->server.sock_fd, (struct sockaddr *)&laddr, sizeof(laddr)) < 0) {
            perror("bind");
            close(t->server.sock_fd);
            for (int j = 0; j <= i; j++) {
                free(targs[j]);
            }
            free(data);
            return EXIT_FAILURE;
        }
        socklen_t len = sizeof(laddr);
        if (getsockname(t->server.sock_fd, (struct sockaddr *)&laddr, &len) < 0) {
            perror("getsockname");
            close(t->server.sock_fd);
            for (int j = 0; j <= i; j++) {
                free(targs[j]);
            }
            free(data);
            return EXIT_FAILURE;
        }
        t->local_port = ntohs(laddr.sin_port);

        // fill in server addr_in
        memset(&t->server.addr, 0, sizeof(t->server.addr));
        t->server.addr.sin_family = AF_INET;
        t->server.addr.sin_port = htons(t->server.port);
        if (inet_pton(AF_INET, t->server.ip, &t->server.addr.sin_addr) != 1) {
            fprintf(stderr, "inet_pton failed for IP %s\n", t->server.ip);
            close(t->server.sock_fd);
            for (int j = 0; j <= i; j++) {
                free(targs[j]);
            }
            free(data);
            return EXIT_FAILURE;
        }

        // start the worker thread
        if (pthread_create(&th[i], NULL, worker, t) != 0) {
            perror("pthread_create");
            close(t->server.sock_fd);
            for (int j = 0; j <= i; j++) {
                free(targs[j]);
            }
            free(data);
            return EXIT_FAILURE;
        }
    }

    // join all the threads together
    int final_code = EXIT_SUCCESS;
    for (int i = 0; i < servn; i++) {
        void *ret;
        pthread_join(th[i], &ret);
        int code = (int)(intptr_t)ret;
        if (code != EXIT_SUCCESS) {
            // if any thread times out, it is prioritized as exit 3
            if (code == EXIT_SERVER_DOWN) {
                final_code = EXIT_SERVER_DOWN;
            }
            // else if any thread hit max retransmissions, and no timeout previously recorded
            else if (code == EXIT_MAX_RETRIES && final_code == EXIT_SUCCESS) {
                final_code = EXIT_MAX_RETRIES;
            }
            // otherwise, record failure if none recorded yet
            else if (final_code == EXIT_SUCCESS) {
                final_code = code;
            }
        }
        // clean up memory
        free(targs[i]);
    }

    free(data);
    return final_code;
}
