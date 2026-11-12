/* CampusCompute - OS Level 3 Mini-Project. Build: make. Run: ./campus help */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/resource.h>

#define MAXJ 32
#define QUANTUM 2

/* ---------- FR1: jobs ---------- */
typedef struct { int id, arrival, burst, priority, rem, start, ct, tat, wt; const char *status; } Job;

static Job sample[] = {
    {1,0,5,2},{2,1,3,1},{3,2,8,4},{4,3,2,3},{5,4,4,1}};
static int sample_n = 5;

static int load_jobs(const char *file, Job *out) {
    if (!file) { memcpy(out, sample, sizeof(sample)); for (int i=0;i<sample_n;i++){out[i].rem=out[i].burst;out[i].status="NEW";} return sample_n; }
    FILE *f = fopen(file, "r");
    if (!f) { fprintf(stderr, "Error: cannot open '%s': %s\n", file, strerror(errno)); return -1; }
    int n = 0;
    while (n < MAXJ) {
        Job j = {0};
        int r = fscanf(f, "%d %d %d %d", &j.id, &j.arrival, &j.burst, &j.priority);
        if (r == EOF) break;
        if (r != 4 || j.id <= 0 || j.arrival < 0 || j.burst <= 0 || j.priority < 0) {
            fprintf(stderr, "Error: malformed job line (need: id arrival burst priority, positive burst)\n"); fclose(f); return -1; }
        j.rem = j.burst; j.status = "NEW"; out[n++] = j;
    }
    fclose(f);
    if (n == 0) { fprintf(stderr, "Error: no jobs in file\n"); return -1; }
    return n;
}

/* ---------- FR2: scheduling ---------- */
/* Tie-break everywhere: earlier arrival first, then lower job ID. Priority: lower number = higher priority. */
static int cmp_arr(const void *a, const void *b) {
    const Job *x=a,*y=b;
    if (x->arrival!=y->arrival) return x->arrival-y->arrival;
    return x->id-y->id;
}
static void seg(int *t, int id, int s, int e) { (void)t; printf("[%d-%d] J%d  ", s, e, id); }

static void report(const char *name, Job *j, int n, double *avgwt, double *avgtat) {
    double sw=0, st=0;
    printf("\n%-4s %-8s %-6s %-5s %-11s %-11s %-8s\n","ID","Arrival","Burst","Prio","Completion","Turnaround","Waiting");
    for (int i=0;i<n;i++){
        j[i].tat=j[i].ct-j[i].arrival; j[i].wt=j[i].tat-j[i].burst; j[i].status="DONE";
        printf("J%-3d %-8d %-6d %-5d %-11d %-11d %-8d\n",j[i].id,j[i].arrival,j[i].burst,j[i].priority,j[i].ct,j[i].tat,j[i].wt);
        sw+=j[i].wt; st+=j[i].tat; }
    *avgwt=sw/n; *avgtat=st/n;
    printf("%s averages: turnaround=%.2f waiting=%.2f\n", name, *avgtat, *avgwt);
}

static void run_fcfs(Job *j, int n, double *w, double *t_) {
    qsort(j,n,sizeof(Job),cmp_arr);
    printf("\n== FCFS (tie-break: arrival, then lower ID) ==\nTimeline: ");
    int t=0;
    for (int i=0;i<n;i++){
        if (t<j[i].arrival) t=j[i].arrival;
        seg(&t,j[i].id,t,t+j[i].burst); t+=j[i].burst; j[i].ct=t; }
    printf("\n"); report("FCFS",j,n,w,t_);
}

static void run_rr(Job *j, int n, double *w, double *t_) {
    qsort(j,n,sizeof(Job),cmp_arr);
    printf("\n== Round Robin (quantum=%d, tie-break: arrival, then lower ID) ==\nTimeline: ", QUANTUM);
    int q[1024], head=0, tail=0, next=0, t=0, done=0;
    while (done<n) {
        while (next<n && j[next].arrival<=t) q[tail++%1024]=next++;
        if (head==tail) { t=j[next].arrival; continue; }
        int k=q[head++%1024];
        int run = j[k].rem<QUANTUM ? j[k].rem : QUANTUM;
        seg(&t,j[k].id,t,t+run); t+=run; j[k].rem-=run;
        while (next<n && j[next].arrival<=t) q[tail++%1024]=next++;
        if (j[k].rem>0) q[tail++%1024]=k; else { j[k].ct=t; done++; }
    }
    printf("\n"); report("RR",j,n,w,t_);
}

static void run_prio(Job *j, int n, double *w, double *t_) {
    qsort(j,n,sizeof(Job),cmp_arr);
    printf("\n== Priority non-preemptive (lower number = higher priority; tie: arrival, then lower ID) ==\nTimeline: ");
    int t=0, done=0, fin[MAXJ]={0};
    while (done<n) {
        int best=-1;
        for (int i=0;i<n;i++) {
            if (fin[i]||j[i].arrival>t) continue;
            if (best<0 || j[i].priority<j[best].priority) best=i; /* array is sorted by arrival,id so ties keep earlier */
        }
        if (best<0) { for (int i=0;i<n;i++) if(!fin[i]){t=j[i].arrival;break;} continue; }
        seg(&t,j[best].id,t,t+j[best].burst); t+=j[best].burst; j[best].ct=t; fin[best]=1; done++;
    }
    printf("\n"); report("Priority",j,n,w,t_);
}

static int cmd_sched(const char *algo, const char *file) {
    Job j[MAXJ]; double w,t;
    int n=load_jobs(file,j); if(n<0) return 1;
    if (!strcmp(algo,"fcfs")) run_fcfs(j,n,&w,&t);
    else if (!strcmp(algo,"rr")) run_rr(j,n,&w,&t);
    else if (!strcmp(algo,"prio")) run_prio(j,n,&w,&t);
    else if (!strcmp(algo,"all")) {
        double r[3][2]; const char *nm[3]={"FCFS","RR","Priority"};
        Job c[MAXJ];
        memcpy(c,j,sizeof(j)); run_fcfs(c,n,&r[0][0],&r[0][1]);
        memcpy(c,j,sizeof(j)); run_rr(c,n,&r[1][0],&r[1][1]);
        memcpy(c,j,sizeof(j)); run_prio(c,n,&r[2][0],&r[2][1]);
        printf("\n== Comparison (same job set) ==\n%-10s %-12s %-12s\n","Policy","AvgWaiting","AvgTurnaround");
        for(int i=0;i<3;i++) printf("%-10s %-12.2f %-12.2f\n",nm[i],r[i][0],r[i][1]);
    } else { fprintf(stderr,"Error: unknown algorithm '%s' (use fcfs|rr|prio|all)\n",algo); return 1; }
    return 0;
}

/* ---------- FR3: producer-consumer logger ---------- */
#define BUFSZ 4
#define NPROD 2
#define PER_PROD 5
static char buf[BUFSZ][80];
static int in_i=0,out_i=0,cnt=0;
static pthread_mutex_t mx=PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t not_full=PTHREAD_COND_INITIALIZER, not_empty=PTHREAD_COND_INITIALIZER;
static const char *LOGFILE="campus.log";

static void *producer(void *arg) {
    long id=(long)arg;
    for (int i=1;i<=PER_PROD;i++) {
        pthread_mutex_lock(&mx);
        while (cnt==BUFSZ) pthread_cond_wait(&not_full,&mx);
        snprintf(buf[in_i],80,"Producer-%ld: job event #%d",id,i);
        printf("[produce] %s (queue=%d)\n",buf[in_i],cnt+1);
        in_i=(in_i+1)%BUFSZ; cnt++;
        pthread_cond_signal(&not_empty);
        pthread_mutex_unlock(&mx);
        usleep(20000*id);
    }
    return NULL;
}
static void *consumer(void *arg) {
    FILE *f=arg;
    for (int k=0;k<NPROD*PER_PROD;k++) {
        pthread_mutex_lock(&mx);
        while (cnt==0) pthread_cond_wait(&not_empty,&mx);
        char line[80]; strcpy(line,buf[out_i]); out_i=(out_i+1)%BUFSZ; cnt--;
        pthread_cond_signal(&not_full);
        pthread_mutex_unlock(&mx);
        fprintf(f,"LOG %02d | %s\n",k+1,line); fflush(f);
        printf("[consume] wrote log entry %02d\n",k+1);
        usleep(30000);
    }
    return NULL;
}
static int cmd_logger(void) {
    in_i=out_i=cnt=0;
    FILE *f=fopen(LOGFILE,"w"); if(!f){perror("fopen campus.log");return 1;}
    pthread_t p[NPROD],c;
    if (pthread_create(&c,NULL,consumer,f)) { perror("pthread_create"); fclose(f); return 1; }
    for (long i=0;i<NPROD;i++) if (pthread_create(&p[i],NULL,producer,(void*)(i+1))) { perror("pthread_create"); return 1; }
    for (int i=0;i<NPROD;i++) pthread_join(p[i],NULL);
    pthread_join(c,NULL);
    fclose(f);
    printf("All threads joined cleanly. %d entries written to %s\n",NPROD*PER_PROD,LOGFILE);
    return 0;
}

/* ---------- FR4: shared memory + named semaphore ---------- */
typedef struct { int n; int data[8]; char msg[64]; } Shared;
static int shm_exists(const char *name){ int fd=shm_open(name,O_RDONLY,0); if(fd>=0){close(fd);return 1;} return 0; }

static int cmd_ipc(void) {
    char shname[64], semname[64];
    snprintf(shname,64,"/campus_shm_%d",getpid()); snprintf(semname,64,"/campus_sem_%d",getpid());
    int fd=shm_open(shname,O_CREAT|O_RDWR|O_EXCL,0600);
    if(fd<0){perror("shm_open");return 1;}
    if(ftruncate(fd,sizeof(Shared))<0){perror("ftruncate");shm_unlink(shname);return 1;}
    Shared *s=mmap(NULL,sizeof(Shared),PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);
    if(s==MAP_FAILED){perror("mmap");shm_unlink(shname);return 1;}
    sem_t *sem=sem_open(semname,O_CREAT|O_EXCL,0600,0);
    if(sem==SEM_FAILED){perror("sem_open");munmap(s,sizeof(Shared));shm_unlink(shname);return 1;}
    printf("Created %s and %s\n",shname,semname);
    pid_t pid=fork();
    if(pid<0){perror("fork");return 1;}
    if(pid==0){ /* reader / consumer process */
        sem_wait(sem);
        int sum=0; for(int i=0;i<s->n;i++) sum+=s->data[i];
        printf("[reader pid=%d] got message '%s', %d values, sum=%d\n",getpid(),s->msg,s->n,sum);
        munmap(s,sizeof(Shared)); sem_close(sem); _exit(0);
    }
    printf("[writer pid=%d] writing data to shared memory\n",getpid());
    s->n=5; for(int i=0;i<5;i++) s->data[i]=(i+1)*10;
    strcpy(s->msg,"CPU usage report from writer");
    sem_post(sem); /* signal data ready */
    int st; waitpid(pid,&st,0);
    munmap(s,sizeof(Shared)); close(fd); sem_close(sem);
    shm_unlink(shname); sem_unlink(semname);
    printf("Cleanup: shm exists? %s | semaphore removed.\n", shm_exists(shname)?"YES (leak!)":"no");
    return 0;
}

/* ---------- FR5: Banker's safety algorithm ---------- */
#define NP 5
#define NR 3
static int cmd_banker(const char *which) {
    int avail[NR], alloc[NP][NR]={{0,1,0},{2,0,0},{3,0,2},{2,1,1},{0,0,2}};
    int max[NP][NR]={{7,5,3},{3,2,2},{9,0,2},{2,2,2},{4,3,3}};
    if(!strcmp(which,"safe")){ int a[NR]={3,3,2}; memcpy(avail,a,sizeof a);}
    else if(!strcmp(which,"unsafe")){ int a[NR]={0,0,1}; memcpy(avail,a,sizeof a);
        alloc[1][0]=2; alloc[1][1]=0; alloc[1][2]=0; max[1][0]=3; max[1][1]=2; max[1][2]=2; }
    else { fprintf(stderr,"Error: use 'banker safe' or 'banker unsafe'\n"); return 1; }
    int need[NP][NR];
    printf("\n== Banker's algorithm (%s case) ==\nAvailable: %d %d %d\n",which,avail[0],avail[1],avail[2]);
    printf("P    Alloc    Max      Need\n");
    for(int i=0;i<NP;i++){ printf("P%d   ",i);
        for(int r=0;r<NR;r++) printf("%d ",alloc[i][r]); printf("  ");
        for(int r=0;r<NR;r++) printf("%d ",max[i][r]); printf("  ");
        for(int r=0;r<NR;r++){ need[i][r]=max[i][r]-alloc[i][r]; printf("%d ",need[i][r]); } printf("\n"); }
    int work[NR], fin[NP]={0}, seq[NP], cntf=0;
    memcpy(work,avail,sizeof work);
    int progress=1;
    while(cntf<NP && progress){ progress=0;
        for(int i=0;i<NP;i++){ if(fin[i]) continue;
            int ok=1; for(int r=0;r<NR;r++) if(need[i][r]>work[r]){ok=0;break;}
            if(ok){ for(int r=0;r<NR;r++) work[r]+=alloc[i][r]; fin[i]=1; seq[cntf++]=i; progress=1; } } }
    if(cntf==NP){ printf("Result: SAFE. Safe sequence: "); for(int i=0;i<NP;i++) printf("P%d%s",seq[i],i<NP-1?" -> ":"\n"); }
    else { printf("Result: UNSAFE. Processes that cannot finish:"); for(int i=0;i<NP;i++) if(!fin[i]) printf(" P%d",i); printf("\n"); }
    return 0;
}

/* ---------- FR6: worker failure detection ---------- */
static int cmd_worker(const char *mode) {
    int fail = !strcmp(mode,"fail");
    if(!fail && strcmp(mode,"normal")){ fprintf(stderr,"Error: use 'worker normal' or 'worker fail'\n"); return 1; }
    pid_t pids[3];
    for(int i=0;i<3;i++){
        pids[i]=fork();
        if(pids[i]<0){perror("fork");return 1;}
        if(pids[i]==0){
            printf("[worker %d pid=%d] started\n",i+1,getpid()); fflush(stdout);
            usleep(50000);
            if(fail && i==1){ printf("[worker 2] SIMULATED failure: exit(3)\n"); fflush(stdout); _exit(3); }
            if(fail && i==2){ printf("[worker 3] SIMULATED crash: raising SIGKILL on itself\n"); fflush(stdout); raise(SIGKILL); }
            _exit(0);
        }
    }
    int bad=0;
    for(int i=0;i<3;i++){
        int st; pid_t p=waitpid(pids[i],&st,0);
        if(p<0){perror("waitpid");continue;}
        if(WIFEXITED(st)){ printf("[parent] pid %d exited with status %d -> %s\n",p,WEXITSTATUS(st),WEXITSTATUS(st)?"FAILED (simulated)":"OK"); if(WEXITSTATUS(st)) bad++; }
        else if(WIFSIGNALED(st)){ printf("[parent] pid %d killed by signal %d (%s) -> FAILED (simulated)\n",p,WTERMSIG(st),strsignal(WTERMSIG(st))); bad++; }
    }
    printf("[parent] summary: %d of 3 workers failed. Remaining children: %s\n",bad,waitpid(-1,NULL,WNOHANG)<0&&errno==ECHILD?"none":"some");
    return 0;
}

/* ---------- FR7: resource limits ---------- */
static int cmd_rlimit(void) {
    pid_t p=fork();
    if(p<0){perror("fork");return 1;}
    if(p==0){
        struct rlimit rl={64UL<<20,64UL<<20};
        if(setrlimit(RLIMIT_AS,&rl)<0){perror("setrlimit");_exit(2);}
        printf("[child] RLIMIT_AS set to 64 MB; trying to allocate 256 MB...\n");
        void *m=malloc(256UL<<20);
        printf("[child] malloc(256MB) -> %s (%s)\n",m?"succeeded":"NULL",m?"limit not enforced":strerror(errno));
        _exit(m?1:0);
    }
    int st; waitpid(p,&st,0);
    printf("[parent] memory-limit child exit status %d (0 = limit observed)\n",WEXITSTATUS(st));
    p=fork();
    if(p==0){
        struct rlimit rl={1,3}; setrlimit(RLIMIT_CPU,&rl);
        printf("[child] RLIMIT_CPU set to 1 s; spinning...\n"); fflush(stdout);
        for(;;);
    }
    waitpid(p,&st,0);
    if(WIFSIGNALED(st)) printf("[parent] CPU-limit child terminated by signal %d (%s)\n",WTERMSIG(st),strsignal(WTERMSIG(st)));
    return 0;
}

/* ---------- FR8 / integrated demo ---------- */
static void usage(void) {
    puts("CampusCompute commands:\n"
         "  help                         show this message\n"
         "  sched fcfs|rr|prio|all [file] CPU scheduling (file lines: id arrival burst priority)\n"
         "  logger                       threaded producer-consumer logger -> campus.log\n"
         "  ipc                          shared memory + named semaphore demo\n"
         "  banker safe|unsafe           Banker's safety algorithm\n"
         "  worker normal|fail           fork/waitpid worker demo (failure is SIMULATED)\n"
         "  rlimit                       setrlimit demo (memory + CPU)\n"
         "  demo                         run everything and print a summary");
}
static int cmd_demo(void) {
    int rc=0; int r[6];
    r[0]=cmd_sched("all",NULL); r[1]=cmd_logger(); r[2]=cmd_ipc();
    r[3]=cmd_banker("safe"); r[4]=cmd_banker("unsafe"); r[5]=cmd_worker("fail");
    fflush(stdout); int x=cmd_rlimit();
    const char *nm[]={"scheduling","logger","ipc","banker-safe","banker-unsafe","workers"};
    printf("\n===== DEMO SUMMARY =====\n");
    for(int i=0;i<6;i++){ printf("%-14s %s\n",nm[i],r[i]?"FAIL":"OK"); rc|=r[i]; }
    printf("%-14s %s\n","rlimit",x?"FAIL":"OK");
    return rc|x;
}

int main(int argc, char **argv) {
    setvbuf(stdout,NULL,_IOLBF,0);
    if(argc<2){ usage(); return 1; }
    const char *c=argv[1];
    if(!strcmp(c,"help")){ usage(); return 0; }
    if(!strcmp(c,"sched")){ if(argc<3){fprintf(stderr,"Error: sched needs an algorithm\n");usage();return 1;} return cmd_sched(argv[2],argc>3?argv[3]:NULL); }
    if(!strcmp(c,"logger")) return cmd_logger();
    if(!strcmp(c,"ipc")) return cmd_ipc();
    if(!strcmp(c,"banker")){ if(argc<3){fprintf(stderr,"Error: banker needs safe|unsafe\n");return 1;} return cmd_banker(argv[2]); }
    if(!strcmp(c,"worker")){ if(argc<3){fprintf(stderr,"Error: worker needs normal|fail\n");return 1;} return cmd_worker(argv[2]); }
    if(!strcmp(c,"rlimit")) return cmd_rlimit();
    if(!strcmp(c,"demo")) return cmd_demo();
    fprintf(stderr,"Error: unknown command '%s'\n",c); usage(); return 1;
}
