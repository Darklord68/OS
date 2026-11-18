#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/resource.h>

#define NJ 32
#define Q 2
typedef struct { int id,a,b,p,rem,ct; } Job;
static const Job defaults[]={
{.id=1,.a=0,.b=5,.p=2}, {.id=2,.a=1,.b=3,.p=1},
{.id=3,.a=2,.b=8,.p=4}, {.id=4,.a=3,.b=2,.p=3},
{.id=5,.a=4,.b=4,.p=1}};
static int reap(pid_t p,int *st){
int r; do {r=waitpid(p,st,0);} while(r<0 && errno==EINTR);
if(r<0) perror("waitpid");
return r<0?1:0;
}
static int order(const void a,const void b){
const Job x=a,y=b;
if(x->a!=y->a) return (x->a>y->a)-(x->a<y->a);
return (x->id>y->id)-(x->id<y->id);
}
static int load(const char path,Job j){
int n=0;
if(!path){memcpy(j,defaults,sizeof defaults);n=5;}
else {
FILE *f=fopen(path,"r");if(!f){perror(path);return -1;}
char line;
while(fgets(line,sizeof line,f)){
char extra; Job v={0};
if(n==NJ || sscanf(line,"%d %d %d %d %c",&v.id,&v.a,&v.b,&v.p,&extra)!=4 ||
v.id<=0 || v.a<0 || v.a>1000000 || v.b<=0 || v.b>1000000 || v.p<0){
fprintf(stderr,"Invalid input: maximum 32 jobs; id>0, arrival 0..1000000, burst 1..1000000, priority>=0.\n");fclose(f);return -1;
}
for(int k=0;k<n;k++) if(j[k].id==v.id){fprintf(stderr,"Duplicate job ID\n");fclose(f);return -1;}
j[n++]=v;
} int bad=ferror(f);fclose(f);if(bad || !n){fprintf(stderr,"Empty/unreadable job file\n");return -1;}
} for(int i=0;i<n;i++){j[i].rem=j[i].b;j[i].ct=0;}
qsort(j,n,sizeof *j,order);return n;
}
/* Scheduler simulation; no hard-coded results. RR arrivals at a quantum
boundary enter the queue before the just-preempted job. */
static void schedule(Job j,int n,int mode,double aw,double *at){
const char *names[]={"FCFS","Round Robin","Priority"};
printf("\n== %s ==\nTie: arrival, then lower ID. Priority: lower number is higher. RR quantum=%d\nTimeline: ",names[mode],Q);
int t=0,done=0,next=0,queue[NJ+1],head=0,tail=0,count=0;
while(done<n){
int k=-1;
if(mode==1){
while(next<n && j[next].a<=t){queue[tail]=next++;tail=(tail+1)%(NJ+1);count++;}
if(!count){printf("[%d-%d] IDLE  ",t,j[next].a);t=j[next].a;continue;}
k=queue[head];head=(head+1)%(NJ+1);count--;
}else{
for(int i=0;i<n;i++) if(j[i].rem && j[i].a<=t){
if(k<0 || (mode==2 && j[i].p<j[k].p)) k=i;
}
if(k<0){for(int i=0;i<n;i++) if(j[i].rem){printf("[%d-%d] IDLE  ",t,j[i].a);t=j[i].a;break;}continue;}
} int run=(mode==1 && j[k].rem>Q)?Q:j[k].rem;
printf("[%d-%d] J%d  ",t,t+run,j[k].id);t+=run;j[k].rem-=run;
if(mode==1){
while(next<n && j[next].a<=t){queue[tail]=next++;tail=(tail+1)%(NJ+1);count++;}
if(j[k].rem){queue[tail]=k;tail=(tail+1)%(NJ+1);count++;}
} if(!j[k].rem){j[k].ct=t;done++;}
} double sw=0,st=0;
puts("\nID Arrival Burst Priority Completion Turnaround Waiting Status");
for(int i=0;i<n;i++){
int tat=j[i].ct-j[i].a,wt=tat-j[i].b;sw+=wt;st+=tat;
printf("J%d %7d %5d %8d %10d %10d %7d DONE\n",j[i].id,j[i].a,j[i].b,j[i].p,j[i].ct,tat,wt);
} aw=sw/n;at=st/n;printf("Average waiting=%.2f turnaround=%.2f\n",aw,at);
}
static int sched(const char alg,const char path){
int m=!strcmp(alg,"fcfs")?0:!strcmp(alg,"rr")?1:!strcmp(alg,"prio")?2:!strcmp(alg,"all")?3:-1;
if(m<0){fprintf(stderr,"Use fcfs|rr|prio|all\n");return 1;}
Job base[NJ],copy[NJ];int n=load(path,base);if(n<0)return 1;
double w={0},t={0};
for(int i=0;i<3;i++) if(m==3 || m==i){memcpy(copy,base,(size_t)nsizeof base);schedule(copy,n,i,&w[i],&t[i]);}
if(m==3){puts("\nComparison: SAME job set\nPolicy       AvgWaiting AvgTurnaround");const char *names[]={"FCFS","RR","Priority"};for(int i=0;i<3;i++)printf("%-12s %10.2f %13.2f\n",names[i],w[i],t[i]);}
return 0;
}

#define CAP 4
typedef struct {
char data[CAP];int head,tail,count,closed,write_error;
pthread_mutex_t mutex;pthread_cond_t full,empty;FILE *file;
} Logger;
typedef struct {Logger *l;int id;} Producer;
static void produce(void arg){
Producer p=arg;Logger l=p->l;
for(int i=1;i<=5;i++){
pthread_mutex_lock(&l->mutex);
while(l->count==CAP)pthread_cond_wait(&l->full,&l->mutex);
snprintf(l->data[l->tail],80,"Producer-%d: job event #%d",p->id,i);
printf("[produce] %s queue=%d\n",l->data[l->tail],l->count+1);
l->tail=(l->tail+1)%CAP;l->count++;
pthread_cond_signal(&l->empty);pthread_mutex_unlock(&l->mutex);
} return NULL;
}
static void consume(void arg){
Logger *l=arg;int n=0;
for(;;){
pthread_mutex_lock(&l->mutex);
while(!l->count && !l->closed)pthread_cond_wait(&l->empty,&l->mutex);
if(!l->count && l->closed){pthread_mutex_unlock(&l->mutex);break;}
char line;strcpy(line,l->data[l->head]);l->head=(l->head+1)%CAP;l->count--;
pthread_cond_signal(&l->full);pthread_mutex_unlock(&l->mutex);
if(fprintf(l->file,"LOG %02d | %s\n",++n,line)<0)l->write_error=1;
printf("[consume] log entry %d: %s\n",n,line);
} return NULL;
}
static int logger(void){
Logger l={.mutex=PTHREAD_MUTEX_INITIALIZER,.full=PTHREAD_COND_INITIALIZER,.empty=PTHREAD_COND_INITIALIZER};
l.file=fopen("campus.log","w");if(!l.file){perror("campus.log");return 1;}
pthread_t c,p;Producer args;int made=0,rc=0;
int e=pthread_create(&c,NULL,consume,&l);
if(e){fprintf(stderr,"pthread_create: %s\n",strerror(e));fclose(l.file);return 1;}
for(int i=0;i<2;i++){
args[i]=(Producer){&l,i+1};e=pthread_create(&p[i],NULL,produce,&args[i]);
if(e){fprintf(stderr,"pthread_create: %s\n",strerror(e));rc=1;break;}made++;
} for(int i=0;i<made;i++)pthread_join(p[i],NULL);
pthread_mutex_lock(&l.mutex);l.closed=1;pthread_cond_broadcast(&l.empty);pthread_mutex_unlock(&l.mutex);
pthread_join(c,NULL);if(fclose(l.file))rc=1;if(l.write_error)rc=1;
pthread_cond_destroy(&l.full);pthread_cond_destroy(&l.empty);pthread_mutex_destroy(&l.mutex);
printf("All %d producer(s) and consumer joined cleanly; synchronization objects destroyed. Log: campus.log\n",made);
return rc;
}

typedef struct {int n,data;char msg;} Shared;
static int ipc(void){
char sn,mn;snprintf(sn,sizeof sn,"/campus_shm_%ld",(long)getpid());snprintf(mn,sizeof mn,"/campus_sem_%ld",(long)getpid());
int fd=-1,rc=1,created=0;Shared s=MAP_FAILED;sem_t sem=SEM_FAILED;
fd=shm_open(sn,O_CREAT|O_EXCL|O_RDWR,0600);if(fd<0){perror("shm_open");return 1;}created=1;
if(ftruncate(fd,sizeof *s)){perror("ftruncate");goto cleanup;}
s=mmap(NULL,sizeof *s,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);if(s==MAP_FAILED){perror("mmap");goto cleanup;}
sem=sem_open(mn,O_CREAT|O_EXCL,0600,0);if(sem==SEM_FAILED){perror("sem_open");goto cleanup;}
printf("Created shared memory %s and semaphore %s\n",sn,mn);
pid_t p=fork();if(p<0){perror("fork");goto cleanup;}
if(!p){
int r;do{r=sem_wait(sem);}while(r<0 && errno==EINTR);
if(r<0){perror("sem_wait");_exit(2);}
int sum=0;for(int i=0;i<s->n;i++)sum+=s->data[i];
printf("[reader pid=%ld] message='%s', values=%d, sum=%d\n",(long)getpid(),s->msg,s->n,sum);
munmap(s,sizeof *s);close(fd);sem_close(sem);_exit(0);
} s->n=5;for(int i=0;i<5;i++)s->data[i]=(i+1)*10;strcpy(s->msg,"CPU usage report from writer");
printf("[writer pid=%ld] data ready; sem_post releases reader\n",(long)getpid());
if(sem_post(sem)){perror("sem_post");kill(p,SIGKILL);}
int st;if(!reap(p,&st) && WIFEXITED(st) && WEXITSTATUS(st)==0)rc=0;
cleanup:
if(s!=MAP_FAILED && munmap(s,sizeof *s)){perror("munmap");rc=1;}
if(fd>=0 && close(fd)){perror("close");rc=1;}
if(sem!=SEM_FAILED){if(sem_close(sem)){perror("sem_close");rc=1;}if(sem_unlink(mn)){perror("sem_unlink");rc=1;}}
if(created && shm_unlink(sn)){perror("shm_unlink");rc=1;}
fd=shm_open(sn,O_RDONLY,0);if(fd>=0){close(fd);rc=1;}else if(errno!=ENOENT)rc=1;
if(sem!=SEM_FAILED){sem_t *check=sem_open(mn,0);if(check!=SEM_FAILED){sem_close(check);rc=1;}else if(errno!=ENOENT)rc=1;}
printf("IPC cleanup verification: %s\n",rc?"ERROR (see above)":"shared memory and semaphore absent; child reaped");return rc;
}
static int banker(const char *which){
int a={{0,1,0},{2,0,0},{3,0,2},{2,1,1},{0,0,2}};
int max={{7,5,3},{3,2,2},{9,0,2},{2,2,2},{4,3,3}};
int avail={3,3,2},need,work,fin={0},seq,n=0;
if(!strcmp(which,"unsafe")){avail=0;avail=0;avail=1;}
else if(strcmp(which,"safe")){fprintf(stderr,"Use banker safe|unsafe\n");return 1;}
printf("\nBanker's %s case\nAvailable: %d %d %d\nP   Allocation Maximum   Need\n",which,avail,avail,avail);
for(int i=0;i<5;i++){
printf("P%d  ",i);
for(int r=0;r<3;r++){printf("%d ",a[i][r]);}
printf("   ");
for(int r=0;r<3;r++){printf("%d ",max[i][r]);}
printf("   ");
for(int r=0;r<3;r++){need[i][r]=max[i][r]-a[i][r];printf("%d ",need[i][r]);}
puts("");
} memcpy(work,avail,sizeof work);
for(;;){int progress=0;
for(int i=0;i<5;i++)if(!fin[i]){
int ok=1;for(int r=0;r<3;r++)if(need[i][r]>work[r])ok=0;
if(ok){for(int r=0;r<3;r++)work[r]+=a[i][r];fin[i]=1;seq[n++]=i;progress=1;}
} if(!progress)break;
} if(n==5){printf("SAFE. Safe sequence: ");for(int i=0;i<5;i++)printf("P%d%s",seq[i],i==4?"\n":" -> ");}
else {printf("UNSAFE. Cannot finish:");for(int i=0;i<5;i++)if(!fin[i])printf(" P%d",i);puts("\nUnsafe means no guaranteed safe sequence; not necessarily an existing deadlock.");}
return 0;
}
static int worker(const char *mode){
int fail=!strcmp(mode,"fail");if(!fail && strcmp(mode,"normal")){fprintf(stderr,"Use worker normal|fail\n");return 1;}
pid_t p;int made=0,rc=0,bad=0;
for(int i=0;i<3;i++){
pid_t x=fork();if(x<0){perror("fork");rc=1;break;}
if(!x){printf("[worker %d pid=%ld] started\n",i+1,(long)getpid());
if(fail && i==1){puts("SIMULATED failure: exit status 3");_exit(3);}
if(fail && i==2){puts("SIMULATED failure: self SIGKILL");raise(SIGKILL);_exit(4);}
_exit(0);
} p[made++]=x;
} for(int i=0;i<made;i++){
int st;if(reap(p[i],&st)){rc=1;continue;}
int expected=0;
if(WIFEXITED(st)){int e=WEXITSTATUS(st);printf("[parent] pid=%ld exit=%d\n",(long)p[i],e);if(e)bad++;expected=e==((fail && i==1)?3:0) && !(fail && i==2);}
else if(WIFSIGNALED(st)){int sig=WTERMSIG(st);bad++;printf("[parent] pid=%ld signal=%d (%s)\n",(long)p[i],sig,strsignal(sig));expected=fail && i==2 && sig==SIGKILL;}
if(!expected)rc=1;
} printf("Workers: %d of %d failed%s; all created children waited for.\n",bad,made,fail?" (SIMULATED)":"");return rc;
}
/* RLIMIT_AS is demonstrated only in a child, leaving the main program unchanged. */
static int limits(void){
pid_t p=fork();if(p<0){perror("fork");return 1;}
if(!p){struct rlimit r={64UL<<20,64UL<<20};
if(setrlimit(RLIMIT_AS,&r)){perror("setrlimit RLIMIT_AS");_exit(2);}
puts("[child] RLIMIT_AS=64 MiB; requesting 256 MiB");
void *m=malloc(256UL<<20);
if(!m){printf("malloc returned NULL: %s (limit observed)\n",strerror(errno));_exit(0);}
printf("Unexpected allocation success at %p\n",m);free(m);_exit(1);
} int st,rc=0;if(reap(p,&st))return 1;
if(!WIFEXITED(st) || WEXITSTATUS(st)!=0)rc=1;
p=fork();if(p<0){perror("fork");return 1;}
if(!p){struct rlimit r={1,3};if(setrlimit(RLIMIT_CPU,&r)){perror("setrlimit RLIMIT_CPU");_exit(2);}
signal(SIGXCPU,SIG_DFL);struct rlimit core={0,0};(void)setrlimit(RLIMIT_CORE,&core);
puts("[child] RLIMIT_CPU soft=1 second, hard=3 seconds; spinning");for(;;){}
} if(reap(p,&st))return 1;
if(WIFSIGNALED(st)){int sig=WTERMSIG(st);printf("[parent] CPU child signal=%d (%s)\n",sig,strsignal(sig));if(sig!=SIGXCPU && sig!=SIGKILL)rc=1;}
else {puts("CPU limit not observed");rc=1;}
printf("Resource limits: %s\n",rc?"FAIL":"OK; both children reaped");return rc;
}
static void usage(void){
puts("CampusCompute commands:\n"
"  help\n  sched fcfs|rr|prio|all [file]\n"
"    file: id arrival burst priority, one job per line; max 32 jobs\n"
"  logger                 bounded queue -> campus.log (overwrites)\n"
"  ipc                    shared memory + named semaphore\n"
"  banker safe|unsafe\n  worker normal|fail     failures are SIMULATED\n"
"  rlimit                 child-only memory and CPU limits\n"
"  demo                   run all features\n"
"Build: make (or gcc -Wall -Wextra -O2 -pthread campus.c -o campus -lrt)\n"
"Clean generated executable/log: make clean");
}
static int demo(void){
int r;r=sched("all",NULL);r=logger();r=ipc();
r=banker("safe");r=banker("unsafe");r=worker("fail");r=limits();
const char *names[]={"scheduling","logger","ipc","banker-safe","banker-unsafe","workers","rlimit"};
int rc=0;puts("\n===== DEMO SUMMARY =====");
for(int i=0;i<7;i++){printf("%-14s %s\n",names[i],r[i]?"FAIL":"OK");rc|=r[i];}return rc;
}
int main(int argc,char **argv){
setvbuf(stdout,NULL,_IOLBF,0);
if(argc==2){
if(!strcmp(argv,"help")){usage();return 0;}
if(!strcmp(argv,"logger"))return logger();
if(!strcmp(argv,"ipc"))return ipc();
if(!strcmp(argv,"rlimit"))return limits();
if(!strcmp(argv,"demo"))return demo();
} if(argc>=3 && argc<=4 && !strcmp(argv,"sched"))return sched(argv,argc==4?argv:NULL);
if(argc==3 && !strcmp(argv,"banker"))return banker(argv);
if(argc==3 && !strcmp(argv,"worker"))return worker(argv);
fprintf(stderr,"Error: invalid command or arguments.\n");usage();return 1;
}
