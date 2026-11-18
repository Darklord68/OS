#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
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
#define CAP 4
#define MSG 256

/* Main-process-only collection: no logger thread exists during fork().
   After computation, two producers enqueue these real events in order;
   one consumer writes the bounded queue to campus.log. */
static char (*events)[MSG];
static size_t event_count,event_capacity;
static int event_error;
static pid_t owner;
static void event(const char *fmt,...){
 if(getpid()!=owner)return;
 if(event_count==event_capacity){
  size_t cap=event_capacity?event_capacity*2:64;
  void *p=realloc(events,cap*sizeof *events);
  if(!p){event_error=1;fprintf(stderr,"Event collection: out of memory\n");return;}
  events=p;event_capacity=cap;
 }
 va_list ap;va_start(ap,fmt);vsnprintf(events[event_count++],MSG,fmt,ap);va_end(ap);
}
static int reap(pid_t p,int *st){
 pid_t r;do{r=waitpid(p,st,0);}while(r<0 && errno==EINTR);
 if(r<0){perror("waitpid");event("ERROR: waitpid pid=%ld: %s",(long)p,strerror(errno));return 1;}return 0;
}

typedef struct {int id,a,b,p,rem,ct;} Job;
static const Job defaults[]={
 {.id=1,.a=0,.b=5,.p=2},{.id=2,.a=1,.b=3,.p=1},
 {.id=3,.a=2,.b=8,.p=4},{.id=4,.a=3,.b=2,.p=3},
 {.id=5,.a=4,.b=4,.p=1}};
static int order(const void *a,const void *b){
 const Job *x=a,*y=b;if(x->a!=y->a)return (x->a>y->a)-(x->a<y->a);
 return (x->id>y->id)-(x->id<y->id);
}
static int load(const char *path,Job *j){
 int n=0;
 if(!path){memcpy(j,defaults,sizeof defaults);n=5;event("Jobs: loaded five built-in sample jobs");}
 else{
  FILE *f=fopen(path,"r");if(!f){perror(path);event("Jobs: cannot open %s: %s",path,strerror(errno));return -1;}
  char line[512];int ln=0,header_seen=0,bad=0;
  while(fgets(line,sizeof line,f)){
   ln++;char *s=line;while(*s==' ' || *s=='\t' || *s=='\r' || *s=='\n')s++;
   if(!*s || *s=='#')continue;
   if(!strchr(line,'\n') && !feof(f)){bad=1;break;}
   char c1[32],c2[32],c3[32],c4[32],extra;
   if(!n && !header_seen && sscanf(s,"%31s %31s %31s %31s %c",c1,c2,c3,c4,&extra)==4 &&
      !strcmp(c1,"Job_ID") && !strcmp(c2,"Arrival_Time") && !strcmp(c3,"CPU_Burst") && !strcmp(c4,"Priority")){
    header_seen=1;continue;
   }
   /* strtol checks conversion overflow before narrowing to int. */
   int v[4];char *p=s;
   for(int k=0;k<4;k++){
    char *end;errno=0;long x=strtol(p,&end,10);
    if(p==end || errno==ERANGE || x<0 || x>INT_MAX){bad=1;break;}
    v[k]=(int)x;p=end;
    if(k<3 && *p!=' ' && *p!='\t'){bad=1;break;}
   }
   if(bad)break;
   while(*p==' ' || *p=='\t' || *p=='\r' || *p=='\n')p++;
   if(*p || n==NJ || v[0]<=0 || v[1]>1000000 || v[2]<=0 || v[2]>1000000){bad=1;break;}
   for(int k=0;k<n;k++)if(j[k].id==v[0])bad=1;
   if(bad)break;
   j[n++]=(Job){.id=v[0],.a=v[1],.b=v[2],.p=v[3]};
  }
  if(ferror(f))bad=1;
  fclose(f);
  if(bad || !n){fprintf(stderr,"Invalid/empty job file at line %d: four integers, unique positive ID, arrival 0..1000000, burst 1..1000000, priority>=0; max 32 jobs.\n",ln);event("Jobs: rejected input %s near line %d",path,ln);return -1;}
  event("Jobs: loaded %d jobs from %s",n,path);
 }
 for(int i=0;i<n;i++){j[i].rem=j[i].b;j[i].ct=0;}
 qsort(j,n,sizeof *j,order);return n;
}
static void schedule(Job *j,int n,int mode,double *aw,double *at){
 const char *names[]={"FCFS","Round Robin","Priority"};
 printf("\n== %s ==\nTie: arrival, then lower ID. Priority: lower number is higher. RR quantum=%d\nTimeline: ",names[mode],Q);
 event("Scheduler: %s started, jobs=%d, RR quantum=%d",names[mode],n,Q);
 int t=0,done=0,next=0,queue[NJ+1],head=0,tail=0,count=0;
 while(done<n){
  int k=-1;
  if(mode==1){
   while(next<n && j[next].a<=t){queue[tail]=next++;tail=(tail+1)%(NJ+1);count++;}
   if(!count){printf("[%d-%d] IDLE  ",t,j[next].a);t=j[next].a;continue;}
   k=queue[head];head=(head+1)%(NJ+1);count--;
  }else{
   for(int i=0;i<n;i++)if(j[i].rem && j[i].a<=t){if(k<0 || (mode==2 && j[i].p<j[k].p))k=i;}
   if(k<0){for(int i=0;i<n;i++)if(j[i].rem){printf("[%d-%d] IDLE  ",t,j[i].a);t=j[i].a;break;}continue;}
  }
  int run=(mode==1 && j[k].rem>Q)?Q:j[k].rem;
  printf("[%d-%d] J%d  ",t,t+run,j[k].id);t+=run;j[k].rem-=run;
  if(mode==1){
   while(next<n && j[next].a<=t){queue[tail]=next++;tail=(tail+1)%(NJ+1);count++;}
   if(j[k].rem){queue[tail]=k;tail=(tail+1)%(NJ+1);count++;}
  }
  if(!j[k].rem){j[k].ct=t;done++;event("Scheduler %s: Job %d completed at simulated time %d",names[mode],j[k].id,t);}
 }
 double sw=0,st=0;puts("\nID Arrival Burst Priority Completion Turnaround Waiting Status");
 for(int i=0;i<n;i++){
  int tat=j[i].ct-j[i].a,wt=tat-j[i].b;sw+=wt;st+=tat;
  printf("J%d %7d %5d %8d %10d %10d %7d DONE\n",j[i].id,j[i].a,j[i].b,j[i].p,j[i].ct,tat,wt);
 }
 *aw=sw/n;*at=st/n;printf("Average waiting=%.2f turnaround=%.2f\n",*aw,*at);
 event("Scheduler %s: average waiting=%.2f turnaround=%.2f",names[mode],*aw,*at);
}
static int sched(const char *alg,const char *path){
 int m=!strcmp(alg,"fcfs")?0:!strcmp(alg,"rr")?1:!strcmp(alg,"prio")?2:!strcmp(alg,"all")?3:-1;
 if(m<0){fprintf(stderr,"Use fcfs|rr|prio|all\n");event("Scheduler: rejected algorithm %s",alg);return 1;}
 Job base[NJ],copy[NJ];int n=load(path,base);if(n<0)return 1;
 double w[3]={0},t[3]={0};
 for(int i=0;i<3;i++)if(m==3 || m==i){memcpy(copy,base,(size_t)n*sizeof *base);schedule(copy,n,i,&w[i],&t[i]);}
 if(m==3){puts("\nComparison: SAME job set\nPolicy       AvgWaiting AvgTurnaround");const char *names[]={"FCFS","RR","Priority"};for(int i=0;i<3;i++)printf("%-12s %10.2f %13.2f\n",names[i],w[i],t[i]);}
 return 0;
}

/* The queue is bounded (CAP); the preceding collection array is dynamic.
   Producers claim and enqueue under one lock to preserve event order. */
typedef struct {
 char data[CAP][MSG];size_t next,written;int head,tail,count,closed,write_error;
 pthread_mutex_t mutex;pthread_cond_t full,empty;FILE *file;
} Logger;
typedef struct {Logger *l;int id;} Producer;
static void *produce(void *arg){
 Producer *p=arg;Logger *l=p->l;
 for(;;){
  pthread_mutex_lock(&l->mutex);
  while(l->count==CAP && l->next<event_count)pthread_cond_wait(&l->full,&l->mutex);
  if(l->next==event_count){pthread_mutex_unlock(&l->mutex);break;}
  strcpy(l->data[l->tail],events[l->next++]);
  printf("[produce-%d] event=%zu queue=%d\n",p->id,l->next,l->count+1);
  l->tail=(l->tail+1)%CAP;l->count++;
  pthread_cond_signal(&l->empty);pthread_mutex_unlock(&l->mutex);
 }
 return NULL;
}
static void *consume(void *arg){
 Logger *l=arg;
 for(;;){
  pthread_mutex_lock(&l->mutex);
  while(!l->count && !l->closed)pthread_cond_wait(&l->empty,&l->mutex);
  if(!l->count && l->closed){pthread_mutex_unlock(&l->mutex);break;}
  char line[MSG];strcpy(line,l->data[l->head]);l->head=(l->head+1)%CAP;l->count--;
  pthread_cond_broadcast(&l->full);pthread_mutex_unlock(&l->mutex);
  l->written++;if(fprintf(l->file,"LOG %04zu | %s\n",l->written,line)<0)l->write_error=1;
  printf("[consume] log entry %zu: %s\n",l->written,line);
 }
 return NULL;
}
static int logger_flush(void){
 Logger l={.mutex=PTHREAD_MUTEX_INITIALIZER,.full=PTHREAD_COND_INITIALIZER,.empty=PTHREAD_COND_INITIALIZER};
 int rc=event_error,made=0,e;pthread_t c,p[2];Producer args[2];
 l.file=fopen("campus.log","w");if(!l.file){perror("campus.log");return 1;}
 e=pthread_create(&c,NULL,consume,&l);
 if(e){fprintf(stderr,"consumer create: %s\n",strerror(e));fclose(l.file);rc=1;goto destroy;}
 for(int i=0;i<2;i++){
  args[i]=(Producer){&l,i+1};e=pthread_create(&p[i],NULL,produce,&args[i]);
  if(e){fprintf(stderr,"producer create: %s\n",strerror(e));rc=1;break;}made++;
 }
 for(int i=0;i<made;i++)pthread_join(p[i],NULL);
 pthread_mutex_lock(&l.mutex);l.closed=1;pthread_cond_broadcast(&l.empty);pthread_mutex_unlock(&l.mutex);
 pthread_join(c,NULL);
 if(fclose(l.file))rc=1;
 if(l.write_error || l.written!=event_count)rc=1;
 printf("Logger: %zu/%zu events written to campus.log; %d producer(s) and consumer joined cleanly.\n",l.written,event_count,made);
destroy:
 pthread_cond_destroy(&l.full);pthread_cond_destroy(&l.empty);pthread_mutex_destroy(&l.mutex);
 puts("Logger synchronization objects destroyed.");return rc;
}
static int logger_test(void){
 /* Standalone synchronization test remains explicitly labelled as samples. */
 for(int p=1;p<=2;p++)for(int i=1;i<=5;i++)event("SAMPLE producer-consumer test: source=%d event=%d",p,i);
 return 0;
}

typedef struct {int n,data[5];char msg[64];} Shared;
static int ipc(void){
 char sn[64],mn[64];snprintf(sn,sizeof sn,"/campus_shm_%ld",(long)getpid());snprintf(mn,sizeof mn,"/campus_sem_%ld",(long)getpid());
 int fd=-1,rc=1,created=0;Shared *s=MAP_FAILED;sem_t *sem=SEM_FAILED;
 fd=shm_open(sn,O_CREAT|O_EXCL|O_RDWR,0600);if(fd<0){perror("shm_open");event("IPC: shm_open failed: %s",strerror(errno));return 1;}created=1;
 if(ftruncate(fd,sizeof *s)){perror("ftruncate");goto cleanup;}
 s=mmap(NULL,sizeof *s,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);if(s==MAP_FAILED){perror("mmap");goto cleanup;}
 sem=sem_open(mn,O_CREAT|O_EXCL,0600,0);if(sem==SEM_FAILED){perror("sem_open");goto cleanup;}
 printf("Created shared memory %s and semaphore %s\n",sn,mn);event("IPC: created %s and %s",sn,mn);
 pid_t p=fork();if(p<0){perror("fork");goto cleanup;}
 if(!p){
  int r;do{r=sem_wait(sem);}while(r<0 && errno==EINTR);
  if(r<0){perror("sem_wait");_exit(2);}
  int sum=0;for(int i=0;i<s->n;i++)sum+=s->data[i];
  printf("[reader pid=%ld] message='%s', values=%d, sum=%d\n",(long)getpid(),s->msg,s->n,sum);
  munmap(s,sizeof *s);close(fd);sem_close(sem);_exit(sum==150?0:3);
 }
 s->n=5;for(int i=0;i<5;i++)s->data[i]=(i+1)*10;strcpy(s->msg,"CPU usage report from writer");
 printf("[writer pid=%ld] data ready; sem_post releases reader\n",(long)getpid());
 if(sem_post(sem)){perror("sem_post");kill(p,SIGKILL);}else event("IPC: writer posted semaphore after writing %d values",s->n);
 int st;if(!reap(p,&st)){
  if(WIFEXITED(st)){event("IPC: reader pid=%ld exit=%d (0 means data sum verified)",(long)p,WEXITSTATUS(st));if(WEXITSTATUS(st)==0)rc=0;}
  else if(WIFSIGNALED(st))event("IPC: reader pid=%ld signal=%d",(long)p,WTERMSIG(st));
 }
cleanup:
 if(s!=MAP_FAILED && munmap(s,sizeof *s)){perror("munmap");rc=1;}
 if(fd>=0 && close(fd)){perror("close");rc=1;}
 if(sem!=SEM_FAILED){if(sem_close(sem)){perror("sem_close");rc=1;}if(sem_unlink(mn)){perror("sem_unlink");rc=1;}}
 if(created && shm_unlink(sn)){perror("shm_unlink");rc=1;}
 fd=shm_open(sn,O_RDONLY,0);if(fd>=0){close(fd);rc=1;}else if(errno!=ENOENT)rc=1;
 if(sem!=SEM_FAILED){sem_t *check=sem_open(mn,0);if(check!=SEM_FAILED){sem_close(check);rc=1;}else if(errno!=ENOENT)rc=1;}
 printf("IPC cleanup verification: %s\n",rc?"ERROR (see above)":"shared memory and semaphore absent; child reaped");
 event("IPC: exchange and cleanup result=%s",rc?"FAIL":"OK");return rc;
}
static int banker(const char *which){
 int a[5][3]={{0,1,0},{2,0,0},{3,0,2},{2,1,1},{0,0,2}};
 int max[5][3]={{7,5,3},{3,2,2},{9,0,2},{2,2,2},{4,3,3}};
 int avail[3]={3,3,2},need[5][3],work[3],fin[5]={0},seq[5],n=0;
 int unsafe=!strcmp(which,"unsafe");
 if(unsafe){avail[0]=0;avail[1]=0;avail[2]=1;}
 else if(strcmp(which,"safe")){fprintf(stderr,"Use banker safe|unsafe\n");event("Banker: rejected mode %s",which);return 1;}
 printf("\nBanker's %s case\nAvailable: %d %d %d\nP   Allocation Maximum   Need\n",which,avail[0],avail[1],avail[2]);
 for(int i=0;i<5;i++){
  printf("P%d  ",i);for(int r=0;r<3;r++){printf("%d ",a[i][r]);}printf("   ");
  for(int r=0;r<3;r++){printf("%d ",max[i][r]);}printf("   ");
  for(int r=0;r<3;r++){need[i][r]=max[i][r]-a[i][r];printf("%d ",need[i][r]);}puts("");
 }
 memcpy(work,avail,sizeof work);
 for(;;){int progress=0;
  for(int i=0;i<5;i++)if(!fin[i]){
   int ok=1;for(int r=0;r<3;r++)if(need[i][r]>work[r])ok=0;
   if(ok){for(int r=0;r<3;r++)work[r]+=a[i][r];fin[i]=1;seq[n++]=i;progress=1;}
  }
  if(!progress)break;
 }
 if(n==5){printf("SAFE. Safe sequence: ");for(int i=0;i<5;i++)printf("P%d%s",seq[i],i==4?"\n":" -> ");event("Banker: SAFE; sequence P%d -> P%d -> P%d -> P%d -> P%d",seq[0],seq[1],seq[2],seq[3],seq[4]);}
 else{printf("UNSAFE. Cannot finish:");for(int i=0;i<5;i++)if(!fin[i])printf(" P%d",i);puts("\nUnsafe means no guaranteed safe sequence; not necessarily an existing deadlock.");event("Banker: UNSAFE; only %d/5 processes can complete in safety simulation",n);}
 return ((n==5)==!unsafe)?0:1;
}
static int worker(const char *mode){
 int fail=!strcmp(mode,"fail");if(!fail && strcmp(mode,"normal")){fprintf(stderr,"Use worker normal|fail\n");event("Workers: rejected mode %s",mode);return 1;}
 pid_t p[3];int made=0,rc=0,bad=0;
 for(int i=0;i<3;i++){
  pid_t x=fork();if(x<0){perror("fork");rc=1;break;}
  if(!x){printf("[worker %d pid=%ld] started\n",i+1,(long)getpid());
   if(fail && i==1){puts("SIMULATED failure: exit status 3");_exit(3);}
   if(fail && i==2){puts("SIMULATED failure: self SIGKILL");raise(SIGKILL);_exit(4);}_exit(0);
  }
  p[made++]=x;event("Workers: created worker %d pid=%ld mode=%s",i+1,(long)x,mode);
 }
 for(int i=0;i<made;i++){
  int st;if(reap(p[i],&st)){rc=1;continue;}int expected=0;
  if(WIFEXITED(st)){int e=WEXITSTATUS(st);printf("[parent] pid=%ld exit=%d\n",(long)p[i],e);if(e)bad++;expected=e==((fail && i==1)?3:0) && !(fail && i==2);event("Workers: pid=%ld exited status=%d%s",(long)p[i],e,fail?" [SIMULATED failure test]":"");}
  else if(WIFSIGNALED(st)){int sig=WTERMSIG(st);bad++;printf("[parent] pid=%ld signal=%d (%s)\n",(long)p[i],sig,strsignal(sig));expected=fail && i==2 && sig==SIGKILL;event("Workers: pid=%ld signal=%d%s",(long)p[i],sig,fail?" [SIMULATED failure test]":"");}
  if(!expected)rc=1;
 }
 printf("Workers: %d of %d failed%s; all created children waited for.\n",bad,made,fail?" (SIMULATED)":"");
 event("Workers: %d/%d failed%s; test=%s",bad,made,fail?" (SIMULATED)":"",rc?"FAIL":"OK");return rc;
}
static int limits(void){
 pid_t p=fork();if(p<0){perror("fork");event("Limits: memory child fork failed");return 1;}
 if(!p){struct rlimit r={64UL<<20,64UL<<20};
  if(setrlimit(RLIMIT_AS,&r)){perror("setrlimit RLIMIT_AS");_exit(2);}
  puts("[child] RLIMIT_AS=64 MiB; requesting 256 MiB");void *m=malloc(256UL<<20);
  if(!m){printf("malloc returned NULL: %s (limit observed)\n",strerror(errno));_exit(0);}
  printf("Unexpected allocation success at %p\n",m);free(m);_exit(1);
 }
 int st,rc=0;if(reap(p,&st))return 1;
 if(!WIFEXITED(st) || WEXITSTATUS(st)!=0)rc=1;
 if(WIFEXITED(st))event("Limits: RLIMIT_AS child exit=%d; 0=64 MiB limit applied and 256 MiB allocation refused",WEXITSTATUS(st));
 else if(WIFSIGNALED(st))event("Limits: memory child unexpectedly terminated by signal=%d",WTERMSIG(st));
 p=fork();if(p<0){perror("fork");event("Limits: CPU child fork failed");return 1;}
 if(!p){struct rlimit r={1,3};if(setrlimit(RLIMIT_CPU,&r)){perror("setrlimit RLIMIT_CPU");_exit(2);}
  signal(SIGXCPU,SIG_DFL);struct rlimit core={0,0};(void)setrlimit(RLIMIT_CORE,&core);
  puts("[child] RLIMIT_CPU soft=1 second, hard=3 seconds; spinning");for(;;){}
 }
 if(reap(p,&st))return 1;
 if(WIFSIGNALED(st)){int sig=WTERMSIG(st);printf("[parent] CPU child signal=%d (%s)\n",sig,strsignal(sig));event("Limits: CPU child signal=%d (%s), configured soft=1s hard=3s",sig,strsignal(sig));if(sig!=SIGXCPU && sig!=SIGKILL)rc=1;}
 else{puts("CPU limit not observed");event("Limits: CPU child exited without expected signal");rc=1;}
 printf("Resource limits: %s\n",rc?"FAIL":"OK; both children reaped");event("Limits: result=%s",rc?"FAIL":"OK");return rc;
}
static void usage(void){
 puts("CampusCompute commands:\n"
 "  help\n  sched fcfs|rr|prio|all [file]\n"
 "    file: id arrival burst priority; max 32 jobs\n"
 "    optional header: Job_ID Arrival_Time CPU_Burst Priority\n"
 "    blank lines and # comments accepted\n"
 "  logger                 standalone SAMPLE producer-consumer test\n"
 "  ipc                    shared memory + named semaphore\n"
 "  banker safe|unsafe\n  worker normal|fail     failures are SIMULATED\n"
 "  rlimit                 child-only memory and CPU limits\n"
 "  demo [file]            integrated run, optional scheduling file\n"
 "Every feature command writes its collected events to campus.log (overwrites).\n"
 "Logging is deferred: after module work/forks, 2 producer threads feed\n"
 "a bounded queue and 1 consumer writes the file. No logging threads at fork.\n"
 "Build: make (or gcc -Wall -Wextra -O2 -pthread campus.c -o campus -lrt)\n"
 "Clean executable/log: make clean");
}
static int demo(const char *path){
 int r[7];r[0]=sched("all",path);r[1]=ipc();r[2]=banker("safe");r[3]=banker("unsafe");
 r[4]=worker("normal");r[5]=worker("fail");r[6]=limits();
 const char *names[]={"scheduling","ipc","banker-safe","banker-unsafe","normal-workers","failed-workers","rlimit"};
 int rc=0;puts("\n===== MODULE SUMMARY =====");
 for(int i=0;i<7;i++){printf("%-16s %s\n",names[i],r[i]?"FAIL":"OK");event("Demo: %s=%s",names[i],r[i]?"FAIL":"OK");rc|=r[i];}return rc;
}
int main(int argc,char **argv){
 setvbuf(stdout,NULL,_IOLBF,0);owner=getpid();int rc=1,valid=1;
 if(argc==2 && !strcmp(argv[1],"help")){usage();return 0;}
 event("CampusCompute: command=%s",argc>1?argv[1]:"(missing)");
 if(argc>=3 && argc<=4 && !strcmp(argv[1],"sched"))rc=sched(argv[2],argc==4?argv[3]:NULL);
 else if(argc==3 && !strcmp(argv[1],"banker"))rc=banker(argv[2]);
 else if(argc==3 && !strcmp(argv[1],"worker"))rc=worker(argv[2]);
 else if(argc==2 && !strcmp(argv[1],"logger"))rc=logger_test();
 else if(argc==2 && !strcmp(argv[1],"ipc"))rc=ipc();
 else if(argc==2 && !strcmp(argv[1],"rlimit"))rc=limits();
 else if((argc==2 || argc==3) && !strcmp(argv[1],"demo"))rc=demo(argc==3?argv[2]:NULL);
 else valid=0;
 if(!valid){fprintf(stderr,"Error: invalid command or arguments.\n");usage();event("CLI: rejected invalid command or argument count");}
 event("CampusCompute: module work finished, result=%s; flushing collected events",rc?"FAIL":"OK");
 puts("\n=== Deferred threaded event logging ===");
 int logrc=logger_flush();free(events);
 printf("\nFINAL STATUS: modules=%s logger=%s\n",rc?"FAIL":"OK",logrc?"FAIL":"OK");return rc|logrc;
}
