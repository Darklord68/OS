static int reap(pid_t p,int *st){
 int r; do {r=waitpid(p,st,0);} while(r<0 && errno==EINTR);
 if(r<0) perror("waitpid");
 return r<0?1:0;
}

static int worker(const char *mode){
 int fail=!strcmp(mode,"fail");if(!fail && strcmp(mode,"normal")){fprintf(stderr,"Use worker normal|fail\n");return 1;}
 pid_t p[3];int made=0,rc=0,bad=0;
 for(int i=0;i<3;i++){
  pid_t x=fork();if(x<0){perror("fork");rc=1;break;}
  if(!x){printf("[worker %d pid=%ld] started\n",i+1,(long)getpid());
   if(fail && i==1){puts("SIMULATED failure: exit status 3");_exit(3);}
   if(fail && i==2){puts("SIMULATED failure: self SIGKILL");raise(SIGKILL);_exit(4);}
   _exit(0);
  }
  p[made++]=x;
 }
 for(int i=0;i<made;i++){
  int st;if(reap(p[i],&st)){rc=1;continue;}
  int expected=0;
  if(WIFEXITED(st)){int e=WEXITSTATUS(st);printf("[parent] pid=%ld exit=%d\n",(long)p[i],e);if(e)bad++;expected=e==((fail && i==1)?3:0) && !(fail && i==2);}
  else if(WIFSIGNALED(st)){int sig=WTERMSIG(st);bad++;printf("[parent] pid=%ld signal=%d (%s)\n",(long)p[i],sig,strsignal(sig));expected=fail && i==2 && sig==SIGKILL;}
  if(!expected)rc=1;
 }
 printf("Workers: %d of %d failed%s; all created children waited for.\n",bad,made,fail?" (SIMULATED)":"");return rc;
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
 }
 int st,rc=0;if(reap(p,&st))return 1;
 if(!WIFEXITED(st) || WEXITSTATUS(st)!=0)rc=1;
 p=fork();if(p<0){perror("fork");return 1;}
 if(!p){struct rlimit r={1,3};if(setrlimit(RLIMIT_CPU,&r)){perror("setrlimit RLIMIT_CPU");_exit(2);}
  signal(SIGXCPU,SIG_DFL);struct rlimit core={0,0};(void)setrlimit(RLIMIT_CORE,&core);
  puts("[child] RLIMIT_CPU soft=1 second, hard=3 seconds; spinning");for(;;){}
 }
 if(reap(p,&st))return 1;
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
 int r[7];r[0]=sched("all",NULL);r[1]=logger();r[2]=ipc();
 r[3]=banker("safe");r[4]=banker("unsafe");r[5]=worker("fail");r[6]=limits();
 const char *names[]={"scheduling","logger","ipc","banker-safe","banker-unsafe","workers","rlimit"};
 int rc=0;puts("\n===== DEMO SUMMARY =====");
 for(int i=0;i<7;i++){printf("%-14s %s\n",names[i],r[i]?"FAIL":"OK");rc|=r[i];}return rc;
}
int main(int argc,char **argv){
 setvbuf(stdout,NULL,_IOLBF,0);
 if(argc==2){
  if(!strcmp(argv[1],"help")){usage();return 0;}
  if(!strcmp(argv[1],"logger"))return logger();
  if(!strcmp(argv[1],"ipc"))return ipc();
  if(!strcmp(argv[1],"rlimit"))return limits();
  if(!strcmp(argv[1],"demo"))return demo();
 }
 if(argc>=3 && argc<=4 && !strcmp(argv[1],"sched"))return sched(argv[2],argc==4?argv[3]:NULL);
 if(argc==3 && !strcmp(argv[1],"banker"))return banker(argv[2]);
 if(argc==3 && !strcmp(argv[1],"worker"))return worker(argv[2]);
 fprintf(stderr,"Error: invalid command or arguments.\n");usage();return 1;
}
