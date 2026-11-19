#define NJ 32
#define Q 2
typedef struct { int id,a,b,p,rem,ct; } Job;
static const Job defaults[]={
 {.id=1,.a=0,.b=5,.p=2}, {.id=2,.a=1,.b=3,.p=1},
 {.id=3,.a=2,.b=8,.p=4}, {.id=4,.a=3,.b=2,.p=3},
 {.id=5,.a=4,.b=4,.p=1}};
static int order(const void *a,const void *b){
 const Job *x=a,*y=b;
 if(x->a!=y->a) return (x->a>y->a)-(x->a<y->a);
 return (x->id>y->id)-(x->id<y->id);
}
static int load(const char *path,Job *j){
 int n=0;
 if(!path){memcpy(j,defaults,sizeof defaults);n=5;}
 else {
  FILE *f=fopen(path,"r");if(!f){perror(path);return -1;}
  char line[256];
  while(fgets(line,sizeof line,f)){
   char extra; Job v={0};
   if(n==NJ || sscanf(line,"%d %d %d %d %c",&v.id,&v.a,&v.b,&v.p,&extra)!=4 ||
      v.id<=0 || v.a<0 || v.a>1000000 || v.b<=0 || v.b>1000000 || v.p<0){
    fprintf(stderr,"Invalid input: maximum 32 jobs; id>0, arrival 0..1000000, burst 1..1000000, priority>=0.\n");fclose(f);return -1;
   }
   for(int k=0;k<n;k++) if(j[k].id==v.id){fprintf(stderr,"Duplicate job ID\n");fclose(f);return -1;}
   j[n++]=v;
  }
  int bad=ferror(f);fclose(f);if(bad || !n){fprintf(stderr,"Empty/unreadable job file\n");return -1;}
 }
 for(int i=0;i<n;i++){j[i].rem=j[i].b;j[i].ct=0;}
 qsort(j,n,sizeof *j,order);return n;
}
/* Scheduler simulation; no hard-coded results. RR arrivals at a quantum
   boundary enter the queue before the just-preempted job. */
static void schedule(Job *j,int n,int mode,double *aw,double *at){
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
  }
  int run=(mode==1 && j[k].rem>Q)?Q:j[k].rem;
  printf("[%d-%d] J%d  ",t,t+run,j[k].id);t+=run;j[k].rem-=run;
  if(mode==1){
   while(next<n && j[next].a<=t){queue[tail]=next++;tail=(tail+1)%(NJ+1);count++;}
   if(j[k].rem){queue[tail]=k;tail=(tail+1)%(NJ+1);count++;}
  }
  if(!j[k].rem){j[k].ct=t;done++;}
 }
 double sw=0,st=0;
 puts("\nID Arrival Burst Priority Completion Turnaround Waiting Status");
 for(int i=0;i<n;i++){
  int tat=j[i].ct-j[i].a,wt=tat-j[i].b;sw+=wt;st+=tat;
  printf("J%d %7d %5d %8d %10d %10d %7d DONE\n",j[i].id,j[i].a,j[i].b,j[i].p,j[i].ct,tat,wt);
 }
 *aw=sw/n;*at=st/n;printf("Average waiting=%.2f turnaround=%.2f\n",*aw,*at);
}
static int sched(const char *alg,const char *path){
 int m=!strcmp(alg,"fcfs")?0:!strcmp(alg,"rr")?1:!strcmp(alg,"prio")?2:!strcmp(alg,"all")?3:-1;
 if(m<0){fprintf(stderr,"Use fcfs|rr|prio|all\n");return 1;}
 Job base[NJ],copy[NJ];int n=load(path,base);if(n<0)return 1;
 double w[3]={0},t[3]={0};
 for(int i=0;i<3;i++) if(m==3 || m==i){memcpy(copy,base,(size_t)n*sizeof *base);schedule(copy,n,i,&w[i],&t[i]);}
 if(m==3){puts("\nComparison: SAME job set\nPolicy       AvgWaiting AvgTurnaround");const char *names[]={"FCFS","RR","Priority"};for(int i=0;i<3;i++)printf("%-12s %10.2f %13.2f\n",names[i],w[i],t[i]);}
 return 0;
}
