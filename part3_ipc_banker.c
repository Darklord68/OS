typedef struct {int n,data[5];char msg[64];} Shared;
static int ipc(void){
 char sn[64],mn[64];snprintf(sn,sizeof sn,"/campus_shm_%ld",(long)getpid());snprintf(mn,sizeof mn,"/campus_sem_%ld",(long)getpid());
 int fd=-1,rc=1,created=0;Shared *s=MAP_FAILED;sem_t *sem=SEM_FAILED;
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
 }
 s->n=5;for(int i=0;i<5;i++)s->data[i]=(i+1)*10;strcpy(s->msg,"CPU usage report from writer");
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
 int a[5][3]={{0,1,0},{2,0,0},{3,0,2},{2,1,1},{0,0,2}};
 int max[5][3]={{7,5,3},{3,2,2},{9,0,2},{2,2,2},{4,3,3}};
 int avail[3]={3,3,2},need[5][3],work[3],fin[5]={0},seq[5],n=0;
 if(!strcmp(which,"unsafe")){avail[0]=0;avail[1]=0;avail[2]=1;}
 else if(strcmp(which,"safe")){fprintf(stderr,"Use banker safe|unsafe\n");return 1;}
 printf("\nBanker's %s case\nAvailable: %d %d %d\nP   Allocation Maximum   Need\n",which,avail[0],avail[1],avail[2]);
 for(int i=0;i<5;i++){
  printf("P%d  ",i);
  for(int r=0;r<3;r++){printf("%d ",a[i][r]);}
  printf("   ");
  for(int r=0;r<3;r++){printf("%d ",max[i][r]);}
  printf("   ");
  for(int r=0;r<3;r++){need[i][r]=max[i][r]-a[i][r];printf("%d ",need[i][r]);}
  puts("");
 }
 memcpy(work,avail,sizeof work);
 for(;;){int progress=0;
  for(int i=0;i<5;i++)if(!fin[i]){
   int ok=1;for(int r=0;r<3;r++)if(need[i][r]>work[r])ok=0;
   if(ok){for(int r=0;r<3;r++)work[r]+=a[i][r];fin[i]=1;seq[n++]=i;progress=1;}
  }
  if(!progress)break;
 }
 if(n==5){printf("SAFE. Safe sequence: ");for(int i=0;i<5;i++)printf("P%d%s",seq[i],i==4?"\n":" -> ");}
 else {printf("UNSAFE. Cannot finish:");for(int i=0;i<5;i++)if(!fin[i])printf(" P%d",i);puts("\nUnsafe means no guaranteed safe sequence; not necessarily an existing deadlock.");}
 return 0;
}
