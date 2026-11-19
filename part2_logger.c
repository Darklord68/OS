#define CAP 4
typedef struct {
 char data[CAP][80];int head,tail,count,closed,write_error;
 pthread_mutex_t mutex;pthread_cond_t full,empty;FILE *file;
} Logger;
typedef struct {Logger *l;int id;} Producer;
static void *produce(void *arg){
 Producer *p=arg;Logger *l=p->l;
 for(int i=1;i<=5;i++){
  pthread_mutex_lock(&l->mutex);
  while(l->count==CAP)pthread_cond_wait(&l->full,&l->mutex);
  snprintf(l->data[l->tail],80,"Producer-%d: job event #%d",p->id,i);
  printf("[produce] %s queue=%d\n",l->data[l->tail],l->count+1);
  l->tail=(l->tail+1)%CAP;l->count++;
  pthread_cond_signal(&l->empty);pthread_mutex_unlock(&l->mutex);
 }
 return NULL;
}
static void *consume(void *arg){
 Logger *l=arg;int n=0;
 for(;;){
  pthread_mutex_lock(&l->mutex);
  while(!l->count && !l->closed)pthread_cond_wait(&l->empty,&l->mutex);
  if(!l->count && l->closed){pthread_mutex_unlock(&l->mutex);break;}
  char line[80];strcpy(line,l->data[l->head]);l->head=(l->head+1)%CAP;l->count--;
  pthread_cond_signal(&l->full);pthread_mutex_unlock(&l->mutex);
  if(fprintf(l->file,"LOG %02d | %s\n",++n,line)<0)l->write_error=1;
  printf("[consume] log entry %d: %s\n",n,line);
 }
 return NULL;
}
static int logger(void){
 Logger l={.mutex=PTHREAD_MUTEX_INITIALIZER,.full=PTHREAD_COND_INITIALIZER,.empty=PTHREAD_COND_INITIALIZER};
 l.file=fopen("campus.log","w");if(!l.file){perror("campus.log");return 1;}
 pthread_t c,p[2];Producer args[2];int made=0,rc=0;
 int e=pthread_create(&c,NULL,consume,&l);
 if(e){fprintf(stderr,"pthread_create: %s\n",strerror(e));fclose(l.file);return 1;}
 for(int i=0;i<2;i++){
  args[i]=(Producer){&l,i+1};e=pthread_create(&p[i],NULL,produce,&args[i]);
  if(e){fprintf(stderr,"pthread_create: %s\n",strerror(e));rc=1;break;}made++;
 }
 for(int i=0;i<made;i++)pthread_join(p[i],NULL);
 pthread_mutex_lock(&l.mutex);l.closed=1;pthread_cond_broadcast(&l.empty);pthread_mutex_unlock(&l.mutex);
 pthread_join(c,NULL);if(fclose(l.file))rc=1;if(l.write_error)rc=1;
 pthread_cond_destroy(&l.full);pthread_cond_destroy(&l.empty);pthread_mutex_destroy(&l.mutex);
 printf("All %d producer(s) and consumer joined cleanly; synchronization objects destroyed. Log: campus.log\n",made);
 return rc;
}
