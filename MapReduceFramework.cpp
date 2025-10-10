
#include "Barrier.h"
#include <algorithm>
#include <iostream>
#include "MapReduceFramework.h"
#include <atomic>
#include <pthread.h>

/**
 * a struct for all the variables rellevent for the managment of the job
 */
typedef struct JobContext {
    pthread_t *threads;
    std::atomic<uint64_t> mapCounter;
    std::atomic<uint64_t> reduceHelperCounter;
    std::atomic<uint64_t> reduceCounter;
    std::atomic<uint64_t> shuffleCounter;
    uint64_t totalValuesCount;
    int multiLevel;
    const InputVec *inputVec;
    OutputVec *outputVec;
    JobState *state;
    Barrier *barrier;
    pthread_mutex_t stateMutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t reduceMutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mapMutex = PTHREAD_MUTEX_INITIALIZER;
    bool pthread_joinCalled;
    std::vector<IntermediateVec> mappedVectors;
    std::vector<IntermediateVec> shuffeledVectores;
} JobContext;

/**
 * a struct of the arguments that the jobFunctionThread (that is given to each thread when it is initialized) need in order to run
 */
typedef struct Args {
    const MapReduceClient *client;
    int tid;
    JobContext *jobContext;
} Args;


void emit2 (K2* key, V2* value, void* context)
{
  Args* args = static_cast<Args*>(context);
  if (pthread_mutex_lock(&(args->jobContext->mapMutex)) != 0)
    {
      std::cout << "system error: failed to mutex lock" << std::endl;
      exit(1);
    }
  args->jobContext->mappedVectors[args->tid].push_back((IntermediatePair (key, value)));
  if (pthread_mutex_unlock(&(args->jobContext->mapMutex)) != 0)
    {
      std::cout << "system error: failed to mutex unlock" << std::endl;
      exit(1);
    }
}


void emit3 (K3* key, V3* value, void* context)
{
  Args* args = static_cast<Args*>(context);
  if (pthread_mutex_lock(&(args->jobContext->reduceMutex)) != 0)
    {
      std::cout << "system error: failed to mutex lock" << std::endl;
      exit(1);
    }
  args->jobContext->outputVec->push_back((OutputPair (key, value)));
  if (pthread_mutex_unlock(&(args->jobContext->reduceMutex)) != 0)
    {
      std::cout << "system error: failed to mutex unlock" << std::endl;
      exit(1);
    }
}

/**
 * @brief maps the vectors using the map function of the client
 *
 * @param args the arguments of the job
 */
void map(Args* args)
{
  while (args->jobContext->mapCounter < args->jobContext->inputVec->size())
    {
      if (pthread_mutex_lock(&(args->jobContext->stateMutex)) != 0)
        {
          std::cout << "system error: failed to mutex lock" << std::endl;
          exit(1);
        }
      args->jobContext->state->stage = MAP_STAGE;
      if (pthread_mutex_unlock(&(args->jobContext->stateMutex)) != 0)
        {
          std::cout << "system error: failed to mutex unlock" << std::endl;
          exit(1);
        }
      args->client->map(args->jobContext->inputVec->at(args->jobContext->mapCounter).first, args->jobContext->inputVec->at(args->jobContext->mapCounter).second, args);
      args->jobContext->mapCounter++;
    }
}

/**
 * @brief comperator to use in the sort part based on comparing the keys of the IntermediatePairs
 */
struct VectorComparator {
    bool operator()(IntermediatePair v1, IntermediatePair v2)
    {
      return *v1.first < *v2.first;
    }
};

/**
 * @brief sort all the mapped vectors so they would be from the smallest key to the largest
 *
 * @param args the arguments of the job
 */
void sort(Args* args)
{
  std::sort(args->jobContext->mappedVectors[args->tid].begin(),
            args->jobContext->mappedVectors[args->tid].end(), VectorComparator());
  args->jobContext->barrier->barrier();
}

/**
 * @brief if tid = 0 this function is called and it creates the shuffeled vectors out of the mapped vectors
 *
 * @param args the arguments of the job
 */
void shuffle(Args* args)
{
  size_t size = 0;
  for (auto vec : args->jobContext->mappedVectors)
    {
      size += vec.size ();
    }
  long unsigned int i = 0;
  while (i < size)
    {
      auto newVec = new IntermediateVec;
      K2 *key = args->jobContext->mappedVectors.at(0).back().first;
      for (auto &vec : args->jobContext->mappedVectors)
        {
          while (!(vec.empty()) && !(*key < *vec.back().first)&& !(*vec.back().first < *key))
            {
              newVec->push_back(vec.back());
              vec.pop_back();
              if(pthread_mutex_lock(&(args->jobContext->stateMutex)))
                {
                  std::cout << "system error: mutex lock failed" << std::endl;
                  exit(1);
                }
              args->jobContext->shuffleCounter++;
              args->jobContext->state->stage = SHUFFLE_STAGE;
              if(pthread_mutex_unlock(&(args->jobContext->stateMutex)))
                {
                  std::cout << "system error: mutex unlock failed" << std::endl;
                  exit(1);
                }
              i++;
            }
        }
      args->jobContext->shuffeledVectores.push_back(*newVec);
      delete newVec;
    }
  args->jobContext->totalValuesCount = 0;
  for (const auto& vec : args->jobContext->shuffeledVectores)
    {
      args->jobContext->totalValuesCount += vec.size();
    }
}

/**
 * @brief reduce the shuffeled vectors to create te output vectors using the reduce function of the client
 *
 * @param args the arguments of the job
 */
void reduce(Args* args)
{
  while (args->jobContext->reduceHelperCounter < args->jobContext->shuffeledVectores.size())
    {
      if (pthread_mutex_lock(&(args->jobContext->stateMutex)) != 0)
        {
          std::cout << "system error: failed to mutex lock" << std::endl;
          exit(1);
        }
      args->jobContext->state->stage = REDUCE_STAGE;
      if (pthread_mutex_unlock(&(args->jobContext->stateMutex)) != 0)
        {
          std::cout << "system error: failed to mutex unlock" << std::endl;
          exit(1);
        }
      if (args->jobContext->reduceHelperCounter < args->jobContext->shuffeledVectores.at(args->jobContext->reduceHelperCounter).size())
        {
          const IntermediateVec& vec = args->jobContext->shuffeledVectores.at(args->jobContext->reduceHelperCounter);
          size_t numValues = vec.size();
          args->client->reduce(&vec, args);
          args->jobContext->reduceCounter = args->jobContext->reduceCounter + numValues;
          args->jobContext->reduceHelperCounter++;
        }
    }
}

/**
 * @brief the job function of each thread
 *
 * @param arguments the arguments of the job
 * @return void*
 */
void* jobFunctionThread(void *arguments)
{
  Args* args = static_cast<Args*>(arguments);
  map(args);
  sort(args);
  if (args->tid == 0)
    {
      shuffle(args);
    }
  args->jobContext->barrier->barrier();
  reduce(args);
  delete args;
  pthread_exit(NULL);
}


JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel)
{
  JobContext *jobContext = new JobContext();
  jobContext->threads = new pthread_t[multiThreadLevel];
  std::atomic_init(&jobContext->mapCounter, 0);
  std::atomic_init(&jobContext->reduceHelperCounter, 0);
  std::atomic_init(&jobContext->reduceCounter, 0);
  std::atomic_init(&jobContext->shuffleCounter, 0);
  jobContext->totalValuesCount = multiThreadLevel * multiThreadLevel;
  jobContext->multiLevel = multiThreadLevel;
  jobContext->inputVec = &inputVec;
  jobContext->outputVec = &outputVec;
  jobContext->state = new JobState();
  jobContext->state->percentage = 0.0;
  jobContext->state->stage = UNDEFINED_STAGE;
  jobContext->barrier = new Barrier(multiThreadLevel);
  pthread_mutex_init(&(jobContext->stateMutex), nullptr);
  pthread_mutex_init(&(jobContext->reduceMutex), nullptr);
  pthread_mutex_init(&(jobContext->mapMutex), nullptr);
  jobContext->pthread_joinCalled = false;
  jobContext->mappedVectors = std::vector<IntermediateVec>(multiThreadLevel);
  jobContext->shuffeledVectores = std::vector<IntermediateVec>(0);
  for (int tid = 0; tid < multiThreadLevel; tid++){ // initializing the threads
      Args *args = new Args();
      args->client = &client;
      args->tid = tid;
      args->jobContext = jobContext;
      if (pthread_create(&(jobContext->threads[tid]), nullptr, jobFunctionThread, args)){
          std::cout << "system error: couldn't create a new thread" << std::endl;
          exit(1);
        }
    }
  return jobContext;
}

void waitForJob(JobHandle job){
  JobContext *jobContext = static_cast<JobContext*>(job);
  if (!jobContext->pthread_joinCalled){
      jobContext->pthread_joinCalled = true;
      for (int tid = 0; tid < jobContext->multiLevel; tid++){
          if (pthread_join(jobContext->threads[tid], nullptr)){
              std::cout << "system error: failed to do pthread_join" << std::endl;
              exit(1);
            }
        }
    }
}

void getJobState(JobHandle job, JobState* state){
  JobContext *jobContext = static_cast<JobContext*>(job);
  if (pthread_mutex_lock(&(jobContext->stateMutex)) != 0){
      std::cout << "system error: failed to mutex lock" << std::endl;
      exit(1);
    }
  state->stage = jobContext->state->stage;
  if (pthread_mutex_unlock(&(jobContext->stateMutex)) != 0){
      std::cout << "system error: failed to mutex unlock" << std::endl;
      exit(1);
    }
  switch (state->stage)
    {
      case UNDEFINED_STAGE:
        state->percentage = 0.0;
      break;

      case MAP_STAGE:
        state->percentage = (static_cast<float>(jobContext->mapCounter) / jobContext->inputVec->size()) * 100;
      break;

      case REDUCE_STAGE:
        state->percentage = (static_cast<float>(jobContext->reduceCounter) / jobContext->totalValuesCount) * 100;
      break;

      case SHUFFLE_STAGE:
        state->percentage = (static_cast<float>(jobContext->shuffleCounter) / jobContext->inputVec->size()) * 100;
      break;
    }
}

void destroyMutex(pthread_mutex_t *mutex) {
  if (pthread_mutex_destroy(mutex)) {
      std::cerr << "System error: pthread_mutex_destroy failed" << std::endl;
      exit(1);
    }
}

void closeJobHandle(JobHandle job){
  waitForJob(job);
  JobContext *jobContext = static_cast<JobContext*>(job);
  pthread_mutex_t* mutexes[] = { &(jobContext->stateMutex), &(jobContext->reduceMutex), &(jobContext->mapMutex) };
  for (auto &mutex : mutexes){
      destroyMutex(mutex);
    }
  for (auto& mappedVector : jobContext->mappedVectors){
      for (auto& mappedPair : mappedVector){
          delete mappedPair.first;
          delete mappedPair.second;
          delete &mappedPair;
        }
    }
  delete[] jobContext->threads;
  delete jobContext->state;
  delete jobContext->barrier;
  delete jobContext;
}





