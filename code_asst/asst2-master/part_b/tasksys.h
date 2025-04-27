#ifndef _TASKSYS_H
#define _TASKSYS_H

#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <shared_mutex>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <shared_mutex>

#include "itasksys.h"

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem
{
public:
    TaskSystemSerial(int num_threads);
    ~TaskSystemSerial();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem
{
public:
    TaskSystemParallelSpawn(int num_threads);
    ~TaskSystemParallelSpawn();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem
{
public:
    TaskSystemParallelThreadPoolSpinning(int num_threads);
    ~TaskSystemParallelThreadPoolSpinning();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

struct TaskBatch
{
    TaskID id;
    IRunnable *runnable;
    int num_tasks;
    std::vector<TaskID> dependencies;
    std::atomic<int> completed_tasks;
    bool is_ready; // 是否所有依赖都已满足

    TaskBatch(TaskID id, IRunnable *r, int n, const std::vector<TaskID> &deps)
        : id(id), runnable(r), num_tasks(n), dependencies(deps),
          completed_tasks(0), is_ready(false) {}
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem
{
private:
    // Worker threads
    int num_threads;
    std::vector<std::thread> threads;
    std::atomic<bool> should_terminate{false};

    // Task queue
    std::queue<std::function<void()>> task_queue;
    std::mutex queue_mutex;
    std::condition_variable queue_cv;

    // Task batch management
    struct BatchInfo
    {
        IRunnable *runnable;
        int num_tasks;
        std::vector<TaskID> dependencies;
        std::atomic<int> completed_tasks{0};
        bool is_ready{false};
        TaskID batch_id; // Added batch_id to the struct
    };

    std::shared_mutex batch_mutex;
    std::atomic<TaskID> next_batch_id{1}; // Start from 1
    std::unordered_map<TaskID, std::shared_ptr<BatchInfo>> batches;
    std::unordered_set<TaskID> pending_batches;

    // Sync mechanism
    std::mutex sync_mutex;
    std::condition_variable sync_cv;

public:
    TaskSystemParallelThreadPoolSleeping(int num_threads);
    ~TaskSystemParallelThreadPoolSleeping();
    const char *name() override;
    void run(IRunnable *runnable, int num_total_tasks) override;
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps) override;
    void sync() override;

private:
    void workerLoop();
    void checkBatchReady(const std::shared_ptr<BatchInfo> &batch);
    void enqueueTasks(const std::shared_ptr<BatchInfo> &batch);
    void notifyBatchCompletion(TaskID batch_id);
};

// Implementation

#endif
