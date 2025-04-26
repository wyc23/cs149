#ifndef _TASKSYS_H
#define _TASKSYS_H

#include <atomic>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <shared_mutex>

#include "itasksys.h"

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        int num_threads_;
        std::thread* threads_;

        void threadFunc(IRunnable* runnable, int num_total_tasks, int* current_task_id, std::mutex* mutex);
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        struct BulkTask {
            IRunnable* runnable;
            int num_total_tasks;
            std::atomic<int> current_task_id;
            int num_completed_tasks;
            std::mutex* mutex;
            std::condition_variable* cv;
        };
        int num_threads_;
        std::thread* threads_;
        std::atomic<int> is_killed_;
        std::atomic<int> is_ready_;
        BulkTask* bulk_task_;
        

        void threadFunc();
        
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        struct BulkTask {
            IRunnable* runnable;
            int num_total_tasks;
            std::atomic<int> current_task_id;
            std::atomic<int> num_completed_tasks;
        };
        int num_threads_;
        std::thread* threads_;
        std::atomic<int> is_killed_;
        int is_ready_;
        std::mutex is_ready_mutex_;
        std::condition_variable is_ready_cv_;
        int is_completed_;
        std::mutex is_completed_mutex_;
        std::condition_variable is_completed_cv_;
        BulkTask* bulk_task_;
        std::shared_mutex bulk_task_mutex_;

        void threadFunc();
};

#endif
