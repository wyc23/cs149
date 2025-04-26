#include <iostream>
#include <mutex>

#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    num_threads_ = num_threads;
    threads_ = new std::thread[num_threads_];
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
    delete[] threads_;
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    std::mutex mutex;
    int current_task_id = 0;
    for (int i = 0; i < num_threads_; i++) {
        threads_[i] = std::thread(&TaskSystemParallelSpawn::threadFunc, this, runnable, num_total_tasks,
                                  &current_task_id, &mutex);
    }
    for (int i = 0; i < num_threads_; i++) {
        threads_[i].join();
    }

}

void TaskSystemParallelSpawn::threadFunc(IRunnable* runnable, int num_total_tasks, int* current_task_id, std::mutex* mutex) {
    int task_id = 0;
    while (true) {
        mutex->lock();
        task_id = *current_task_id;
        *current_task_id += 1;
        mutex->unlock();
        if (task_id >= num_total_tasks) {
            break;
        }
        runnable->runTask(task_id, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads_ = num_threads;
    threads_ = new std::thread[num_threads_];
    is_killed_.store(0);
    is_ready_. store(0);
    bulk_task_ = new BulkTask;
    bulk_task_->runnable = nullptr;
    bulk_task_->num_total_tasks = 0;
    bulk_task_->current_task_id.store(0);
    bulk_task_->num_completed_tasks = 0;
    bulk_task_->mutex = new std::mutex;
    bulk_task_->cv = new std::condition_variable;
    for (int i = 0; i < num_threads_; i++) {
        threads_[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::threadFunc, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    is_killed_.store(1);
    for (int i = 0; i < num_threads_; i++) {
        threads_[i].join();
    }
    delete[] threads_;
    delete bulk_task_->mutex;
    delete bulk_task_->cv;
    delete bulk_task_;
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    bulk_task_->runnable = runnable;
    bulk_task_->num_total_tasks = num_total_tasks;
    bulk_task_->current_task_id.store(0);
    bulk_task_->num_completed_tasks = 0;
    is_ready_.store(1);
    std::unique_lock<std::mutex> lock(*bulk_task_->mutex);
    bulk_task_->cv->wait(lock, [this]() {
        return bulk_task_->num_completed_tasks == bulk_task_->num_total_tasks;
    });
    is_ready_.store(0);

}

void TaskSystemParallelThreadPoolSpinning::threadFunc() {
    int task_id = 0;
    int num_total_tasks = 0;
    while (true) {
        if (is_killed_.load() == 1) {
            break;
        }
        // Spin
        if (is_ready_.load() == 0) {
            continue;
        }
        IRunnable* runnable = bulk_task_->runnable;
        num_total_tasks = bulk_task_->num_total_tasks;
        task_id = bulk_task_->current_task_id.fetch_add(1);
        if (task_id >= num_total_tasks) {
            // No more tasks to run
            is_ready_.store(0);
            continue;
        }
        runnable->runTask(task_id, num_total_tasks);
        bulk_task_->mutex->lock();
        bulk_task_->num_completed_tasks++;
        if (bulk_task_->num_completed_tasks == num_total_tasks) {
            bulk_task_->cv->notify_all();
        }
        bulk_task_->mutex->unlock();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    num_threads_ = num_threads;
    threads_ = new std::thread[num_threads_];
    is_killed_.store(0);
    is_ready_ = 0;
    is_completed_ = 0;
    bulk_task_ = new BulkTask;
    bulk_task_->runnable = nullptr;
    bulk_task_->num_total_tasks = 0;
    bulk_task_->current_task_id.store(0);
    bulk_task_->num_completed_tasks.store(0);

    for (int i = 0; i < num_threads_; i++) {
        threads_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::threadFunc, this);
    }

}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    is_killed_.store(1);
    std::unique_lock<std::shared_mutex> task_shrared_lock(bulk_task_mutex_);
    task_shrared_lock.unlock();
    std::unique_lock<std::mutex> ready_lock(is_ready_mutex_);
    is_ready_ = 1;
    is_ready_cv_.notify_all();
    ready_lock.unlock();
    for (int i = 0; i < num_threads_; i++) {
        threads_[i].join();
    }
    delete[] threads_;
    delete bulk_task_;
}

void TaskSystemParallelThreadPoolSleeping::threadFunc() {
    IRunnable* runnable = nullptr;
    int task_id = 0;
    int num_total_tasks = 0;
    int num_completed_tasks = 0;
    while (true) {
        std::shared_lock<std::shared_mutex> task_shrared_lock(bulk_task_mutex_);
        if (is_killed_.load() == 1) {
            task_shrared_lock.unlock();
            break;
        }
        runnable = bulk_task_->runnable;
        num_total_tasks = bulk_task_->num_total_tasks;
        task_id = bulk_task_->current_task_id.fetch_add(1);
        num_completed_tasks = bulk_task_->num_completed_tasks.load();
        if (task_id >= num_total_tasks) {
            // No more tasks to run
            std::unique_lock<std::mutex> ready_lock(is_ready_mutex_);
            is_ready_ = 0;
            task_shrared_lock.unlock();
            is_ready_cv_.wait(ready_lock, [this]() {
                return is_ready_ == 1;
            });
            continue;
        }
        runnable->runTask(task_id, num_total_tasks);
        num_completed_tasks = bulk_task_->num_completed_tasks.fetch_add(1);
        if (num_completed_tasks + 1 == num_total_tasks) {
            std::unique_lock<std::mutex> completed_lock(is_completed_mutex_);
            is_completed_ = 1;
            is_completed_cv_.notify_all();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::unique_lock<std::shared_mutex> task_shrared_lock(bulk_task_mutex_);
    bulk_task_->runnable = runnable;
    bulk_task_->num_total_tasks = num_total_tasks;
    bulk_task_->current_task_id.store(0);
    bulk_task_->num_completed_tasks.store(0);
    is_ready_ = 1;
    is_ready_cv_.notify_all();
    task_shrared_lock.unlock();
    std::unique_lock<std::mutex> completed_lock(is_completed_mutex_);
    if (is_completed_ == 0) {
        is_completed_cv_.wait(completed_lock, [this]() {
            return is_completed_ == 1;
        });
    }
    is_completed_ = 0;

}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
