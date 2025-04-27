#include "tasksys.h"

#include <iostream>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name()
{
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads)
{
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                          const std::vector<TaskID> &deps)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync()
{
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name()
{
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync()
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name()
{
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync()
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads), num_threads(num_threads)
{ // Fixed: Call base class constructor
    // Create worker threads
    for (int i = 0; i < num_threads; ++i)
    {
        threads.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
{
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        should_terminate = true;
    }
    queue_cv.notify_all();
    //  std::cout << "Terminating threads..." << std::endl;
    for (auto &thread : threads)
    {
        thread.join();
    }
    //  std::cout << "All threads terminated." << std::endl;
}

const char *TaskSystemParallelThreadPoolSleeping::name()
{
    return "Parallel + Thread Pool + Sleep";
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{
    // Implement synchronous version using async version
    runAsyncWithDeps(runnable, num_total_tasks, {}); // Removed unused variable
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps)
{

    // std::cout << "Running async with dependencies..." << std::endl;
    // for (auto dep : deps) {
    //     std::cout << "Dependency: " << dep << std::endl;
    // }

    TaskID batch_id = next_batch_id++;
    auto batch = std::make_shared<BatchInfo>();
    batch->runnable = runnable;
    batch->num_tasks = num_total_tasks;
    batch->dependencies = deps;
    batch->batch_id = batch_id; // Store batch_id in the struct

    {
        std::unique_lock<std::shared_mutex> lock(batch_mutex);
        batches[batch_id] = batch;
        pending_batches.insert(batch_id);
    }

    checkBatchReady(batch);

    return batch_id;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{
    // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    // std::cout << "Syncing..." << std::endl;
    std::unique_lock<std::mutex> lock(sync_mutex);
    //  std::cout << "Size of pending batches: " << pending_batches.size() << std::endl;
    sync_cv.wait(lock, [this]()
                 {
     std::shared_lock<std::shared_mutex> lock(batch_mutex);
     return pending_batches.empty(); });
    // std::cout << "Sync complete." << std::endl;
}

void TaskSystemParallelThreadPoolSleeping::workerLoop()
{
    while (true)
    {
        std::function<void()> task;

        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            //  queue_cv.wait(lock, [this]() {
            //      return !task_queue.empty() || should_terminate;
            //  });
            if (task_queue.empty())
            {
                // std::cout << "Thread " << std::this_thread::get_id() << " is waiting for tasks." << std::endl;
                queue_cv.wait(lock, [this]()
                              { return !task_queue.empty() || should_terminate; });
            }

            if (should_terminate)
            {
                return;
            }

            task = std::move(task_queue.front());
            task_queue.pop();
        }

        //  std::cout << "Thread " << std::this_thread::get_id() << " is executing a task." << std::endl;

        task();
    }
}

void TaskSystemParallelThreadPoolSleeping::checkBatchReady(const std::shared_ptr<BatchInfo> &batch)
{
    // std::this_thread::sleep_for(std::chrono::milliseconds(10));
    // std::cout << "Checking if batch " << batch->batch_id << " is ready..." << std::endl;
    bool all_deps_completed = true;

    for (TaskID dep_id : batch->dependencies)
    {
        std::shared_lock<std::shared_mutex> lock(batch_mutex);
        if (pending_batches.count(dep_id))
        {
            // std::cout << "Dependency " << dep_id << " is not completed." << std::endl;
            all_deps_completed = false;
            break;
        }
    }

    if (all_deps_completed && !batch->is_ready)
    {
        // std::cout << "Batch " << batch->batch_id << " is ready to run." << std::endl;
        batch->is_ready = true;
        enqueueTasks(batch);
    }
}

void TaskSystemParallelThreadPoolSleeping::enqueueTasks(const std::shared_ptr<BatchInfo> &batch)
{
    std::lock_guard<std::mutex> lock(queue_mutex);

    for (int i = 0; i < batch->num_tasks; ++i)
    {
        task_queue.emplace([this, batch, i]() { // Now capturing i explicitly
            batch->runnable->runTask(i, batch->num_tasks);

            if (++batch->completed_tasks == batch->num_tasks)
            {
                notifyBatchCompletion(batch->batch_id);
            }
        });
    }

    queue_cv.notify_all();
}

// void TaskSystemParallelThreadPoolSleeping::notifyBatchCompletion(TaskID batch_id) {
//     std::cout << "Batch " << batch_id << " completed." << std::endl;
//  {
//      std::lock_guard<std::mutex> lock(batch_mutex);
//      pending_batches.erase(batch_id);
//     //  std::cout << "Size of pending batches: " << pending_batches.size() << std::endl;
//  }

//  // Check if any dependent batches are now ready
//  for (auto& pair : batches) {
//      if (!pair.second->is_ready) {
//         std::cout << "Checking batch " << pair.first << " for readiness." << std::endl;
//         checkBatchReady(pair.second);
//      }
//  }
//     // for (TaskID id : pending_batches) {
//     //     std::cout << "Checking batch " << id << " for readiness." << std::endl;
//     //     checkBatchReady(batches[id]);
//     // }
//  sync_cv.notify_all();
// }

void TaskSystemParallelThreadPoolSleeping::notifyBatchCompletion(TaskID batch_id)
{
    // 先完成当前批次的清理工作
    {
        std::unique_lock<std::shared_mutex> lock(batch_mutex);
        pending_batches.erase(batch_id);
    }

    // 检查是否有其他批次可以运行
    std::shared_lock<std::shared_mutex> lock(batch_mutex);
    for (auto &pair : batches)
    {
        if (!pair.second->is_ready)
        {
            checkBatchReady(pair.second);
        }
    }
    lock.unlock();

    // 通知等待线程
    sync_cv.notify_all();
}