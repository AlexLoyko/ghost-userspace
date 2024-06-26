//
// Created by root on 4/17/24.
//

#ifndef GHOST_SCHEDULERS_ENERGY_AWARE_CENTRALIZED_SCHEDULER_H
#define GHOST_SCHEDULERS_ENERGY_AWARE_CENTRALIZED_SCHEDULER_H

#include <cstdint>
#include <map>
#include <memory>

#include "absl/time/time.h"
#include "lib/agent.h"
#include "lib/scheduler.h"

namespace ghost {
  struct EnergyAwareTask : public Task<> {
    enum class RunState {
      kBlocked,
      kQueued,
      kRunnable,
      kOnCpu,
      kYielding,
    };

    EnergyAwareTask(Gtid energy_aware_task_gtid, ghost_sw_info sw_info)
        : Task<>(energy_aware_task_gtid, sw_info) {}

    ~EnergyAwareTask() override {}

    bool blocked() const { return run_state == RunState::kBlocked; }

    bool queued() const { return run_state == RunState::kQueued; }

    bool runnable() const { return run_state == RunState::kRunnable; }

    bool oncpu() const { return run_state == RunState::kOnCpu; }

    bool yielding() const { return run_state == RunState::kYielding; }

    static std::string_view RunStateToString(EnergyAwareTask::RunState run_state) {
      switch (run_state) {
        case EnergyAwareTask::RunState::kBlocked:
          return "Blocked";
        case EnergyAwareTask::RunState::kQueued:
          return "Queued";
        case EnergyAwareTask::RunState::kRunnable:
          return "Runnable";
        case EnergyAwareTask::RunState::kOnCpu:
          return "OnCpu";
        case EnergyAwareTask::RunState::kYielding:
          return "Yielding";
      }
    }

    friend std::ostream &operator<<(std::ostream &os,
                                    EnergyAwareTask::RunState run_state) {
      return os << RunStateToString(run_state);
    }

    RunState run_state = RunState::kBlocked;
    Cpu cpu{Cpu::UninitializedType::kUninitialized};

    // Whether the last execution was preempted or not.
    bool preempted = false;
    bool prio_boost = false;

    double lastEnergyReading;

    enum energyConsumptionLevel { LOW, MEDIUM, HIGH } energyConsumption;
  };

  class EnergyAwareScheduler : public BasicDispatchScheduler<EnergyAwareTask> {
  public:
    EnergyAwareScheduler(Enclave* enclave, CpuList cpulist,
                         std::shared_ptr<TaskAllocator<EnergyAwareTask>> allocator,
                         int32_t global_cpu, absl::Duration preemption_time_slice);
    ~EnergyAwareScheduler();

    void EnclaveReady();
    Channel& GetDefaultChannel() { return global_channel_; };

    // Handles task messages received from the kernel via shared memory queues.
    void TaskNew(EnergyAwareTask* task, const Message& msg);
    void TaskRunnable(EnergyAwareTask* task, const Message& msg);
    void TaskDeparted(EnergyAwareTask* task, const Message& msg);
    void TaskDead(EnergyAwareTask* task, const Message& msg);
    void TaskYield(EnergyAwareTask* task, const Message& msg);
    void TaskBlocked(EnergyAwareTask* task, const Message& msg);
    void TaskPreempted(EnergyAwareTask* task, const Message& msg);

    // Handles cpu "not idle" message. Currently a nop.
    void CpuNotIdle(const Message& msg);

    // Handles cpu "timer expired" messages. Currently a nop.
    void CpuTimerExpired(const Message& msg);

    bool Empty() { return num_tasks_ == 0; }

    // Removes 'task' from the runqueue.
    void RemoveFromRunqueue(EnergyAwareTask* task);

    // Main scheduling function for the global agent.
    void GlobalSchedule(const StatusWord& agent_sw, BarrierToken agent_sw_last);

    int32_t GetGlobalCPUId() {
      return global_cpu_.load(std::memory_order_acquire);
    }

    void SetGlobalCPU(const Cpu& cpu) {
      global_cpu_core_ = cpu.core();
      global_cpu_.store(cpu.id(), std::memory_order_release);
    }

    // When a different scheduling class (e.g., CFS) has a task to run on the
    // global agent's CPU, the global agent calls this function to try to pick a
    // new CPU to move to and, if a new CPU is found, to initiate the handoff
    // process.
    bool PickNextGlobalCPU(BarrierToken agent_barrier, const Cpu& this_cpu);

    // Print debug details about the current tasks managed by the global agent,
    // CPU state, and runqueue stats.
    void DumpState(const Cpu& cpu, int flags);
    std::atomic<bool> debug_runqueue_ = false;

    static const int kDebugRunqueue = 1;

  private:
    struct CpuState {
      EnergyAwareTask* current = nullptr;
      const Agent* agent = nullptr;
      absl::Time last_commit;
    } ABSL_CACHELINE_ALIGNED;

    // Updates the state of `task` to reflect that it is now running on `cpu`.
    // This method should be called after a transaction scheduling `task` onto
    // `cpu` succeeds.
    void TaskOnCpu(EnergyAwareTask* task, const Cpu& cpu);

    // Marks a task as yielded.
    void Yield(EnergyAwareTask* task);
    // Takes the task out of the yielding_tasks_ runqueue and puts it back into
    // the global runqueue.
    void Unyield(EnergyAwareTask* task);

    // Adds a task to the energy aware runqueue.
    void Enqueue(EnergyAwareTask* task);

    // Removes and returns the task at the front of the runqueue.
    EnergyAwareTask* Dequeue();

    // Prints all tasks (includin tasks not running or on the runqueue) managed by
    // the global agent.
    void DumpAllTasks();

    // Returns 'true' if a CPU can be scheduled by ghOSt. Returns 'false'
    // otherwise, usually because a higher-priority scheduling class (e.g., CFS)
    // is currently using the CPU.
    bool Available(const Cpu& cpu);

    CpuState* cpu_state_of(const EnergyAwareTask* task);

    CpuState* cpu_state(const Cpu& cpu) { return &cpu_states_[cpu.id()]; }

    size_t RunqueueSize() const { return run_queue_.size(); }

    bool RunqueueEmpty() const { return RunqueueSize() == 0; }

    CpuState cpu_states_[MAX_CPUS];

    int global_cpu_core_;
    std::atomic<int32_t> global_cpu_;
    LocalChannel global_channel_;
    int num_tasks_ = 0;

    const absl::Duration preemption_time_slice_;

    std::deque<EnergyAwareTask*> run_queue_;
    std::vector<EnergyAwareTask*> yielding_tasks_;

    absl::Time schedule_timer_start_;
    absl::Duration schedule_durations_;
    uint64_t iterations_ = 0;
  };

  // Initializes the task allocator and the energy-aware scheduler.
  std::unique_ptr<EnergyAwareScheduler> SingleThreadEnergyAwareScheduler(
      Enclave* enclave, CpuList cpulist, int32_t global_cpu,
      absl::Duration preemption_time_slice);

  // Operates as the Global or Satellite agent depending on input from the
  // global_scheduler->GetGlobalCPU callback.
  class EnergyAwareAgent : public LocalAgent {
  public:
    EnergyAwareAgent(Enclave* enclave, Cpu cpu, EnergyAwareScheduler* global_scheduler)
        : LocalAgent(enclave, cpu), global_scheduler_(global_scheduler) {}

    void AgentThread() override;
    Scheduler* AgentScheduler() const override { return global_scheduler_; }

  private:
    EnergyAwareScheduler* global_scheduler_;
  };

  class EnergyAwareConfig : public AgentConfig {
  public:
    EnergyAwareConfig() {}
    EnergyAwareConfig(Topology* topology, CpuList cpulist, Cpu global_cpu,
                      absl::Duration preemption_time_slice)
        : AgentConfig(topology, std::move(cpulist)),
          global_cpu_(global_cpu),
          preemption_time_slice_(preemption_time_slice) {}

    Cpu global_cpu_{Cpu::UninitializedType::kUninitialized};
    absl::Duration preemption_time_slice_ = absl::InfiniteDuration();
  };

  // A global agent scheduler. It runs a single-threaded energy-aware scheduler on the
  // global_cpu.
  template <class EnclaveType>
  class FullEnergyAwareAgent : public FullAgent<EnclaveType> {
  public:
    explicit FullEnergyAwareAgent(EnergyAwareConfig config) : FullAgent<EnclaveType>(config) {
      global_scheduler_ = SingleThreadEnergyAwareScheduler(
          &this->enclave_, *this->enclave_.cpus(), config.global_cpu_.id(),
          config.preemption_time_slice_);
      this->StartAgentTasks();
      this->enclave_.Ready();
    }

    ~FullEnergyAwareAgent() override {
      // Terminate global agent before satellites to avoid a false negative error
      // from ghost_run(). e.g. when the global agent tries to schedule on a CPU
      // without an active satellite agent.
      auto global_cpuid = global_scheduler_->GetGlobalCPUId();

      if (this->agents_.front()->cpu().id() != global_cpuid) {
        // Bring the current globalcpu agent to the front.
        for (auto it = this->agents_.begin(); it != this->agents_.end(); it++) {
          if (((*it)->cpu().id() == global_cpuid)) {
            auto d = std::distance(this->agents_.begin(), it);
            std::iter_swap(this->agents_.begin(), this->agents_.begin() + d);
            break;
          }
        }
      }

      CHECK_EQ(this->agents_.front()->cpu().id(), global_cpuid);

      this->TerminateAgentTasks();
    }

    std::unique_ptr<Agent> MakeAgent(const Cpu& cpu) override {
      return std::make_unique<EnergyAwareAgent>(&this->enclave_, cpu,
                                         global_scheduler_.get());
    }

    void RpcHandler(int64_t req, const AgentRpcArgs& args,
                    AgentRpcResponse& response) override {
      switch (req) {
        case EnergyAwareScheduler::kDebugRunqueue:
          global_scheduler_->debug_runqueue_ = true;
          response.response_code = 0;
          return;
        default:
          response.response_code = -1;
          return;
      }
    }

  private:
    std::unique_ptr<EnergyAwareScheduler> global_scheduler_;
  };
}

#endif //GHOST_SCHEDULERS_ENERGY_AWARE_CENTRALIZED_SCHEDULER_H
