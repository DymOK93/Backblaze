#pragma once
#include <fmt/format.h>
#include "unordered_dense/include/ankerl/unordered_dense.h"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <thread>

namespace util {
template <class Duration>
class Timer {
  using Clock = std::chrono::high_resolution_clock;

 public:
  Timer() = default;

  [[nodiscard]] Duration elapsed() const noexcept {
    return std::chrono::duration_cast<Duration>(Clock::now() - m_time_point);
  }

 private:
  Clock::time_point m_time_point = Clock::now();
};
}  // namespace util

namespace bb {
struct Counters {
  uint64_t drive_day = 0;
  uint64_t failure = 0;

  Counters& operator+=(const Counters& other) noexcept {
    drive_day += other.drive_day;
    failure += other.failure;
    return *this;
  }
};

struct Stats {
  Counters counters;

  void merge(const Stats& other) noexcept { counters += other.counters; }
};

using ModelStats = ankerl::unordered_dense::map<std::string, Stats>;

void readRawStats(ModelStats& map, const std::filesystem::path& file_path);
void readParsedStats(ModelStats& map, const std::filesystem::path& file_path);
void writeParsedStats(const ModelStats& map,
                      const std::filesystem::path& file_path,
                      bool enable_merge);
void mergeParsedStats(ModelStats& map, const ModelStats& other);
}  // namespace bb

template <class DirIt>
bb::ModelStats parseStats(DirIt it) {
  const auto thread_count{std::thread::hardware_concurrency()};

  std::mutex it_mutex;
  const auto get_next_file_path{[&it_mutex, &it]() {
    const std::scoped_lock lock{it_mutex};

    std::filesystem::path csv_path;
    for (; it != DirIt{} && csv_path.empty(); ++it) {
      const auto& dir_entry{*it};

      if (const auto& file_path = dir_entry.path();
          file_path.extension() == ".csv") {
        csv_path = file_path;
        fmt::print("Processing {}\n", csv_path.string());
      }
    }

    return csv_path;
  }};

  std::vector<bb::ModelStats> maps(thread_count);
  std::vector<std::thread> workers(thread_count);

  for (size_t idx = 0; idx < thread_count; ++idx) {
    workers[idx] = std::thread{[idx, &maps, &get_next_file_path]() {
      auto& stats{maps[idx]};

      for (;;) {
        const std::filesystem::path file_path{get_next_file_path()};
        if (file_path.empty()) {
          break;
        }

        readRawStats(stats, file_path);
      }
    }};
  }

  for (auto& worker : workers) {
    worker.join();
  }

  bb::ModelStats result;
  for (auto& stats : maps) {
    mergeParsedStats(result, stats);
  }

  return result;
}