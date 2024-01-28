#pragma once
#include <fmt/format.h>
#include "unordered_dense/include/ankerl/unordered_dense.h"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

namespace util {
//
//
//
template <class Duration>
class Timer {
  using Clock = std::chrono::high_resolution_clock;

 public:
  Timer() = default;

  [[nodiscard]] Duration elapsed() const noexcept {
    return std::chrono::duration_cast<Duration>(Clock::now() - m_time_point);
  }

 private:
  Clock::time_point m_time_point{Clock::now()};
};
}  // namespace util

namespace bb {
//
//
//
inline constexpr size_t kDateLength{3};
inline constexpr uint16_t kFirstYear{2013};
inline constexpr uint16_t kLastYear{2023};
inline constexpr uint8_t kMonthPerYear{12};
inline constexpr size_t kCounterCount{(kLastYear - kFirstYear + 1) *
                                      static_cast<size_t>(kMonthPerYear)};
//
//
//
inline constexpr std::array<std::string_view, 3> kOutputPrefix{
    "model", "serial_number", "failure"};

//
//
//
using Date = std::pair<uint16_t, uint8_t>;

//
//
//
std::optional<Date> ParseDate(const std::filesystem::path& file_path);

//
//
//
struct DriveStats {
  using Counters = std::vector<uint64_t>;

  DriveStats() noexcept : drive_day(kCounterCount), failure{false} {}

  Counters drive_day;
  bool failure;
};

//
//
//
struct StringHash {
  using is_transparent = void;
  using is_avalanching = void;

  [[nodiscard]] uint64_t operator()(std::string_view str) const noexcept {
    return ankerl::unordered_dense::hash<std::string_view>{}(str);
  }
};

//
//
//
using SerialNumber = std::string;

//
//
//
struct ModelStats {
  using DriveMap = ankerl::unordered_dense::
      map<SerialNumber, DriveStats, StringHash, std::equal_to<>>;

  DriveMap drives;
};

//
//
//
using ModelName = std::string;
using ModelMap = ankerl::unordered_dense::
    map<ModelName, ModelStats, StringHash, std::equal_to<>>;

//
//
//
void ReadRawStats(ModelMap& map, const std::filesystem::path& file_path);

//
//
//
void ReadParsedStats(ModelStats& map, const std::filesystem::path& file_path);

//
//
//
void WriteParsedStats(const ModelMap& map,
                      const std::filesystem::path& file_path);
//
//
//
void MergeParsedStats(ModelMap& map, const ModelMap& other);
}  // namespace bb

//
//
//
template <class DirIt>
bb::ModelMap ParseRawStats(DirIt it) {
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

  std::vector<bb::ModelMap> maps(thread_count);
  std::vector<std::thread> workers(thread_count);

  for (size_t idx = 0; idx < thread_count; ++idx) {
    workers[idx] = std::thread{[idx, &maps, &get_next_file_path]() {
      auto& stats{maps[idx]};
      for (;;) {
        try {
          const std::filesystem::path file_path{get_next_file_path()};
          if (file_path.empty()) {
            break;
          }

          ReadRawStats(stats, file_path);
        } catch (const std::exception& exc) {
          printf("%s\n", exc.what());
        }
      }
    }};
  }

  for (auto& worker : workers) {
    worker.join();
  }

  bb::ModelMap result;
  for (auto& stats : maps) {
    MergeParsedStats(result, stats);
  }

  return result;
}