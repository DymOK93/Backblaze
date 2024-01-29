#pragma once
#include <fmt/format.h>
#include "unordered_dense/include/ankerl/unordered_dense.h"

#include <charconv>
#include <chrono>
#include <concepts>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <thread>

//
//
//
using Date = std::chrono::year_month_day;

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

//
//
//
template <std::invocable InitFn>
class Lazy {
 public:
  explicit Lazy(InitFn fn) noexcept(
      std::is_nothrow_move_constructible_v<InitFn>)
      : m_fn{std::move(fn)} {}

  operator std::invoke_result_t<InitFn>() && noexcept(
      std::is_nothrow_invocable_v<InitFn>) {
    return std::invoke(std::move(m_fn));
  }

  operator std::invoke_result_t<const InitFn>() const& noexcept(
      std::is_nothrow_invocable_v<InitFn>) {
    return std::invoke(m_fn);
  }

 private:
  InitFn m_fn;
};

//
//
//
void print_exception(std::exception_ptr exc_ptr) noexcept;

//
//
//
template <std::integral Ty>
Ty ToInt(std::string_view str) {
  Ty value;
  if (const auto [_, ec] =
          std::from_chars(data(str), data(str) + size(str), value);
      ec != std::errc{}) {
    throw std::system_error{make_error_code(ec)};
  }
  return value;
}

//
//
//
template <class Ty>
std::string ToString(const Ty& value) {
  return fmt::format("{}", value);
}

//
//
//
inline std::string ToString(const Date& date) {
  return fmt::format("{}-{}-{}", static_cast<int>(date.year()),
                     static_cast<unsigned int>(date.month()),
                     static_cast<unsigned int>(date.day()));
}

//
//
//
template <class Ty>
std::string ToString(const std::optional<Ty>& value) {
  return value ? ToString(*value) : "";
}
}  // namespace util

namespace bb {
//
//
//
constexpr uint64_t BytesToGBytes(uint64_t bytes_count) noexcept {
  return bytes_count * 1000 * 1000 * 1000;
}

//
//
//
constexpr uint64_t BytesToTBytes(uint64_t bytes_count) noexcept {
  return BytesToGBytes(bytes_count) * 1000;
}

//
// Very old drives
//
inline constexpr uint64_t kMinCapacityBytes{BytesToGBytes(40)};

//
// Modern HAMR drives
//
inline constexpr uint64_t kMaxCapacityBytes{BytesToTBytes(40)};

//
//
//
inline constexpr uint16_t kFirstYear{2013};
inline constexpr uint16_t kLastYear{2023};
inline constexpr uint8_t kMonthPerYear{12};
inline constexpr uint8_t kDateLength{3};

//
//
//
inline constexpr size_t kCounterCount{(kLastYear - kFirstYear + 1) *
                                      static_cast<size_t>(kMonthPerYear)};
//
//
//
inline constexpr std::array kOutputPrefix{
    "model", "serial_number", "capacity_bytes", "initial_power_on_hour"};

//
//
//
struct DriveStats {
  using Counters = std::vector<uint64_t>;

  DriveStats(std::optional<uint64_t> power_on_hour) noexcept
      : drive_day(kCounterCount), initial_power_on_hour{power_on_hour} {}

  Counters drive_day;
  std::optional<uint64_t> initial_power_on_hour;
  std::vector<Date> failure_date;
};

//
//
//
using SerialNumber = std::string;

//
//
//
struct ModelStats {
  using DriveMap = ankerl::unordered_dense::map<SerialNumber, DriveStats>;

  DriveMap drives;
  std::optional<uint64_t> capacity_bytes;
};

//
//
//
using ModelName = std::string;

//
//
//
struct DataCenterStats {
  using ModelMap = ankerl::unordered_dense::map<ModelName, ModelStats>;

  ModelMap models;
  uint64_t max_failure = 0;

  void UpdateMaxFailure(size_t failure_count) noexcept {
    if (failure_count > max_failure) {
      max_failure = failure_count;
    }
  }
};

//
//
//
void ReadRawStats(DataCenterStats& dc_stats,
                  const std::filesystem::path& file_path);

//
//
//
void ReadParsedStats(ModelStats& map, const std::filesystem::path& file_path);

//
//
//
void WriteParsedStats(const DataCenterStats& dc_stats,
                      const std::filesystem::path& file_path);
//
//
//
void MergeParsedStats(DataCenterStats& dc_stats,
                      const DataCenterStats& other_stats);
}  // namespace bb

//
//
//
template <std::input_iterator DirIt>
bb::DataCenterStats ParseRawStats(DirIt it) {
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

  std::vector<bb::DataCenterStats> dc_stats(thread_count);
  std::vector<std::thread> workers(thread_count);

  for (size_t idx = 0; idx < thread_count; ++idx) {
    workers[idx] = std::thread{[idx, &dc_stats, &get_next_file_path]() {
      for (;;) {
        try {
          const std::filesystem::path file_path{get_next_file_path()};
          if (file_path.empty()) {
            break;
          }

          ReadRawStats(dc_stats[idx], file_path);

        } catch (...) {
          util::print_exception(std::current_exception());
        }
      }
    }};
  }

  for (auto& worker : workers) {
    worker.join();
  }

  bb::DataCenterStats result;
  for (auto& stats : dc_stats) {
    MergeParsedStats(result, stats);
  }

  return result;
}