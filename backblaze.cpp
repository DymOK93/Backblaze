#include "backblaze.hpp"

#include <rapidcsv.h>
#include <spdlog/stopwatch.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include "unordered_dense/include/ankerl/unordered_dense.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <stdexcept>
#include <variant>

using namespace std;

int main(int argc, char* argv[]) {
  try {
    spdlog::set_pattern("[%T.%e] [T%t] [%^%l%$] %v");

    if (argc != 3) {
      throw invalid_argument{"Usage: <input-path> <output-path>"};
    }

    const filesystem::path input{argv[1]};
    const filesystem::path output{argv[2]};

    if (output.extension() != ".csv") {
      throw invalid_argument{"Only CSV output is supported"};
    }

    spdlog::info("Input: {}", input.string());
    spdlog::info("Output: {}", output.string());

    const spdlog::stopwatch timer;
    const bb::DataCenterStats model_map{[&input] {
      if (is_directory(input)) {
        return ParseRawStats(filesystem::recursive_directory_iterator{input});
      }

      bb::DataCenterStats map;
      ReadRawStats(map, input);
      return map;
    }()};

    info("Finished: {:.3} seconds", timer);
    WriteParsedStats(model_map, output);

  } catch (...) {
    util::PrintException(current_exception());
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}

namespace util {
//
//
//
void PrintExceptionImpl(const exception_ptr& exc_ptr) {
  try {
    rethrow_exception(exc_ptr);

  } catch (const system_error& exc) {
    const error_code ec{exc.code()};
    spdlog::error("{} - code {}: {}", exc.what(), ec.value(),
                  ec.message().c_str());

  } catch (const exception& exc) {
    spdlog::error("{}", exc.what());

  } catch (...) {
    spdlog::error("Unknown exception");
  }
}

//
//
//
void PrintException(const exception_ptr& exc_ptr) noexcept {
  try {
    PrintExceptionImpl(exc_ptr);

  } catch (const std::bad_alloc&) {
    printf("Can't print exception: allocation failure\n");
  }
}
}  // namespace util

namespace bb {
//
//
//
static Date ReadDate(const rapidcsv::Document& doc, size_t row_idx) {
  vector<string> yy_mm_dd;
  yy_mm_dd.reserve(kDateLength);

  split(yy_mm_dd, doc.GetCell<string>("date", row_idx), boost::is_any_of("-"));
  if (size(yy_mm_dd) != kDateLength) {
    throw invalid_argument{"Invalid date format"};
  }

  do {
    const auto year{util::ToInt<uint16_t>(yy_mm_dd[0])};
    if (year < kFirstYear || year > kLastYear) {
      break;
    }

    const auto month{util::ToInt<uint8_t>(yy_mm_dd[1])};
    const auto day{util::ToInt<uint8_t>(yy_mm_dd[2])};

    const Date date{chrono::year{year}, chrono::month{month}, chrono::day{day}};
    if (!date.ok()) {
      break;
    }

    return date;

  } while (false);

  throw invalid_argument{fmt::format("Invalid date {}-{}-{}", yy_mm_dd[0],
                                     yy_mm_dd[1], yy_mm_dd[2])};
}

//
//
//
static string ReadId(const rapidcsv::Document& doc,
                     const string& field,
                     size_t row_idx) {
  auto id{doc.GetCell<string>(field, row_idx)};
  const auto [first, last]{
      ranges::remove_if(id, [](unsigned char ch) { return isspace(ch); })};
  id.erase(first, last);
  return id;
}

//
//
//
static variant<int64_t, uint64_t> ReadCapacity(const rapidcsv::Document& doc,
                                               size_t row_idx) {
  const auto raw_capacity{doc.GetCell<int64_t>("capacity_bytes", row_idx)};
  do {
    if (raw_capacity < 0) {
      break;
    }

    const auto capacity{static_cast<uint64_t>(raw_capacity)};
    if (capacity < kMinCapacityBytes || capacity > kMaxCapacityBytes) {
      break;
    }

    return capacity;

  } while (false);

  return raw_capacity;
}

//
//
//
static void UpdateCapacity(const ModelName& model_name,
                           ModelStats& model_stats,
                           optional<uint64_t> new_capacity) {
  if (auto& capacity_bytes = model_stats.capacity_bytes;
      new_capacity > capacity_bytes) {
    if (capacity_bytes) {
      spdlog::warn(
          "{} capacity change: was {}, now {}", model_name, *capacity_bytes,
          *new_capacity);  // NOLINT(bugprone-unchecked-optional-access)
    }

    capacity_bytes = new_capacity;
  }
}

//
//
//
void ReadRawStats(DataCenterStats& dc_stats,
                  const filesystem::path& file_path) {
  ifstream input{file_path, ios::binary};
  input.exceptions(ios::badbit | ios::failbit);

  const rapidcsv::Document doc{input};
  const size_t row_count{doc.GetRowCount()};

  for (size_t idx = 0; idx < row_count; ++idx) {
    const auto model_name{ReadId(doc, "model", idx)};
    auto& model_stats{dc_stats.models[model_name]};

    if (const auto capacity = ReadCapacity(doc, idx);
        holds_alternative<uint64_t>(capacity)) {
      UpdateCapacity(model_name, model_stats, get<uint64_t>(capacity));
    } else {
      spdlog::warn("{} invalid capacity: {} bytes", model_name,
                   get<int64_t>(capacity));
    }

    const auto serial_number{ReadId(doc, "serial_number", idx)};
    auto& drive_stats{
        model_stats.drives
            .try_emplace(serial_number, util::Lazy{[&doc, idx] {
                           const auto power_on_hour{
                               doc.GetCell<string>("smart_9_raw", idx)};
                           return power_on_hour.empty()
                                      ? optional<uint64_t>{}
                                      : util::ToInt<uint64_t>(power_on_hour);
                         }})
            .first->second};

    const auto date{ReadDate(doc, idx)};
    const auto year_idx{static_cast<int>(date.year()) - kFirstYear};
    const auto month_idx{static_cast<unsigned int>(date.month()) - 1};
    ++drive_stats.drive_day[static_cast<uint8_t>(year_idx * kMonthPerYear +
                                                 month_idx)];

    if (doc.GetCell<int>("failure", idx) != 0) {
      auto& failure_date{drive_stats.failure_date};
      failure_date.insert(ranges::upper_bound(failure_date, date), date);
      dc_stats.UpdateMaxFailure(size(failure_date));
    }
  }
}

//
//
//
static vector<string> MakeParsedStatsHeader(const DataCenterStats& dc_stats) {
  const size_t max_failure{dc_stats.max_failure};

  vector<string> header;
  header.reserve(size(kOutputPrefix) + max_failure + DriveStats::kCounterCount);

  for (const auto& prefix : kOutputPrefix) {
    header.emplace_back(prefix);
  }

  for (size_t idx = 0; idx < max_failure; ++idx) {
    header.push_back(fmt::format("failure_{}", idx + 1));
  }

  for (auto year = kFirstYear; year <= kLastYear; ++year) {
    for (uint8_t month = 1; month <= kMonthPerYear; ++month) {
      header.push_back(fmt::format("date_{}_{}", year, month));
    }
  }

  return header;
}

//
//
//
static auto MakeParsedStatsRow(const DataCenterStats& dc_stats,
                               const string& model_name,
                               const ModelStats& model_stats,
                               const string& serial_number,
                               const DriveStats& drive_stats) {
  const size_t max_failure{dc_stats.max_failure};

  vector<string> row;
  row.reserve(size(kOutputPrefix) + dc_stats.max_failure +
              DriveStats::kCounterCount);

  row.push_back(model_name);
  row.push_back(serial_number);
  row.push_back(util::ToString(model_stats.capacity_bytes));
  row.push_back(util::ToString(drive_stats.initial_power_on_hour));

  const auto& failure_date{drive_stats.failure_date};
  for (const auto& date : failure_date) {
    row.push_back(util::ToString(date));
  }

  row.insert(end(row), max_failure - size(failure_date), "");

  vector<uint8_t> drive_day(DriveStats::kCounterCount);
  for (const auto& [idx, value] : drive_stats.drive_day) {
    drive_day[idx] = value;
  }

  ranges::transform(drive_day, back_inserter(row), [](uint64_t number) {
    return number == 0 ? "" : util::ToString(number);
  });

  return row;
}

//
//
//
void WriteParsedStats(const DataCenterStats& dc_stats,
                      const filesystem::path& file_path) {
  rapidcsv::Document doc;

  const vector header{MakeParsedStatsHeader(dc_stats)};
  for (size_t idx = 0; idx < size(header); ++idx) {
    doc.SetColumnName(idx, header[idx]);
  }

  size_t row_idx = 0;
  for (const auto& [model_name, model_stats] : dc_stats.models) {
    for (const auto& [serial_number, drive_stats] : model_stats.drives) {
      vector row{MakeParsedStatsRow(dc_stats, model_name, model_stats,
                                    serial_number, drive_stats)};
      doc.SetRow(row_idx++, row);
    }
  }

  ofstream output{file_path, ios::binary};
  output.exceptions(ios::badbit | ios::failbit);
  doc.Save(output);
}

//
//
//
void MergeParsedStats(DataCenterStats& dc_stats,
                      const DataCenterStats& other_stats) {
  for (const auto& [model_name, other_model_stats] : other_stats.models) {
    auto& model_stats{dc_stats.models[model_name]};
    UpdateCapacity(model_name, model_stats, other_model_stats.capacity_bytes);

    for (const auto& [serial_number, other_drive_stats] :
         other_model_stats.drives) {
      auto& drive_stats{
          model_stats.drives
              .try_emplace(serial_number,
                           other_drive_stats.initial_power_on_hour)
              .first->second};
      auto& drive_day{drive_stats.drive_day};
      for (const auto& [idx, value] : other_drive_stats.drive_day) {
        drive_day[idx] += value;
      }

      auto& failure_date{drive_stats.failure_date};
      const auto& other_failure_date{other_drive_stats.failure_date};
      const auto middle{failure_date.insert(end(failure_date),
                                            begin(other_failure_date),
                                            end(other_failure_date))};
      ranges::inplace_merge(failure_date, middle);

      dc_stats.UpdateMaxFailure(size(failure_date));
    }
  }
}
}  // namespace bb