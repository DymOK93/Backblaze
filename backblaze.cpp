#include "backblaze.hpp"

#include <rapidcsv.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include "unordered_dense/include/ankerl/unordered_dense.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <stdexcept>

using namespace std;

int main(int argc, char* argv[]) {
  try {
    if (argc != 3) {
      throw invalid_argument{"Usage: <input-path> <output-path>"};
    }

    const filesystem::path input{argv[1]};
    const filesystem::path output{argv[2]};

    if (output.extension() != ".csv") {
      throw invalid_argument{"Only CSV output is supported"};
    }

    fmt::print("Input: {}\n", input.string());
    fmt::print("Output: {}\n", output.string());

    const util::Timer<chrono::seconds> timer;
    const bb::ModelMap model_map{[&input] {
      if (is_directory(input)) {
        return ParseRawStats(filesystem::recursive_directory_iterator{input});
      }

      bb::ModelMap map;
      ReadRawStats(map, input);
      return map;
    }()};

    fmt::print("Finished: {} seconds\n", timer.elapsed().count());
    WriteParsedStats(model_map, output);

  } catch (...) {
    util::print_exception(current_exception());
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}

namespace util {
//
//
//
void print_exception(std::exception_ptr exc_ptr) noexcept {
  try {
    rethrow_exception(exc_ptr);

  } catch (const system_error& exc) {
    const error_code ec{exc.code()};
    printf("%s - error %d: %s\n", exc.what(), ec.value(), ec.message().c_str());

  } catch (const exception& exc) {
    printf("%s\n", exc.what());

  } catch (...) {
    printf("Unknown exception\n");
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
                     const std::string& field,
                     size_t row_idx) {
  auto id{doc.GetCell<string>(field, row_idx)};
  const auto [first, last]{
      std::ranges::remove_if(id, [](unsigned char ch) { return isspace(ch); })};
  id.erase(first, last);
  return id;
}

//
//
//
static optional<uint64_t> ReadCapacity(const rapidcsv::Document& doc,
                                       size_t row_idx) {
  const auto raw_capacity{doc.GetCell<int64_t>("capacity_bytes", row_idx)};
  if (raw_capacity < 0) {
    return nullopt;
  }

  const auto capacity{static_cast<uint64_t>(raw_capacity)};
  if (capacity < kMinCapacityBytes || capacity > kMaxCapacityBytes) {
    return nullopt;
  }

  return capacity;
}

//
//
//
static void UpdateCapacity(const ModelName& model_name,
                           ModelStats& model_stats,
                           uint64_t new_capacity) {
  if (auto& capacity_bytes = model_stats.capacity_bytes;
      new_capacity > capacity_bytes) {
    if (capacity_bytes) {
      fmt::print("{} capacity change: was {}, now {}\n", model_name,
                 *capacity_bytes, new_capacity);
    }

    capacity_bytes = new_capacity;
  }
}

//
//
//
static void UpdateFailureDate(const ModelName& model_name,
                              const SerialNumber& serial_number,
                              DriveStats& drive_stats,
                              Date new_date) {
  if (auto& failure_date = drive_stats.failure_date; new_date > failure_date) {
    if (failure_date) {
      fmt::print("{} S/N {} multiple failure: was {}, now {}\n", model_name,
                 serial_number, util::ToString(*failure_date),
                 util::ToString(new_date));
    }

    failure_date = new_date;
  }
}

//
//
//
void ReadRawStats(ModelMap& map, const filesystem::path& file_path) {
  ifstream input{file_path, ios::binary};
  input.exceptions(ios::badbit | ios::failbit);

  const rapidcsv::Document doc{input};
  const size_t row_count{doc.GetRowCount()};

  for (size_t idx = 0; idx < row_count; ++idx) {
    const auto model_name{ReadId(doc, "model", idx)};
    auto& model_stats{map[model_name]};

    if (const auto capacity_bytes = ReadCapacity(doc, idx); capacity_bytes) {
      UpdateCapacity(model_name, model_stats, *capacity_bytes);
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
    ++drive_stats.drive_day[year_idx * kMonthPerYear + month_idx];

    if (doc.GetCell<int>("failure", idx) != 0) {
      UpdateFailureDate(model_name, serial_number, drive_stats, date);
    }
  }
}

//
//
//
static vector<string> MakeParsedStatsHeader() {
  vector<string> header;
  header.reserve(size(kOutputPrefix) + kCounterCount);

  for (const auto& prefix : kOutputPrefix) {
    header.emplace_back(prefix);
  }

  for (auto year = kFirstYear; year <= kLastYear; ++year) {
    for (uint8_t month = 1; month <= kMonthPerYear; ++month) {
      header.push_back(fmt::format("date_{}-{}", year, month));
    }
  }

  return header;
}

//
//
//
static auto MakeParsedStatsRow(const string& model_name,
                               const ModelStats& model_stats,
                               const string& serial_number,
                               const DriveStats& drive_stats) {
  vector<string> row;
  row.reserve(size(kOutputPrefix) + kCounterCount);
  row.push_back(model_name);
  row.push_back(serial_number);
  row.push_back(util::ToString(model_stats.capacity_bytes));
  row.push_back(util::ToString(drive_stats.initial_power_on_hour));
  row.push_back(util::ToString(drive_stats.failure_date));
  ranges::transform(drive_stats.drive_day, back_inserter(row),
                    [](uint64_t number) {
                      return number == 0 ? "" : util::ToString(number);
                    });
  return row;
}

//
//
//
void WriteParsedStats(const ModelMap& map, const filesystem::path& file_path) {
  rapidcsv::Document doc;

  const vector header{MakeParsedStatsHeader()};
  for (size_t idx = 0; idx < size(header); ++idx) {
    doc.SetColumnName(idx, header[idx]);
  }

  size_t row_idx = 0;
  for (const auto& [model_name, model_stats] : map) {
    for (const auto& [serial_number, drive_stats] : model_stats.drives) {
      doc.SetRow(row_idx++, MakeParsedStatsRow(model_name, model_stats,
                                               serial_number, drive_stats));
    }
  }

  ofstream output{file_path, ios::binary};
  output.exceptions(ios::badbit | ios::failbit);
  doc.Save(output);
}

//
//
//
void MergeParsedStats(ModelMap& map, const ModelMap& other) {
  for (const auto& [model_name, other_model_stats] : other) {
    auto& model_stats{map[model_name]};

    auto& capacity_bytes{model_stats.capacity_bytes};
    if (const auto& other_capacity_bytes = other_model_stats.capacity_bytes;
        other_capacity_bytes > capacity_bytes) {
      capacity_bytes = other_capacity_bytes;
    }

    for (const auto& [serial_number, other_drive_stats] :
         other_model_stats.drives) {
      auto& drive_stats{
          model_stats.drives
              .try_emplace(serial_number,
                           other_drive_stats.initial_power_on_hour)
              .first->second};
      auto& drive_day{drive_stats.drive_day};

      ranges::transform(drive_day, other_drive_stats.drive_day,
                        begin(drive_day), plus<uint64_t>{});

      if (const auto& other_failure_date = other_drive_stats.failure_date;
          other_failure_date) {
        UpdateFailureDate(model_name, serial_number, drive_stats,
                          *other_failure_date);
      }
    }
  }
}
}  // namespace bb