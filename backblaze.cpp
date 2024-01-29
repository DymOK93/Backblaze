#include "backblaze.hpp"

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include "csv-parser/include/csv.hpp"
#include "unordered_dense/include/ankerl/unordered_dense.h"

#include <algorithm>
#include <charconv>
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

    fmt::print("Input: {}\nOutput: {}\n", input.string(), output.string());

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
void print_exception(std::exception_ptr exc_ptr) noexcept {
  try {
    rethrow_exception(std::move(exc_ptr));

  } catch (const system_error& exc) {
    const error_code ec{exc.code()};
    printf("%s - error %d: %s\n", exc.what(), ec.value(), ec.message().c_str());

  } catch (const exception& exc) {
    printf("%s\n", exc.what());

  } catch (error_code ec) {
    /**
     * TODO: fix csv::MmapParser::next() - throw system_error
     */
    printf("Error %d: %s\n", ec.value(), ec.message().c_str());

  } catch (...) {
    printf("Unknown exception\n");
  }
}
}  // namespace util

namespace bb {
//
//
//
template <class Ty, enable_if_t<is_integral_v<Ty>, int> = 0>
static Ty ToInt(string_view str) {
  Ty value;
  if (const auto [_, ec] = from_chars(data(str), data(str) + size(str), value);
      ec != std::errc{}) {
    throw system_error{make_error_code(ec)};
  }
  return value;
}

//
//
//
template <class Ty>
static string ToString(const Ty& value) {
  return fmt::format("{}", value);
}

//
//
//
static Date ReadDate(csv::CSVField field) {
  vector<string_view> yy_mm_dd;
  yy_mm_dd.reserve(kDateLength);

  split(yy_mm_dd, field.get<string_view>(), boost::is_any_of("-"));
  if (size(yy_mm_dd) != kDateLength) {
    std::cout << field << std::endl;
    throw invalid_argument{"Invalid date format"};
  }

  do {
    const auto year{ToInt<uint16_t>(yy_mm_dd[0])};
    if (year < kFirstYear || year > kLastYear) {
      break;
    }

    const auto month{ToInt<uint8_t>(yy_mm_dd[1])};
    if (month < 1 || month > kMonthPerYear) {
      break;
    }

    const auto day{ToInt<uint8_t>(yy_mm_dd[2])};
    if (day > kMaxDayPerMonth[month - 1]) {
      break;
    }

    return Date{year, month, day};

  } while (false);

  throw invalid_argument{fmt::format("Invalid date {}-{}-{}", yy_mm_dd[0],
                                     yy_mm_dd[1], yy_mm_dd[2])};
}

//
//
//
static string ReadId(csv::CSVField field) {
  auto id{field.get<string>()};
  const auto [first, last]{
      std::ranges::remove_if(id, [](unsigned char ch) { return isspace(ch); })};
  id.erase(first, last);
  return id;
}

//
//
//
static optional<uint64_t> ReadCapacity(csv::CSVField field) {
  const auto capacity{field.get<int64_t>()};
  if (capacity < 0) {
    return nullopt;
  }
  return static_cast<uint64_t>(capacity);
}

static void UpdateCapacity(const ModelName& model_name,
                           ModelStats& model_stats,
                           uint64_t capacity_bytes) {
  auto& capacity{model_stats.capacity_bytes};
  if (capacity && *capacity != capacity_bytes) {
    fmt::print("{} capacity change: was {}, now {}", model_name, *capacity,
               capacity_bytes);
  }
  capacity = capacity_bytes;
}

static void UpdateFailureDate(const ModelName& model_name,
                              const SerialNumber& serial_number,
                              DriveStats& drive_stats,
                              Date new_date) {
  auto& failure_date{drive_stats.failure_date};
  if (failure_date) {
    fmt::print("{} S/N {} multiple failure: was {}-{}-{}, now {}-{}-{}",
               model_name, serial_number, failure_date->year,
               failure_date->month, failure_date->day, new_date.year,
               new_date.month, new_date.day);
  }
  failure_date = new_date;
}

//
//
//
void ReadRawStats(ModelMap& map, const filesystem::path& file_path) {
  /**
   * TODO: fix CSVReader bugs with std::ifstream and ill-formed rows
   * @see 2015-01-24.csv#L39279
   */
  csv::CSVReader reader{file_path.string(),
                        csv::CSVFormat{}.header_row(0).delimiter(',')};

  for (const csv::CSVRow& row : reader) {
    const auto model_name{ReadId(row["model"])};
    auto& model_stats{map[model_name]};

    if (const auto capacity_bytes = ReadCapacity(row["capacity_bytes"]);
        capacity_bytes) {
      UpdateCapacity(model_name, model_stats, *capacity_bytes);
    }

    const auto serial_number{ReadId(row["serial_number"])};
    auto& drive_stats{model_stats.drives[serial_number]};

    const auto date{ReadDate(row["date"])};
    const auto year_idx{static_cast<size_t>(date.year) - kFirstYear};
    const auto month_idx{static_cast<size_t>(date.month) - 1};
    ++drive_stats.drive_day[year_idx * kMonthPerYear + month_idx];

    if (row["failure"].get<int>() != 0) {
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
      header.push_back(fmt::format("Y{}_M{}", year, month));
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
  row.push_back(ToString(model_stats.capacity_bytes.value_or(0)));

  const Date failure_date{drive_stats.failure_date.value_or(Date{})};
  row.emplace_back(ToString(failure_date.year));
  row.emplace_back(ToString(failure_date.month));
  row.emplace_back(ToString(failure_date.day));

  ranges::transform(drive_stats.drive_day, back_inserter(row),
                    [](const auto number) { return ToString(number); });
  return row;
}

//
//
//
void WriteParsedStats(const ModelMap& map, const filesystem::path& file_path) {
  ofstream output{file_path};
  auto writer{csv::make_csv_writer_buffered(output)};

  writer << MakeParsedStatsHeader();

  for (const auto& [model_name, model_stats] : map) {
    for (const auto& [serial_number, drive_stats] : model_stats.drives) {
      writer << MakeParsedStatsRow(model_name, model_stats, serial_number,
                                   drive_stats);
    }
  }

  writer.flush();
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
      auto& drive_stats{model_stats.drives[serial_number]};
      auto& drive_day{drive_stats.drive_day};

      ranges::transform(drive_day, other_drive_stats.drive_day,
                        begin(drive_day), plus<uint64_t>{});

      if (const auto& other_failure_date = other_drive_stats.failure_date;
          other_failure_date) {
        drive_stats.failure_date = other_failure_date;
      }
    }
  }
}
}  // namespace bb