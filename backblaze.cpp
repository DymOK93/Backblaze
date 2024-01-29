#include "backblaze.hpp"

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include "csv-parser/include/csv.hpp"
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
    fmt::print("Input: {}\nOutput: {}\n", input.string(), output.string());

    const util::Timer<chrono::seconds> timer;
    const bb::ModelMap model_map{
        ParseRawStats(filesystem::recursive_directory_iterator{input})};
    fmt::print("Finished: {} seconds\n", timer.elapsed().count());
    WriteParsedStats(model_map, output);

  } catch (const exception& exc) {
    printf("%s\n", exc.what());
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}

namespace bb {
//
//
//
optional<Date> ParseDate(const filesystem::path& file_path) {
  vector<string> yy_mm_dd;
  yy_mm_dd.reserve(kDateLength);

  split(yy_mm_dd, file_path.filename().string(), boost::is_any_of("-"));
  if (size(yy_mm_dd) != kDateLength)
    return nullopt;

  const auto year{stoul(yy_mm_dd[0])};
  if (year < kFirstYear || year > kLastYear)
    return nullopt;

  const auto month{stoul(yy_mm_dd[1])};
  if (month < 1 || month > kMonthPerYear)
    return nullopt;

  const auto day{stoul(yy_mm_dd[2])};
  if (day > kMaxDayPerMonth[month - 1])
    return nullopt;

  return Date{static_cast<uint16_t>(year), static_cast<uint8_t>(month),
              static_cast<uint8_t>(day)};
}

//
//
//
static std::string RemoveSpaces(std::string str) {
  const auto [first, last]{std::ranges::remove_if(
      str, [](unsigned char ch) { return isspace(ch); })};
  str.erase(first, last);
  return str;
}

//
//
//
void ReadRawStats(ModelMap& map, const filesystem::path& file_path) {
  const auto date{ParseDate(file_path)};
  if (!date)
    throw invalid_argument{"Invalid filename format"};

  ifstream input{file_path, ios::in | ios::binary};
  csv::CSVReader reader{input, csv::CSVFormat{}.header_row(0).delimiter(',')};

  for (const csv::CSVRow& row : reader) {
    auto model_name{RemoveSpaces(row["model"].get<string>())};
    auto& model_stats{map[std::move(model_name)]};

    auto serial_number{RemoveSpaces(row["serial_number"].get<string>())};
    auto& drive_stats{model_stats.drives[std::move(serial_number)]};

    const auto year_idx{static_cast<size_t>(date->year) - kFirstYear};
    const auto month_idx{static_cast<size_t>(date->month) - 1};
    ++drive_stats.drive_day[year_idx * kMonthPerYear + month_idx];

    if (row["failure"].get<int>() != 0) {
      drive_stats.failure_date = *date;
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
                               const string& serial_number,
                               const DriveStats& drive_stats) {
  vector<string> row;
  row.reserve(size(kOutputPrefix) + kCounterCount);
  row.push_back(model_name);
  row.push_back(serial_number);

  const Date failure_date{drive_stats.failure_date.value_or(Date{})};
  row.emplace_back(fmt::format("{}", failure_date.year));
  row.emplace_back(fmt::format("{}", failure_date.month));
  row.emplace_back(fmt::format("{}", failure_date.day));

  ranges::transform(drive_stats.drive_day, back_inserter(row),
                    [](uint64_t number) { return fmt::format("{}", number); });
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
      writer << MakeParsedStatsRow(model_name, serial_number, drive_stats);
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