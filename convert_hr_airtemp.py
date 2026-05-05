#!/usr/bin/env python3
"""
Convert WTQ*.hr hourly files into normalized hourly air-temperature CSVs.

Input files are expected to be whitespace-delimited with seven columns:
year, day-of-year, local-standard-time hour, water temperature, discharge,
and two ERA5-Land temperature columns. Use --air-column to identify which
of the last two columns contains air temperature.

The output timestamp is UTC and formatted like the compiled water CSVs:
YYYY-MM-DD HH:MM:SS.
"""

import argparse
import csv
import sys
from datetime import datetime, timedelta
from pathlib import Path


MISSING_SENTINEL = -7777.0
OUTPUT_COLUMNS = [
    "station_code",
    "timestamp",
    "utc_offset",
    "dewpoint_temp_c",
    "air_temp_c",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Normalize WTQ*.hr hourly ERA5-Land air-temperature data."
    )
    parser.add_argument(
        "input",
        type=Path,
        help=(
            "Path to a WTQ*.hr whitespace-delimited file, or a directory "
            "containing *.hr files (one per station, named <station-code>.hr)."
        ),
    )
    parser.add_argument(
        "--station-code",
        help=(
            "Station code to write into the normalized CSV, e.g. 02FW002. "
            "Required for single-file input; inferred from filenames when a "
            "directory is given."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        help=(
            "Output CSV path. Defaults to "
            "airtemp_data/<station-code>_airtemp_hourly.csv. "
            "Ignored when input is a directory."
        ),
    )
    parser.add_argument(
        "--source-utc-offset-hours",
        type=float,
        default=-8.0,
        help=(
            "UTC offset of the source hour column. WTQ*.hr files from the "
            "sample are fixed Pacific Standard Time, so the default is -8."
        ),
    )
    parser.add_argument(
        "--air-column",
        type=int,
        choices=(6, 7),
        default=6,
        help=(
            "1-based source column containing air temperature. Defaults to 6; "
            "the other temperature column is treated as dewpoint."
        ),
    )
    parser.add_argument(
        "--missing-value",
        type=float,
        default=MISSING_SENTINEL,
        help="Numeric sentinel to treat as missing. Defaults to -7777.",
    )
    return parser.parse_args()


def default_output_path(station_code):
    return Path(__file__).resolve().parent / "airtemp_data" / f"{station_code}_airtemp_hourly.csv"


def parse_value(token, missing_value):
    value = float(token)
    if value == missing_value:
        return None
    return value


def format_value(value):
    if value is None:
        return ""
    text = f"{value:.4f}"
    return text.rstrip("0").rstrip(".")


def timestamp_from_year_day_hour(year, day_of_year, hour, source_utc_offset_hours):
    if day_of_year < 1:
        raise ValueError("day-of-year must be >= 1")
    if hour < 0 or hour > 23:
        raise ValueError("hour must be between 0 and 23")

    local_dt = datetime(year, 1, 1) + timedelta(days=day_of_year - 1, hours=hour)
    if local_dt.year != year:
        raise ValueError(f"day-of-year {day_of_year} is outside year {year}")

    utc_dt = local_dt - timedelta(hours=source_utc_offset_hours)
    return utc_dt.strftime("%Y-%m-%d %H:%M:%S")


def convert(input_path, output_path, station_code, source_utc_offset_hours, air_column, missing_value):
    dewpoint_column = 7 if air_column == 6 else 6
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows_written = 0
    comparable_temp_rows = 0
    dewpoint_above_air_rows = 0
    with input_path.open("r", encoding="utf-8") as source, output_path.open(
        "w", encoding="utf-8", newline=""
    ) as destination:
        writer = csv.DictWriter(destination, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()

        for line_number, line in enumerate(source, start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            parts = stripped.split()
            if len(parts) != 7:
                raise ValueError(
                    f"Line {line_number}: expected 7 whitespace-delimited columns, got {len(parts)}"
                )

            try:
                year = int(parts[0])
                day_of_year = int(parts[1])
                hour = int(parts[2])
                timestamp = timestamp_from_year_day_hour(
                    year,
                    day_of_year,
                    hour,
                    source_utc_offset_hours,
                )

                air_temp = parse_value(parts[air_column - 1], missing_value)
                dewpoint_temp = parse_value(parts[dewpoint_column - 1], missing_value)
            except ValueError as exc:
                raise ValueError(f"Line {line_number}: {exc}") from exc

            writer.writerow(
                {
                    "station_code": station_code,
                    "timestamp": timestamp,
                    "utc_offset": "0.0",
                    "dewpoint_temp_c": format_value(dewpoint_temp),
                    "air_temp_c": format_value(air_temp),
                }
            )
            rows_written += 1
            if air_temp is not None and dewpoint_temp is not None:
                comparable_temp_rows += 1
                if dewpoint_temp > air_temp:
                    dewpoint_above_air_rows += 1

    if comparable_temp_rows:
        dewpoint_above_air_fraction = dewpoint_above_air_rows / comparable_temp_rows
        if dewpoint_above_air_fraction > 0.5:
            print(
                "Warning: dewpoint is warmer than air temperature in "
                f"{dewpoint_above_air_fraction:.1%} of rows. "
                "The air/dewpoint columns may be swapped; try --air-column 6 or 7.",
                file=sys.stderr,
            )

    return rows_written


def main():
    args = parse_args()
    input_path = args.input.expanduser().resolve()
    if not input_path.exists():
        print(f"Input path not found: {input_path}", file=sys.stderr)
        return 1

    if input_path.is_dir():
        hr_files = sorted(input_path.glob("*.hr"))
        if not hr_files:
            print(f"No *.hr files found in {input_path}", file=sys.stderr)
            return 1
        if args.output is not None:
            print(
                "--output is ignored when input is a directory; "
                "outputs are written next to each *.hr file's station code.",
                file=sys.stderr,
            )

        total_rows = 0
        for hr_file in hr_files:
            station_code = args.station_code or hr_file.stem
            output_path = default_output_path(station_code)
            rows_written = convert(
                input_path=hr_file,
                output_path=output_path,
                station_code=station_code,
                source_utc_offset_hours=args.source_utc_offset_hours,
                air_column=args.air_column,
                missing_value=args.missing_value,
            )
            total_rows += rows_written
            print(f"Wrote {rows_written:,} hourly rows to {output_path}")

        print(
            f"Processed {len(hr_files)} file(s); {total_rows:,} hourly rows total."
        )
        print("Timestamps are UTC in YYYY-MM-DD HH:MM:SS format.")
        return 0

    station_code = args.station_code or input_path.stem
    output_path = args.output or default_output_path(station_code)
    rows_written = convert(
        input_path=input_path,
        output_path=output_path,
        station_code=station_code,
        source_utc_offset_hours=args.source_utc_offset_hours,
        air_column=args.air_column,
        missing_value=args.missing_value,
    )

    print(f"Wrote {rows_written:,} hourly rows to {output_path}")
    print("Timestamps are UTC in YYYY-MM-DD HH:MM:SS format.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
